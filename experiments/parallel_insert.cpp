/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "parallel_insert.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>

#include "common/configuration.hpp"
#include "common/database.hpp"
#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "common/spin_lock.hpp"
#include "data_structures/interface.hpp"
#include "data_structures/parallel.hpp"
#include "distributions/driver.hpp"
#include "distributions/interface.hpp"

using namespace std;
using namespace common;

namespace experiments {

namespace {

class DistributionParallel {
    distributions::Interface* m_distribution;
    SpinLock m_lock;
    uint64_t m_position = 0; // current position in the distribution

public:
    DistributionParallel(distributions::Interface* distribution) : m_distribution(distribution) {
        assert(distribution != nullptr);
    }

    // Fetch up to num_keys from the distribution. Return the window in the distribution for the
    // keys to use as the interval: [position, position + count).
    struct FetchOutput { uint64_t m_position; uint64_t m_count;};
    FetchOutput fetch(size_t num_keys){
        scoped_lock<SpinLock> lock{ m_lock }; // synchronise
        assert(m_position <= m_distribution->size() && "Overflow");
        auto position = m_position;
        size_t count = std::min(m_distribution->size() - m_position, num_keys);
        m_position += count;
        return FetchOutput{ position, count };
    }

    // Get the key at the given position
    int64_t get(uint64_t index){
        assert(index < m_distribution->size() && "Overflow");
        return m_distribution->key(index);
    }
};

static void pin_thread_to_socket(){
#if defined(HAVE_LIBNUMA)
    pin_thread_to_numa_node(0);
#endif
}

static
void thread_execute_inserts(int worker_id, data_structures::Interface* data_structure, DistributionParallel* distribution, atomic<int>* startup_counter){
    pin_thread_to_socket();

    assert(data_structure != nullptr && distribution != nullptr);

    constexpr uint64_t keys_to_fetch = 8;
    auto distribution_window = distribution->fetch(keys_to_fetch);

    // invoke the init callback
    data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<data_structures::ParallelCallbacks*>(data_structure);
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_worker(worker_id); }

    // wait for all threads to init
    barrier();
    (*startup_counter)--;
    while(*startup_counter > 0) /* nop */;
    barrier();

    while(distribution_window.m_count > 0){
        for(size_t i = 0; i < distribution_window.m_count; i++){
            int64_t key = distribution->get(distribution_window.m_position + i);
            int64_t value = key * 100;
            data_structure->insert(key, value);
        }

        // fetch the next chunk of the keys to insert
        distribution_window = distribution->fetch(keys_to_fetch);
    }

    // invoke the clean up callback
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_worker(worker_id); }

    // done
}

static
void thread_execute_scans(int worker_id, data_structures::Interface* data_structure, atomic<int>* startup_counter, uint64_t* output_num_elements_visited){
    pin_thread_to_socket();

    uint64_t num_elements_visited = 0; // return value
    assert(data_structures::global_parallel_scan_enabled == true && "Global variable to allow scans not initialised");
    assert(output_num_elements_visited != nullptr && "The output pointer is null");

    // invoke the init callback
    data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<data_structures::ParallelCallbacks*>(data_structure);
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_worker(worker_id); }

    // wait for all threads to init
    barrier();
    (*startup_counter)--;
    while(*startup_counter > 0) /* nop */;
    barrier();

    while(::data_structures::global_parallel_scan_enabled){
        auto scan = data_structure->sum(0, numeric_limits<int64_t>::max());
        num_elements_visited += scan.m_num_elements;
    }

    // invoke the clean up callback
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_worker(worker_id); }

    *output_num_elements_visited = num_elements_visited;
}

} // anonymous namespace


ParallelInsert::ParallelInsert(std::shared_ptr<data_structures::Interface> data_structure, uint64_t insert_threads, uint64_t scan_threads)
    : m_data_structure(data_structure), m_insert_threads(insert_threads), m_scan_threads(scan_threads) {
    if(data_structure.get() == nullptr) RAISE_EXCEPTION(ExperimentError, "Null pointer for the PMA interface");
}

ParallelInsert::~ParallelInsert() {

}

void ParallelInsert::preprocess() {
    if(m_data_structure.get() == nullptr) RAISE_EXCEPTION(ExperimentError, "Null pointer");

    // initialize the elements that we need to add
    LOG_VERBOSE("Generating the set of elements to insert ... ");
    m_distribution = distributions::generate_distribution();
}

void ParallelInsert::run() {
    // number of threads to start
    atomic<int> num_threads_to_start = m_insert_threads + m_scan_threads;

    // init the wrapper to fetch the keys from the distribution synchronously
    DistributionParallel distribution { m_distribution.get() };

    // allow scans to execute
    ::data_structures::global_parallel_scan_enabled = true;

    // threads managed
    std::vector<thread> threads;
    threads.reserve(num_threads_to_start);

    // record the number of elements visited in a scan, per each thread
    uint64_t num_elements_visited_per_thread[m_scan_threads];

    // invoke the callback for the init of the main thread
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(m_data_structure.get());
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_main(m_insert_threads + m_scan_threads); }

    // start the insertion threads
    LOG_VERBOSE("Starting `" << m_insert_threads << "' insertion threads ... ");
    for(size_t i = 0; i < m_insert_threads; i++){
        threads.emplace_back(thread_execute_inserts, /* worker id = */ (int) i, m_data_structure.get(), &distribution, &num_threads_to_start);
    }

    // start the scan threads
    LOG_VERBOSE("Starting `" << m_scan_threads << "' scan threads ... ");
    for(size_t i = 0; i < m_scan_threads; i++){
        threads.emplace_back(thread_execute_scans, /* worker id = */ (int) m_insert_threads + i, m_data_structure.get(), &num_threads_to_start, num_elements_visited_per_thread + i);
    }

    // wait for all threads to start
    LOG_VERBOSE("Waiting for all threads to start...");
    barrier();
    while( num_threads_to_start > 0) /* nop */;
    barrier();

    LOG_VERBOSE("Executing the experiment...");
    Timer timer { true }; barrier();

    // ... zzZz ...

    // wait for the experiment to complete
    for(size_t i = 0; i < m_insert_threads; i++){ threads[i].join(); }
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_complete(); } // flush the asynchronous updates
    ::data_structures::global_parallel_scan_enabled = false;
    for(size_t i = m_insert_threads; i < m_insert_threads + m_scan_threads; i++){ threads[i].join(); }

    // stop the timer
    barrier(); timer.stop(); barrier();

    // find the amount of elements visited in a scan
    uint64_t num_elements_visited = 0;
    for(size_t i = 0; i < m_scan_threads; i++){ num_elements_visited += num_elements_visited_per_thread[i]; }

    // invoke the clean up callback
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_main(); }

    config().db()->add("parallel_insert")
                    ("time_insert", timer.microseconds())
                    ("num_elements_scan", num_elements_visited);

    double seconds = timer.seconds();
    LOG_VERBOSE("Execution terminated, completion time: " << timer.seconds() << " seconds, "
            "insertion throughput (" << m_insert_threads << " threads): " << m_distribution->size() / seconds << ", "
            "scan throughput (" << m_scan_threads << " threads): " << num_elements_visited / seconds);
}


} /* namespace pma */
