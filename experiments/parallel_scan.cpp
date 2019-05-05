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

#include "parallel_scan.hpp"

#include <atomic>
#include <random>
#include <thread>

#include "common/configuration.hpp"
#include "common/cpu_topology.hpp"
#include "common/database.hpp"
#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "common/timer.hpp"
#include "data_structures/interface.hpp"
#include "data_structures/iterator.hpp"
#include "data_structures/parallel.hpp"
#include "distributions/driver.hpp"
#include "distributions/interface.hpp"

#define RAISE(message) RAISE_EXCEPTION(experiments::ExperimentError, message)

using namespace std;
using namespace common;

namespace experiments {

namespace { // anonymous

/**
 * Base class for the container keys.
 */
struct ContainerKeys {
public:
    virtual ~ContainerKeys() { }

    // The number of keys in the container
    virtual uint64_t size() const = 0;

    // Get the key at the given position
    virtual int64_t key_at(size_t pos) const = 0;
    int64_t at(size_t pos) const { return key_at(pos); } // alias

    // Get the expected the sum of the keys for a scan in the interval [pos_min, pos_max]
    virtual uint64_t expected_sum(size_t pos_min, size_t pos_max) const = 0;
};

/**
 * Dense container. All the keys in [min, min + length), i.e. [min, min+1, min+2, ..., min + length -1]
 */
class ContainerKeysDense : public ContainerKeys {
    const int64_t m_minimum;
    const size_t m_length;

public:
    ContainerKeysDense(int64_t min, size_t length): m_minimum(min), m_length(length){ }

    uint64_t size() const { return m_length; }
    int64_t key_at(size_t pos) const {
        assert(pos < size());
        return m_minimum + (int64_t) pos;
    }
    uint64_t expected_sum(size_t pos_min, size_t pos_max) const {
        int64_t min = key_at(pos_min);
        int64_t max = key_at(pos_max);
        return (max * (max+1) - (min) * (min-1)) /2;
    }
};

/**
 * Sparse container. To validate it, we need to gather all keys container and build a prefix sum for validation purposes.
 * With 1G keys, it needs ~16GB of space.
 */
class ContainerKeysSparse : public ContainerKeys {
    vector<pair<int64_t, uint64_t>> m_keys;

public:

    ContainerKeysSparse(data_structures::Interface* data_structure){
        m_keys.reserve(data_structure->size());
        auto it = data_structure->iterator();
        int64_t i = 0;
        while(it->hasNext()){
            int64_t key = it->next().first;

            int64_t prefix_sum = key;
            if(i>0) prefix_sum += m_keys[i-1].second;

            m_keys.emplace_back(key, prefix_sum);
            i++;
        }
    }

    uint64_t size() const { return m_keys.size(); }

    int64_t key_at(size_t pos) const {
        assert(pos < size());
        return m_keys.at(pos).first;
    }

    uint64_t expected_sum(size_t pos_min, size_t pos_max) const {
        assert(pos_min <= pos_max && pos_max < size()); // range checks

        uint64_t result = m_keys[pos_max].second;
        if(pos_min > 0) result -= m_keys[pos_min -1].second;

        return result;
    }
};

void execute_workload(int worker_id, data_structures::Interface* data_structure, const ContainerKeys* keys, uint64_t seed, int64_t cpu_id, atomic<int>* const wait_to_start, uint64_t* output_result){
    pin_thread_to_cpu(cpu_id, /* do not print to stdout */ false);

    std::mt19937_64 random_generator(seed);
    size_t length = static_cast<int64_t>(0.01 * data_structure->size()); // 1%
    if(length < 1) length = 1;

//        std::cout << "interval_size: " << interval_sz << ", length: " << length << "\n";
    int64_t rstart = 0;
    int64_t rend = data_structure->size() - length;
    if(rend < rstart) { rend = rstart; length = data_structure->size() - rstart; } // [1, 1]
    uniform_int_distribution<size_t> distribution(rstart, rend);

    // invoke the init callback
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(data_structure);
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_worker(worker_id); }

    // spin lock, wait for all threads to start
    (*wait_to_start)--;
    while(*wait_to_start != 0) /* nop */;

    uint64_t num_elts_visited = 0;
    while(::data_structures::global_parallel_scan_enabled){
        size_t outcome = distribution(random_generator);
        auto pos_min = outcome;
        auto pos_max = outcome + length -1;
        auto min = keys->at(pos_min);
        auto max = keys->at(pos_max);
        auto result = data_structure->sum(min, max);
        num_elts_visited += result.m_num_elements;
    }

    // invoke the clean up callback
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_worker(worker_id); }

    *output_result = num_elts_visited; // output
}

} // anonymous namespace



ParallelScan::ParallelScan(shared_ptr<data_structures::Interface> data_structure, std::chrono::seconds execution_time) :
m_data_structure(data_structure), m_execution_time(execution_time) {
    if(execution_time.count() <= 0) RAISE("[ExperimentParallelScan::ctor] The execution time per simulation is zero");
}

ParallelScan::~ParallelScan() { }

void ParallelScan::preprocess() {
    if(m_data_structure.get() == nullptr) RAISE("Null pointer");
    if(m_data_structure->size() > 0)
        RAISE("The data structure is not empty. Size: " << m_data_structure->size());

    // initialize the elements that we need to add
    LOG_VERBOSE("Generating the set of elements to insert ... ");
    auto distribution = distributions::generate_distribution();

#if defined(HAVE_LIBNUMA)
    pin_thread_to_numa_node(0);
#endif

    // Insert the elements in the data structure
    int64_t key_min = numeric_limits<int64_t>::max();
    auto data_structure = m_data_structure.get();
    LOG_VERBOSE("Inserting " << distribution->size() << " elements in the data structure ...");
    Timer t_insert { true };
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(data_structure);
    if(parallel_callbacks != nullptr){
        auto num_threads = std::thread::hardware_concurrency();
        int64_t key_mins[num_threads];
        const int64_t keys_per_threads = distribution->size() / num_threads;
        const int64_t odd_threads =  distribution->size() % num_threads;

        parallel_callbacks->on_init_main(num_threads);
        vector<std::thread> threads;
        for(int j = 0; j < num_threads; j++){
            threads.emplace_back([&](int thread_id, int64_t* out_key_min_local){
                parallel_callbacks->on_init_worker(thread_id);
                int64_t key_from = thread_id * keys_per_threads + std::min<int64_t>(thread_id, odd_threads);
                int64_t key_to = key_from + keys_per_threads + (thread_id < odd_threads);
                int64_t key_min_local = numeric_limits<int64_t>::max();
                for(int64_t i = key_from; i < key_to; i++){
                    auto key = distribution->key(i);
                    key_min_local = min(key_min_local, key);
                    auto value = key * 10;
                    data_structure->insert(key, value);
                }
                parallel_callbacks->on_destroy_worker(thread_id);
                *out_key_min_local = key_min_local;
            }, j, key_mins + j);
        }

        for(int j = 0; j < num_threads; j++){
            threads[j].join();
            key_min = min(key_min, key_mins[j]);
        }

        parallel_callbacks->on_destroy_main();
    } else {
        for(size_t i = 0, sz = distribution->size(); i < sz; i++){
            auto key = distribution->key(i);
            key_min = min(key_min, key);
            auto value = key * 10;
            data_structure->insert(key, value);
        }
    }
    t_insert.stop();
    LOG_VERBOSE("# Insertion time (initial size): " << t_insert.milliseconds() << " millisecs");

    // `Build' time
    Timer timer_build(true);
    data_structure->build();
    timer_build.stop();
    if(timer_build.milliseconds() > 0){
        LOG_VERBOSE("# Build time: " << timer_build.milliseconds() << " millisecs");
    }

    // Container for the keys
    Timer timer_container(true);
    if(distribution->is_dense()){
        m_keys.reset(new ContainerKeysDense(key_min, data_structure->size()));
    } else {
        m_keys.reset(new ContainerKeysSparse(data_structure));
    }
    timer_container.stop();
    if(timer_container.milliseconds() > 0){
        LOG_VERBOSE("# Key mapping time: " << timer_container.milliseconds() << " millisecs");
    }

    unpin_thread();
}

static void log_execution_start(int numa_node, const vector<int>& cpu_ids, int num_threads){
    if(config().verbose()){
        stringstream stream;
        stream << "Executing on node: " << numa_node << ", threads: ";
        for(int i = 0; i < num_threads; i++){
            if(i > 0) stream << ", ";
            stream << cpu_ids[i];
        }
        LOG_VERBOSE(stream.str());
    }
}

void ParallelScan::run_on_threads(int numa_node, const vector<int>& cpu_ids, int num_threads){
    log_execution_start(numa_node, cpu_ids, num_threads);
    random_device rd;
    vector<thread> threads;
    uint64_t results[num_threads];
    atomic<int> wait_to_start = num_threads;

    // invoke the callback for the init of the main thread
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(m_data_structure.get());
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_main(num_threads); }

    // enable scans
    ::data_structures::global_parallel_scan_enabled = true;

    // execute the threads
    Timer t_workload {true};
    for(int i = 0; i < num_threads; i++){
        uint64_t seed = rd(); seed = (seed << 32) | rd();
        LOG_VERBOSE("CPU ID thread[" << i << "]: " << cpu_ids[i]);
        threads.emplace_back(execute_workload, i, m_data_structure.get(), m_keys.get(), seed, cpu_ids[i], &wait_to_start, results + i);
    }

    this_thread::sleep_for(m_execution_time);
    ::data_structures::global_parallel_scan_enabled = false;

    // wait for all threads to finish
    for(int i = 0; i < num_threads; i++){ threads[i].join(); }
    t_workload.stop();
    auto duration_secs = t_workload.milliseconds() / 1000;

    // invoke the clean up callback
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_main(); }

    // collect the results & compute the average
    uint64_t total = 0;
    for(int i = 0; i < num_threads; i++){
        // save the result
        config().db()->add("parallel_scan")
                        ("socket", numa_node)
                        ("num_threads", num_threads)
                        ("cpu_id", cpu_ids[i])
                        ("throughput_secs",results[i] / duration_secs);

        total += results[i];
    }

    config().db()->add("parallel_scan")
                    ("socket", numa_node)
                    ("num_threads", num_threads)
                    ("cpu_id", -1)
                    ("throughput_secs", total /duration_secs);

    LOG_VERBOSE("Execution terminated, throughput: " << (total /duration_secs) << " elts/sec");
}


void ParallelScan::run_on_all_threads(){
    auto thread_ids = get_cpu_topology().get_threads(true, /* SMT ? */ true);
    for(int i = 0, sz = thread_ids.size(); i < sz; i++){
        run_on_threads(-1, thread_ids, i +1);
    }
}

void ParallelScan::run_on_socket(int numa_node) {
    LOG_VERBOSE("Running on node: " << numa_node);

    // get all threads on the given socket
    auto thread_ids = get_cpu_topology().get_threads_on_node(numa_node, /* SMT ? */ false);

    for(int i = 0, sz = thread_ids.size(); i < sz; i++){
        run_on_threads(numa_node, thread_ids, i +1);
    }
}

void ParallelScan::run() {
    ::data_structures::global_parallel_scan_enabled = true; // enable scans

    run_on_all_threads();

#if defined(HAVE_LIBNUMA)
    if(get_numa_max_node() > 0){
        for(int i = 0; i <= get_numa_max_node(); i++){
            run_on_socket(i);
        }
    }
#endif
}

} /* namespace experiments */
