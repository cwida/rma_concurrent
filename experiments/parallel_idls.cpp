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

#include "parallel_idls.hpp"

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <thread>


#include "common/configuration.hpp"
#include "common/database.hpp"
#include "common/miscellaneous.hpp" // pin_thread_to_cpu(), unpin_thread()
#include "common/spin_lock.hpp"
#include "data_structures/interface.hpp"
#include "data_structures/parallel.hpp"
#include "distributions/interface.hpp"

using namespace common;
using namespace std;

// Error
#define RAISE(message) RAISE_EXCEPTION(experiments::ExperimentError, message)

// Report the given message `preamble' together with the associated `time' in milliseconds
#define REPORT_TIME( preamble, time ) { std::cout << preamble << ' '; \
    if( time > 3000 ) { std::cout << (static_cast<double>(time) / 1000) << " seconds" << std::endl; } \
    else { std::cout << time << " milliseconds" << std::endl; } }

namespace experiments {

static void pin_thread_to_socket(){
#if defined(HAVE_LIBNUMA)
    pin_thread_to_numa_node(0);
#endif
}

//#define DEBUG
//static mutex _debug_mutex;
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[" << this_thread::get_id() << "][" << __FUNCTION__ << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


namespace {

class DistributionParallel {
    distributions::idls::Distribution<long>* m_distribution;
    SpinLock m_lock;

public:
    DistributionParallel(distributions::idls::Distribution<long>* distribution) : m_distribution(distribution) {
        assert(distribution != nullptr);
    }

    void fetch(std::vector<int64_t>& keys){
        int64_t count = 0;
        { // restrict the scope
            m_lock.lock();
            while(m_distribution->hasNext() && count < keys.size()){
                keys[count] = m_distribution->next();
                count++;
            }
            m_lock.unlock();
        }

        keys.resize(count);
    }

};

struct Task {
    enum class Type { UPDATE, SCAN_ALL };
    Type m_type;
    uint64_t m_payload;
};

} // anononymous namespce

static Task* _task_stop = reinterpret_cast<Task*>(0x01);

class ExperimentParallelIDLSThread {
public:
    uint64_t m_scan_elements = 0;
private:
    thread m_handle;
    data_structures::Interface* m_interface;
    bool m_started = false;
    Task* m_task = nullptr; // the current task to process
    condition_variable m_conditition_variable;
    mutex m_mutex;

    void main_thread(uint64_t worker_id){
        pin_thread_to_socket();

        data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<data_structures::ParallelCallbacks*>(m_interface);
        if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_worker(worker_id); }

        {
            unique_lock<mutex> lock(m_mutex);
            m_started = true;
            m_conditition_variable.notify_all();
        }

        while(true){
            { // fetch the next task to execute
                unique_lock<mutex> lock(m_mutex);
                m_conditition_variable.wait(lock, [this](){ return m_task != nullptr; });
            }
            if(m_task == _task_stop) break;

            COUT_DEBUG("Task fetched");
            do_execute();
            COUT_DEBUG("Task executed");

            {
                unique_lock<mutex> lock(m_mutex);
                delete m_task; m_task = nullptr;
                m_conditition_variable.notify_all();
            }
        }

        if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_worker(worker_id); }

        { // done
            unique_lock<mutex> lock(m_mutex);
            m_task = nullptr; // do not delete the stop task
            m_conditition_variable.notify_all();
        }
    }

    void do_execute(){
        switch(m_task->m_type){
        case Task::Type::UPDATE: {
            auto distribution = reinterpret_cast<DistributionParallel*>(m_task->m_payload);
            constexpr uint64_t keys_to_fetch = 8;
            std::vector<int64_t> keys; keys.resize(keys_to_fetch);

            distribution->fetch(keys);
            while(keys.size() > 0){
                for(int64_t key : keys){
                    if(key >= 0){ // insertion
                        COUT_DEBUG("Insert " << key);
                        m_interface->insert(key, key);
                    } else { // deletion
                        key = -key; // flip the sign
                        COUT_DEBUG("Remove " << key);
                        m_interface->remove(key);
                    }
                }

                // fetch the next chunk of keys to insert
                distribution->fetch(keys);
            }
        } break;
        case Task::Type::SCAN_ALL: {
            while(::data_structures::global_parallel_scan_enabled){
                auto scan = m_interface->sum(0, numeric_limits<int64_t>::max());
                m_scan_elements += scan.m_num_elements;
            }
        } break;
        default:
            assert(0 && "Invalid task");
        }
    }

public:

    ExperimentParallelIDLSThread(data_structures::Interface* interface, uint64_t id) : m_interface(interface) {
        m_handle = thread(&ExperimentParallelIDLSThread::main_thread, this, id);
        unique_lock<mutex> lock(m_mutex);
        if(!m_started){ m_conditition_variable.wait(lock, [this](){ return m_started; }); }
    }


    ~ExperimentParallelIDLSThread(){
        execute(_task_stop);
        m_handle.join();
    }


    void execute(Task* task){
        assert(m_task == nullptr && "A task is already on execution");
        unique_lock<mutex> lock(m_mutex);
        m_task = task;
        m_conditition_variable.notify_all();
    }

    void wait_done(){
        unique_lock<mutex> lock(m_mutex);
        if(m_task != nullptr) { m_conditition_variable.wait(lock, [this](){ return m_task == nullptr; }); };
    }
};


ParallelIDLS::ParallelIDLS(std::shared_ptr<data_structures::Interface> data_structure, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
    std::string insert_distribution, double insert_alpha,
    std::string delete_distribution, double delete_alpha,
    double beta, uint64_t seed,
    uint64_t insert_threads, uint64_t scan_threads) :
    m_data_structure(data_structure),
    N_initial_inserts(N_initial_inserts), N_insdel(N_insdel), N_consecutive_operations(N_consecutive_operations),
    m_distribution_type_insert(get_distribution_type(insert_distribution)), m_distribution_param_alpha_insert(insert_alpha),
    m_distribution_type_delete(get_distribution_type(delete_distribution)), m_distribution_param_alpha_delete(delete_alpha),
    m_distribution_param_beta(beta), m_distribution_seed(seed){

    if(beta <= 1){
        RAISE("Invalid value for the parameter --beta: " << beta << ". It defines the range of the distribution and it must be > 1");
    }

    m_insert_threads.resize(insert_threads);
    m_scan_threads.resize(scan_threads);
}

distributions::idls::eDistributionType ParallelIDLS::get_distribution_type(string value) const{
    if(value == "uniform"){
        return distributions::idls::eDistributionType::uniform;
    } else if (value == "zipf"){
        return distributions::idls::eDistributionType::zipf;
    } else {
        RAISE("Invalid distribution type: `" << value << "'");
    }
}

ParallelIDLS::~ParallelIDLS() {
    unpin_thread();
}

void ParallelIDLS::preprocess () {
    distributions::idls::Generator generator;
    generator.set_initial_size(N_initial_inserts);
    generator.set_insdel(N_insdel, N_consecutive_operations);
    generator.set_lookups(0);

    // distribution
    generator.set_distribution_type_init(distributions::idls::eDistributionType::uniform, 0);
    generator.set_distribution_type_insert(m_distribution_type_insert, m_distribution_param_alpha_insert);
    generator.set_distribution_type_delete(m_distribution_type_delete, m_distribution_param_alpha_delete);
    generator.set_distribution_range(m_distribution_param_beta);
    generator.set_seed(m_distribution_seed);


    // 8/5/2018
    // The IDLS distribution runs an (a,b)-tree to check and yields the keys to insert & delete, so that we are guaranteed that those
    // keys exist when the experiment runs. Furthermore, additional vectors are built, to store the generated keys. To avoid additional
    // overhead when running the actual experiment, the idea is to store all these side data structures in the secondary memory node (node=1),
    // while running the experiment on the first numa node (node=0).

    // if NUMA, make the keys in the secondary socket
    int numa_max_node = get_numa_max_node();
    if(numa_max_node >= 1){
        LOG_VERBOSE("Pinning to NUMA node 1");
        pin_thread_to_numa_node(1);
    }

    // generate the keys to use in the experiment
    LOG_VERBOSE("Generating the keys for the experiment...");
    m_keys_experiment = generator.generate();

    // now move to the first socket
    if(numa_max_node >= 1){
        LOG_VERBOSE("Pinning to NUMA node 0");
        pin_thread_to_numa_node(0);
    }
}

void ParallelIDLS::run_updates(distributions::idls::Distribution<long>* distribution){
    unique_ptr<DistributionParallel> parallel_distribution { new DistributionParallel( distribution ) };
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(m_data_structure.get());

    ::data_structures::global_parallel_scan_enabled = true;
    for(auto& thread : m_insert_threads){ thread->execute(new Task { Task::Type::UPDATE, reinterpret_cast<uint64_t>(parallel_distribution.get())} ); }
    for(auto& thread : m_scan_threads) { thread->execute(new Task { Task::Type::SCAN_ALL, 0ull }); }

    COUT_DEBUG("Waiting to complete...");

    for(auto& thread : m_insert_threads) { thread->wait_done(); }
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_complete(); } // flush the asynchronous updates
    ::data_structures::global_parallel_scan_enabled = false;
    for(auto& thread : m_scan_threads) { thread->wait_done(); }

    COUT_DEBUG("Done");
}

void ParallelIDLS::run_initial_inserts(){
    auto ptr = m_keys_experiment.preparation_step();
    auto distribution = ptr.get();
    run_updates(distribution);
    assert(m_data_structure->size() == N_initial_inserts);
}

void ParallelIDLS::run_insert_deletions(){
    auto ptr = m_keys_experiment.insdel_step();
    auto distribution = ptr.get();
    run_updates(distribution);
}

void ParallelIDLS::run() {
    // Create the threads
    ::data_structures::ParallelCallbacks* parallel_callbacks = dynamic_cast<::data_structures::ParallelCallbacks*>(m_data_structure.get());
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_init_main(m_insert_threads.size() + m_scan_threads.size()); }
    for(size_t i = 0; i < m_insert_threads.size(); i++){
        m_insert_threads[i] = new ExperimentParallelIDLSThread{ m_data_structure.get(), i };
    }
    for(size_t i = 0; i < m_scan_threads.size(); i++){
        m_scan_threads[i] = new ExperimentParallelIDLSThread{ m_data_structure.get(), m_insert_threads.size() + i };
    }

    // Perform the initial inserts
    Timer t_initial_inserts{true};
    run_initial_inserts();
    t_initial_inserts.stop();
    m_keys_experiment.unset_preparation_step(); // release some memory
    uint64_t num_initial_scan_elts = 0;
    for(auto scanner : m_scan_threads){ num_initial_scan_elts += scanner->m_scan_elements; scanner->m_scan_elements = 0;/* reset */ }

    double seconds = t_initial_inserts.microseconds() * 1000 * 1000;
    LOG_VERBOSE("Initial step: " << N_initial_inserts << " insertions. Elapsed time: " << seconds << " seconds, "
            "insertion throughput (" << m_insert_threads.size() << " threads): " << N_initial_inserts / seconds << ", "
            "scan throughput (" << m_scan_threads.size() << " threads): " << num_initial_scan_elts / seconds );

    // Perform the bulk of insertions and deletions
    Timer t_updates{true};
    run_insert_deletions();
    t_updates.stop();
    m_keys_experiment.unset_insdel_step(); // release some additional memory
    uint64_t num_bulk_scan_elts = 0;
    for(auto scanner : m_scan_threads){ num_bulk_scan_elts += scanner->m_scan_elements; scanner->m_scan_elements = 0;/* reset */ }
    LOG_VERBOSE("Update step: " << N_insdel << " updates in sequences of " << N_consecutive_operations << " operations. Elapsed time: " << seconds << " seconds, "
            "insertion throughput (" << m_insert_threads.size() << " threads): " << N_insdel / seconds << ", "
            "scan throughput (" << m_scan_threads.size() << " threads): " << num_initial_scan_elts / seconds );


    config().db()->add("parallel_idls")
                    ("initial_size", N_initial_inserts)
                    ("t_init_millisecs", t_initial_inserts.milliseconds<uint64_t>())
                    ("scan_init", num_initial_scan_elts)
                    ("updates", N_insdel)
                    ("t_updates_millisecs", t_updates.milliseconds<uint64_t>())
                    ("scan_updates", num_bulk_scan_elts)
                    ;


    // Destroy the threads
    for(auto& t : m_insert_threads){ delete t; t = nullptr; }
    for(auto& t : m_scan_threads){ delete t; t = nullptr; }
    if(parallel_callbacks != nullptr){ parallel_callbacks->on_destroy_main(); }
}

} // namespace experiments
