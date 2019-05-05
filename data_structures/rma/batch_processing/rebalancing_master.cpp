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

#include "rebalancing_master.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib> // abs, debug only
#include <iostream>
#include <mutex>
#include <sstream>

#include "common/configuration.hpp" // LOG_VERBOSE
#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "rma/common/static_index.hpp"
#include "garbage_collector.hpp"
#include "gate.hpp"
#include "packed_memory_array.hpp"
#include "rebalancing_task.hpp"
#include "rebalancing_worker.hpp"
#include "wakelist.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::batch_processing {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingMaster::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

RebalancingMaster::RebalancingMaster(PackedMemoryArray* pma, uint64_t num_workers) : m_instance(pma), m_thread_pool(num_workers){ }

RebalancingMaster::~RebalancingMaster() {
    stop();

#if defined(PROFILING)
    LOG_VERBOSE("[RebalancingMaster::dtor] Computing the rebalancing statistics....");
    auto t0 = chrono::steady_clock::now();
    auto stats = get_rebalancing_stastistics(m_stats_completed_tasks);
    auto t1 = chrono::steady_clock::now();
    cout << "Statistics computed in " << chrono::duration_cast<chrono::seconds>(t1 - t0).count() << " seconds\n";
    cout << stats << endl;;
#endif
}

void RebalancingMaster::start(){
    COUT_DEBUG("Starting the master...");

    scoped_lock<mutex> lock(m_mutex);
    if(m_handle.joinable()){ RAISE_EXCEPTION(Exception, "Main thread already started") };
    m_queue.clear();
    m_handle = thread(&RebalancingMaster::main_thread, this);
}

void RebalancingMaster::stop(){
    COUT_DEBUG("Stopping the master...");
    { // restrict the scope
        scoped_lock<mutex> lock(m_mutex);
        if(!m_handle.joinable()) return; // already stopped
        m_queue.append(InternalTask{InternalTask::Type::Stop, 0});
        m_condvar.notify_one();
    }
    m_handle.join();
}

RebalancingPool& RebalancingMaster::thread_pool(){
    return m_thread_pool;
}

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/
void RebalancingMaster::rebalance(uint64_t gate_id){
    {
        scoped_lock<mutex> lock(m_mutex);
        m_queue.append(InternalTask{InternalTask::Type::Rebalance, gate_id });
    }
    m_condvar.notify_one();
}

void RebalancingMaster::exit(uint64_t gate_id){
    {
        scoped_lock<mutex> lock(m_mutex);
        m_queue.append(InternalTask{InternalTask::Type::ClientExit, gate_id });
    }
    m_condvar.notify_one();
}

void RebalancingMaster::task_done(RebalancingTask* task){
    assert(task != nullptr && "Null pointer");
    {
        scoped_lock<mutex> lock(m_mutex);
        m_queue.append(InternalTask{InternalTask::Type::TaskDone, reinterpret_cast<uint64_t>(task) });
    }
    m_condvar.notify_one();
}


void RebalancingMaster::complete(){
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    COUT_DEBUG("Waiting for the rebalancer to become idle... ");
    {
        scoped_lock<mutex> lock(m_mutex);
        m_queue.append(InternalTask{InternalTask::Type::Wait2Complete, reinterpret_cast<uint64_t>(&producer)});
    }
    m_condvar.notify_one();

    consumer.wait(); // Zzz

    COUT_DEBUG("Done");
}

/*****************************************************************************
 *                                                                           *
 *   Controller thread                                                       *
 *                                                                           *
 *****************************************************************************/
void RebalancingMaster::main_thread(){
    COUT_DEBUG("Master node started");
    set_thread_name("RB Master");

    // we promised in the paper that all threads are pinned to the first socket
#if defined(HAVE_LIBNUMA)
    pin_thread_to_numa_node(0);
#endif

    bool stop_loop = false;
    m_thread_pool.start();

    do {
        InternalTask task;

        { // Fetch the next task from the queue
            unique_lock<mutex> lock(m_mutex);
            if(m_queue.empty()){ m_condvar.wait(lock, [this](){ return !m_queue.empty(); }); }
            assert(!m_queue.empty() && "Precondition not satified: there should be at least one item in the queue at this point");
            task = m_queue[0];
            m_queue.pop();
        }

        COUT_DEBUG("Task received: " << task.to_string());

        switch(task.m_type){
        case InternalTask::Type::Rebalance: {
            uint64_t gate_id = task.m_payload;
            assert(gate_id < m_instance->get_number_locks() && "Invalid gate ID");
            if(!m_resizing && !ignore_lock(gate_id)){
                RebalancingTask* task = rebal_init(gate_id);
                if(task != nullptr){ // task == nullptr => ignore this request
                    rebal_resume(task);

                    // append the task in the list of tasks to execute
                    m_todo.append(task);

                    // process the list of tasks
                    process_todo_list();
                }
            } // otherwise this gate is already going to be rebalanced
        } break;
        case InternalTask::Type::TaskDone: {
            // a task has been performed by a rebal worker
            RebalancingTask* rebal_task = reinterpret_cast<RebalancingTask*>(task.m_payload);
            IF_PROFILING( auto task_done_t0 = chrono::steady_clock::now() );

            // remove the task from the execution list
            auto it = std::find_if(begin(m_executing), end(m_executing), [rebal_task](const RebalancingTask* task){ return rebal_task == task; });
            assert(it != end(m_executing) && "Task not found ?");
            m_executing.erase(it);


            switch(rebal_task->m_plan.m_operation){
            case RebalanceOperation::REBALANCE:
            {
                // 1) update the todo list with the rebalances that cannot proceed
                for(size_t i = 0, sz = m_todo.size(); i < sz; i++){
                    if(m_todo[i] != nullptr && m_todo[i]->m_blocked_on_lock == rebal_task->get_lock_start()){
                        m_todo[i]->m_blocked_on_lock = -1;
                    }
                }
                // 2) unlock the client threads associated to the gates rebalanced
                WakeList worker_list;
                auto now = chrono::steady_clock::now();
                for(size_t i = rebal_task->get_lock_start(), end = rebal_task->get_lock_end(); i < end; i++){
                    release_lock(i, /* workspace */ worker_list, /* time of the last rebalance */ now);
                }
                // 3) go through the todo list
                process_todo_list();
            } break;
            case RebalanceOperation::RESIZE:
            case RebalanceOperation::RESIZE_REBALANCE:
            {
                while(!m_todo.empty() && m_todo[0] == nullptr) m_todo.pop(); // remove the nullptrs from the todo list
                assert(m_todo.empty() && "All gates should have been locked");
                m_resizing = false;

                // 1) Invalidate the old storage
                if(rebal_task->m_plan.m_operation == RebalanceOperation::RESIZE){
                    COUT_DEBUG("[Storage OLD] keys: " << m_instance->m_storage.m_keys << ", values: " << m_instance->m_storage.m_values << ", cardinalities: " << m_instance->m_storage.m_segment_sizes
                            << ", rw keys: " << m_instance->m_storage.m_memory_keys << ", rw values:" << m_instance->m_storage.m_memory_values << ", rw cardinalities: " << m_instance->m_storage.m_memory_sizes);
                    COUT_DEBUG("[Storage NEW] keys: " << rebal_task->m_ptr_storage->m_keys << ", values: " << rebal_task->m_ptr_storage->m_values << ", cardinalities: " << rebal_task->m_ptr_storage->m_segment_sizes
                            << ", rw keys: " << rebal_task->m_ptr_storage->m_memory_keys << ", rw values:" << rebal_task->m_ptr_storage->m_memory_values << ", rw cardinalities: " << rebal_task->m_ptr_storage->m_memory_sizes);

                    m_instance->m_storage = std::move(*(rebal_task->m_ptr_storage));
                    delete rebal_task->m_ptr_storage; rebal_task->m_ptr_storage = nullptr;
                }

                // 2) Set the time when the storage was created
                auto now = chrono::steady_clock::now();
                Gate* locks_new = rebal_task->m_ptr_locks;
                for(size_t i = 0, sz = rebal_task->get_lock_length(); i < sz; i++){
                    locks_new[i].m_time_last_rebal = now;
                }

                // 3) Install the new index & the group of locks
                size_t num_locks_old = rebal_task->m_num_locks;
                Gate* locks_old = m_instance->m_locks.get_unsafe();
                /* Gate* lock_new = ... // already initialised */
                assert(locks_old != locks_new);
                common::StaticIndex* index_old = m_instance->m_index.get_unsafe();
                common::StaticIndex* index_new = rebal_task->m_ptr_index;
                assert(index_old != index_new);

                m_instance->m_locks.timestamp() = m_instance->m_index.timestamp() = numeric_limits<uint64_t>::max();
                barrier();
                m_instance->m_locks.set(locks_new);
                m_instance->m_index.set(index_new);
                barrier();
                m_instance->m_locks.timestamp() = m_instance->m_index.timestamp() = rdtscp();

                // 4) Invalidate the old locks and unblock the threads
                WakeList worker_list;
                for(size_t i = 0; i < num_locks_old; i++){
                    cleanup_lock(locks_old[i],  /* workspace */ worker_list);
                }

                // 5) Mark the old data structures for garbage collection
                m_instance->GC()->mark(locks_old, [](Gate* ptr){ Gate::deallocate(ptr); });
                m_instance->GC()->mark(index_old);
            } break;
            default:
                assert(0 && "Invalid task type");
            }

            IF_PROFILING(auto task_done_t1 = chrono::steady_clock::now());
            IF_PROFILING(rebal_task->m_statistics.m_master_release_time = chrono::duration_cast<chrono::microseconds>(task_done_t1 - task_done_t0).count());
            IF_PROFILING(rebal_task->m_statistics.m_master_wallclock_time = chrono::duration_cast<chrono::microseconds>(task_done_t1 - rebal_task->m_statistics.m_time_init).count());
            IF_PROFILING(m_stats_completed_tasks.push_back(rebal_task->m_statistics));

            // release the memory for the task
            delete rebal_task; rebal_task = nullptr;

            // are there still threads waiting for the rebalancer to become idle?
            if(!busy()){
                for(auto p : m_wait2complete){ p->set_value(); }
                m_wait2complete.clear();
            }
        } break;
        case InternalTask::Type::ClientExit: {
            // a client thread has just released a gate/lock
            uint64_t lock_id = task.m_payload;
            COUT_DEBUG("ClientExit lock_id: " << lock_id);
            RebalancingTask* task = get_todo_task_for(lock_id);
            assert(task != nullptr && "Task associated to the given lock not found");
            wait_to_complete_remove(task, lock_id);
            if(task->ready_for_execution()){ process_todo_list(); }
        } break;
        case InternalTask::Type::Wait2Complete: {
            auto producer = reinterpret_cast<std::promise<void>*>(task.m_payload);
            if(!busy()){
                assert(m_wait2complete.empty() && "If the rebalancer is not busy, there should no other promises in the list ");
                producer->set_value();
            } else {
                m_wait2complete.push_back(producer);
            }
        } break;
        case InternalTask::Type::Stop: {
            assert(!m_thread_pool.active() && "Wrong termination order: all client threads must have terminated before invoking this method!");
            assert(m_executing.size() == 0 && "There should be no jobs on execution");
            m_thread_pool.stop();
            stop_loop = true;
        } break; // done
        default:
            assert(0 && "Invalid task");
        }

    } while(!stop_loop);

    m_thread_pool.stop();

    COUT_DEBUG("Master node stopped");
}

bool RebalancingMaster::ignore_lock(uint64_t lock_id) const {
    if(get_todo_task_for(lock_id) != nullptr){
        return true;
    }

    for(size_t i = 0, sz = m_executing.size(); i < sz; i++){
        if(m_executing[i]->is_superset_of(lock_id, 1))
            return true;
    }

    return false;
}

const RebalancingTask* RebalancingMaster::find_child_on_execution(uint64_t lock_start, uint64_t lock_length) const {
    for(size_t i = 0, sz = m_executing.size(); i < sz; i++){
        if(m_executing[i]->overlaps(lock_start, lock_length))
            return m_executing[i];
    }

    return nullptr;
}

RebalancingTask* RebalancingMaster::get_todo_task_for(size_t lock_id) const {
    for(size_t i = 0, sz = m_todo.size(); i < sz; i++){
        if(m_todo[i] != nullptr && m_todo[i]->is_superset_of(lock_id, 1))
            return m_todo[i];
    }

    return nullptr;
}

void RebalancingMaster::wait_to_complete_remove(RebalancingTask* task, uint64_t lock_id){
    assert(task != nullptr && "Null pointer");
    assert(task->is_superset_of(lock_id, 1) && "This task is not responsible for the given lock");

    auto it_wtc = std::find_if(begin(task->m_wait_to_complete), end(task->m_wait_to_complete), [lock_id](const RebalancingTask::WaitToComplete wtc){
       return wtc.m_lock_id == lock_id;
    });
    assert(it_wtc != end(task->m_wait_to_complete) && "The given lock was not registered");

    // the lock was released by a writer. Update the cardinality
    Gate& gate = m_instance->m_locks.get_unsafe()[lock_id];
    assert(it_wtc->m_cardinality != -1 && "Behaviour changed on 19/03/19, the old cardinality must be also saved for readers");
    int64_t cardinality_old = it_wtc->m_cardinality;
    // no need to lock the gate, it should be already in the REBAL state
    assert(gate.m_state == Gate::State::REBAL);
    int64_t cardinality_new = gate.m_cardinality;
    COUT_DEBUG("gate: " << gate.lock_id() << ", cardinality_old: " << cardinality_old << ", cardinality_new: " << cardinality_new << ", difference: " << (cardinality_new - cardinality_old) << ", task cardinality (before): " << task->m_plan.m_cardinality_before);
    task->m_plan.m_cardinality_before += (cardinality_new - cardinality_old);
    task->m_plan.m_cardinality_change += bulk_loading_init(task, &gate);

    // remove the lock from the waiting list
    task->m_wait_to_complete.erase(it_wtc);
}

RebalancingTask* RebalancingMaster::rebal_init(uint64_t lock_id){
    assert(lock_id < m_instance->get_number_locks() && "Invalid gate/lock ID");
    Gate* gate = m_instance->m_locks.get_unsafe() + lock_id;

    // Unfortunately we do still need to acquire a lock for this gate. Consider the case
    // 1. Writer A acquires the lock for Gate 1, set the flag to Rebal, unlocks the gate
    // 2. Writer B acquires the lock for Gate 2, and sends the rebalance request
    // 3. The Rebalancer handles the request by Writer B, rebalances both Gate 1 and Gate 2, unlocks both Writer A and Writer B
    // 4. Writer B wakes up and resumes
    // 5. Writer A finally sends an (obsolote) rebalancing request for Gate 1. However the state of the gate is not anymore REBAL because of step 3.

    unique_lock<Gate> llock(*gate);
    if(gate->m_state != Gate::State::REBAL){
        // this request is obsolete, the gate has been already rebalanced between the time the writer changed the state for the lock
        // and the request has been sent
        return nullptr;
    }

    RebalancingTask* task = new RebalancingTask(m_instance, this, gate);
    task->m_plan.m_cardinality_change += bulk_loading_init(task, gate);

    COUT_DEBUG("task: " << task);
    return task;
}


void RebalancingMaster::rebal_resume(RebalancingTask* task){
    COUT_DEBUG("task: " << task << ", # pma locks: " << m_instance->get_number_locks());
    assert(task != nullptr);
    IF_PROFILING( RebalancingTimer timer { task->m_statistics.m_master_search_time } );
    IF_PROFILING( task->m_statistics.m_master_num_resumes++ );

    const int64_t segments_per_lock = m_instance->get_segments_per_lock();
    const int64_t num_locks = m_instance->get_number_locks();
    const int64_t capacity_per_lock = segments_per_lock * m_instance->m_storage.m_segment_capacity;
    const int64_t window_id = task->m_window_id;
    int64_t lock_start = task->get_lock_start();
    int64_t lock_length = task->get_lock_length();
    assert(lock_length > 0);
    double height = log2(task->get_window_length()) +1.;
    uint64_t cardinality_before = task->m_plan.get_cardinality_before();
    uint64_t cardinality_after = task->m_plan.get_cardinality_after();

    int64_t index_left = lock_start -1;
    int64_t index_right = lock_start + lock_length;

    bool can_process = true;
    bool do_rebalance = false;

    // siblings
    std::vector<RebalancingTask*> siblings;
    siblings.reserve(m_todo.size());
    lock_length = next_window_length(lock_length);

    while(/*can_process &&*/ !do_rebalance && lock_length <= num_locks){
        height = log2(lock_length) +1.;

        int64_t lock_start_new = (window_id / static_cast<int64_t>(pow(2, (height -1)))) * lock_length;
        if(lock_start_new + lock_length >= num_locks){
            lock_start_new = num_locks - lock_length;
        } else if (lock_start_new > lock_start){ // when merging with other tasks, the window in the calibrator tree might be unaligned
            lock_start_new = lock_start;
        } else if (lock_start_new + lock_length < task->get_lock_end()){ // as above, due to merging, windows might get unaligned
            lock_start_new = task->get_lock_end() - lock_length;
        }

        COUT_DEBUG("(begin iteration) height: " << height << ", previous start position: " << lock_start << ", new start position: " << lock_start_new << ", window: [" << lock_start_new << ", " << lock_start_new + lock_length << ")");
        assert(lock_start_new <= lock_start);
        assert(lock_start_new + lock_length >= task->get_lock_end());
        lock_start = lock_start_new;
        int64_t lock_end = lock_start + lock_length;

        // can we execute this window?
        const RebalancingTask* execution_task = find_child_on_execution(lock_start, lock_length);
        if(execution_task != nullptr){
            // we cannot process this window now because a rebalance is currently on execution with a child of this window
            task->m_blocked_on_lock = execution_task->get_lock_start();
            can_process = false;
            COUT_DEBUG("cannot proceed, blocked by task: " << execution_task);
            break; // exit from the while loop
        }

        // corner case: can we merge tasks ?
        for(size_t i = 0, sz = m_todo.size(); i < sz; i++){
            if(m_todo[i] != nullptr && m_todo[i]->overlaps(lock_start, lock_length)){
                siblings.push_back(m_todo[i]);
                // adjust the current window
                COUT_DEBUG("merge with task: " << m_todo[i]);
                if(m_todo[i]->get_lock_start() < lock_start){
                    lock_length += (lock_start - m_todo[i]->get_lock_start());
                    lock_start = m_todo[i]->get_lock_start();
                }
                if(m_todo[i]->get_lock_end() > lock_start + lock_length){
                    lock_length = m_todo[i]->get_lock_end() - lock_start;
                }

                m_todo[i] = nullptr;
            }
        }
        lock_end = lock_start + lock_length; // update the end of the interval
        std::sort(begin(siblings), end(siblings), [](const RebalancingTask* t1, const RebalancingTask* t2){
            return t1->get_lock_start() < t2->get_lock_start();
        });

        // read the cardinality of the new window
        // proceed right to left
        RebalancingTask* last_sibling { nullptr };
        if(!siblings.empty()) { last_sibling = siblings.back(); siblings.pop_back(); }
        int64_t index = lock_end -1;
        COUT_DEBUG("window: [" << lock_start << ", " << lock_end << "), index_left: " << index_left << ", index_right: " << index_right << ", cardinality after: " << cardinality_after);
        while(index >= index_right){
            // merge with an overlapping task?
            if(UNLIKELY(last_sibling != nullptr && last_sibling->is_superset_of(index, 1))){
                COUT_DEBUG("[rhs] merge with task: " << last_sibling);
                for(size_t i = 0; i < last_sibling->m_wait_to_complete.size(); i++){
                    task->m_wait_to_complete.push_back(last_sibling->m_wait_to_complete[i]);
                }
                cardinality_before += last_sibling->m_plan.get_cardinality_before();
                cardinality_after += last_sibling->m_plan.get_cardinality_after();
                for(auto queue: last_sibling->m_blkld_elts){ task->m_blkld_elts.push_back(queue); }
                index = last_sibling->get_lock_start() -1;
                delete last_sibling; last_sibling = nullptr;
                if(!siblings.empty()) { last_sibling = siblings.back(); siblings.pop_back(); }
                IF_PROFILING( task->m_statistics.m_master_num_tasks_merged++ );
            } else {
                auto cards = acquire_lock(task, index);
                cardinality_before += cards.first;
                cardinality_after += cards.second;
                index--;
            }
        }
        index_right = lock_end; // for the next round
        index = index_left;
        while(index >= lock_start){
            if(UNLIKELY(last_sibling != nullptr && last_sibling->is_superset_of(index, 1))){
                COUT_DEBUG("[lhs] merge with task: " << last_sibling);
                for(size_t i = 0; i < last_sibling->m_wait_to_complete.size(); i++){
                    task->m_wait_to_complete.push_back(last_sibling->m_wait_to_complete[i]);
                }
                cardinality_before += last_sibling->m_plan.get_cardinality_before();
                cardinality_after += last_sibling->m_plan.get_cardinality_after();
                for(auto queue: last_sibling->m_blkld_elts){ task->m_blkld_elts.push_back(queue); }
                index = last_sibling->get_lock_start() -1;
                delete last_sibling; last_sibling = nullptr;
                if(!siblings.empty()) { last_sibling = siblings.back(); siblings.pop_back(); }
                IF_PROFILING( task->m_statistics.m_master_num_tasks_merged++ );
            } else {
                auto cards = acquire_lock(task, index);
                cardinality_before += cards.first;
                cardinality_after += cards.second;
                index--;
            }
        }
        index_left = lock_start -1; // for the next round

        // compute the density
        height = log2(segments_per_lock * lock_length) +1.;
        auto density_bounds = m_instance->get_thresholds(height);
        const int64_t window_capacity = lock_length * capacity_per_lock;
        int64_t cardinality_min = window_capacity * /* rho */ density_bounds.first;
        int64_t cardinality_max = window_capacity * /* tau */ density_bounds.second;

        COUT_DEBUG("(end iteration) cardinality (before/after): " << cardinality_before << "/" << cardinality_after << ", lock_start: " << lock_start << ", lock_length: " << lock_length << ", capacity_per_lock: " << capacity_per_lock);
        COUT_DEBUG("height: " << height << ", rho: " << density_bounds.first << ", density: " << static_cast<double>(cardinality_after) / static_cast<double>(lock_length * capacity_per_lock) << ", tau: " << density_bounds.second << ","
                " cardinality_min: " << cardinality_min << ", cardinality: " << cardinality_after << ", cardinality_max: " << cardinality_max << " (tree height: " << m_instance->m_storage.hyperheight() << ")");

        // save the current state
        task->set_lock_window(lock_start, lock_length);
        task->m_plan.m_cardinality_before = cardinality_before;
        task->m_plan.m_cardinality_change = cardinality_after - cardinality_before;
        task->m_cardinality_min = cardinality_min;
        task->m_cardinality_max = cardinality_max;

        if(cardinality_min <= cardinality_after && cardinality_after <= cardinality_max && !task->m_forced_resize /* => cardinality < capacity/2, that is we are wasting too much space, prefer a resize */) {
            do_rebalance = true;
        } else {
            if(lock_length == num_locks) break;
            lock_length = next_window_length(lock_length);
            assert(lock_length <= num_locks);
        }
    } // while loop

    if(can_process){
        if(!do_rebalance){
            assert(height >= m_instance->m_storage.height());
            task->m_plan.m_operation = RebalanceOperation::RESIZE; // it might actually become RESIZE_REBALANCE
        } else {
            task->m_plan.m_operation = RebalanceOperation::REBALANCE;
        }

        COUT_DEBUG("do_rebalance: " << do_rebalance << ", final task: " << task);
    }
}


void RebalancingMaster::launch_task(RebalancingWorker* worker, RebalancingTask* task){
    assert(worker != nullptr && "Null pointer");
    debug_validate_launch_task_cardinality(task);
    IF_PROFILING( RebalancingTimer timer { task->m_statistics.m_master_launch_time } );

    // at this point, the task can either be RESIZE or REBALANCE (but not RESIZE_REBALANCE)
    if(task->m_plan.m_operation == RebalanceOperation::RESIZE) m_resizing = true;

    COUT_DEBUG("density: " << static_cast<double>(task->m_plan.get_cardinality_after()) / task->m_ptr_storage->capacity() << ", threshold: " << m_instance->get_thresholds().densities().theta_h);

    m_instance->rebalance_plan(&(task->m_plan));
    task->m_pma->set_thresholds(task->m_plan); // update the thresholds in the calibrator tree
    task->m_num_locks = m_instance->get_number_locks(); // always set to the previous number of locks/gates

    auto operation = task->m_plan.m_operation;
    if(operation == RebalanceOperation::RESIZE || operation == RebalanceOperation::RESIZE_REBALANCE){
        assert(m_executing.empty() && "There should be no other tasks in execution while resizing");

        // update the index & the number of gates
        task->m_ptr_index = new common::StaticIndex(m_instance->m_index.get_unsafe()->node_size(), task->get_lock_length());
        task->m_ptr_locks = Gate::allocate(task->get_lock_length(), m_instance->get_segments_per_lock());

        // update the storage
        if(operation == RebalanceOperation::RESIZE){
            task->m_ptr_storage = new Storage(m_instance->m_storage.m_segment_capacity, m_instance->m_storage.m_pages_per_extent, task->get_window_length());
        } else { // RebalanceOperation::RESIZE_REBALANCE
            assert(task->m_plan.m_window_length >= m_instance->m_storage.m_number_segments);
            task->m_ptr_storage->extend(task->m_plan.m_window_length - m_instance->m_storage.m_number_segments);
        }

        // profiling statistics
#if defined(PROFILING)
        if(task->get_window_length() >= m_instance->m_storage.m_number_segments){
            task->m_statistics.m_type = RebalancingStatistics::Type::UPSIZE;
        } else {
            task->m_statistics.m_type = RebalancingStatistics::Type::DOWNSIZE;
        }
#endif
    }

#if defined(DEBUG)
    switch(task->m_plan.m_operation){
    case RebalanceOperation::RESIZE:
        COUT_DEBUG("Task dispatched: RESIZE " << task->m_num_locks * m_instance->get_segments_per_lock() << " -> " << task->get_window_length());
        break;
    case RebalanceOperation::RESIZE_REBALANCE:
        COUT_DEBUG("Task dispatched: RESIZE_REBALANCE " << task->m_num_locks * m_instance->get_segments_per_lock() << " -> " << task->get_window_length());
        break;
    case RebalanceOperation::REBALANCE:
        COUT_DEBUG("Task dispatched: REBALANCE [" << task->get_window_start() << ", " << task->get_window_end() << ")");
        break;
    default:
        COUT_DEBUG("Task dispatched: ???");
        break;
    }
#endif

    IF_PROFILING(task->m_statistics.m_window_length = task->m_plan.m_window_length);

    m_executing.push_back(task);
    worker->execute(task);
}


void RebalancingMaster::process_todo_list(){
    bool workers_available = true;

    // go through the whole list of tasks to be processed
    for(size_t i = 0, sz = m_todo.size(); i < sz; i++){
        bool task_in_execution = false;

        RebalancingTask* task = m_todo[0];
        m_todo.pop();
        if(task == nullptr) continue; // ignore

        if(workers_available && task->m_blocked_on_lock == -1){
            if(!task->is_rebalancing_window_computed()){ rebal_resume(task); }

            if(task->ready_for_execution()){
                RebalancingWorker* worker = m_thread_pool.acquire();
                if(worker == nullptr){ // there are no threads available at the moment to execute this task
                    workers_available = false;
                } else {
                    launch_task(worker, task);
                    task_in_execution = true;
                }
            }
        }

        // add the task back at the end of the queue
        if(!task_in_execution){
            m_todo.append(task);
        }
    }
}

std::pair<uint64_t, uint64_t> RebalancingMaster::acquire_lock(RebalancingTask* task, uint64_t lock_id){
    assert(task != nullptr && "Null pointer");
    assert(lock_id < m_instance->get_number_locks() && "Invalid gate/lock ID");
    Gate* gate = m_instance->m_locks.get_unsafe() + lock_id;
    uint64_t num_elts_bulk_loading {0}; // bulk loaded elements

    // first acquire the lock for the gate
    gate->lock();

    // update the state of this gate
    auto previous_state = gate->m_state;
    gate->m_state = Gate::State::REBAL;

    // mark this task on wait
    switch(previous_state){
    case Gate::State::READ: // read the cardinality again because readers can indeed perform pending deletions
    case Gate::State::WRITE:
    case Gate::State::TIMEOUT: // the last reader or writer will now invoke #exit(gate_id) because gate->m_state == REBAL
        task->m_wait_to_complete.push_back({ lock_id, gate->m_cardinality });
        break;
    default:
        assert(gate->m_num_active_threads == 0 && "There are still workers operating on this gate");

        // add elts to insert in bulk from the writer's queue
        num_elts_bulk_loading = bulk_loading_init(task, gate);
    }

    // read the cardinality of the gate
    uint64_t cardinality = gate->m_cardinality;

//    COUT_DEBUG("lock_id: " << lock_id << ", cardinality: " << cardinality);

    // release the lock
    gate->unlock();

    return std::pair<uint64_t, uint64_t>{cardinality, cardinality + num_elts_bulk_loading};
}

void RebalancingMaster::release_lock(uint64_t lock_id, WakeList& worker_list, chrono::steady_clock::time_point time_last_rebal){
    assert(lock_id < m_instance->get_number_locks() && "Invalid gate/lock ID");
    Gate* gate = m_instance->m_locks.get_unsafe() + lock_id;

    // acquire the spin lock associated to this gate
    gate->lock();
    assert(gate->m_state == Gate::State::REBAL && "This gate was supposed to be acquired previously");
    assert(gate->m_num_active_threads == 0 && "This gate should be closed for rebalancing");
    assert(gate->m_async_queue == nullptr && "We should have already cleared the asynchronous queue");

    gate->m_state = Gate::State::FREE;
    gate->m_time_last_rebal = time_last_rebal;

    // Use #wake_all rather than #wake_next! Potentially the fence keys have been changed, to threads
    // upon wake up might move to other gates. If other threads are in the wait list, they
    // might potentially end up blocked forever.
    gate->wake_all(/* out */ worker_list);

    // done
    gate->unlock();

    worker_list();
}

void RebalancingMaster::cleanup_lock(Gate& gate, WakeList& worker_list){
    gate.lock();
    assert(gate.m_async_queue == nullptr && "We should have reset this field already in #bulk_loading_init");

    gate.m_fence_low_key = gate.m_fence_high_key = numeric_limits<int64_t>::min();
    gate.wake_all(/* out */ worker_list);

    gate.unlock();

    worker_list();
}

int64_t RebalancingMaster::next_window_length(int64_t current_window_length) const {
    int64_t next_length = hyperceil(current_window_length);
    if(next_length == current_window_length){
        next_length *= 2;
    }

    if(next_length > m_instance->get_number_locks())
        next_length = m_instance->get_number_locks();

    return next_length;

}

uint64_t RebalancingMaster::bulk_loading_init(RebalancingTask* task, Gate* gate){
    assert(task != nullptr && gate != nullptr && "Null pointers");
    assert(gate->m_num_active_threads == 0 && "There should be no client threads active on this gate");
    assert(gate->m_state == Gate::State::REBAL && "The state of the gate should have been switched to REBAL");

    // is there an asynchronous queue associated to this gate?
    ClientContextQueue* async_queue = gate->m_async_queue;
    if(async_queue == nullptr) return 0;
    gate->m_async_queue = nullptr; // reset the value of the async queue

    // Perform the remaining deletions
    auto& deletions = async_queue->deletions();
    if(!deletions.empty()){
        std::sort(begin(deletions), end(deletions)); // better caching
        int64_t num_deletions = 0;
        for(auto& key : deletions){
            int64_t value = -1;
            // ignore the return value, we are already rebalancing
            m_instance->do_remove(gate, key, /* output */ &value);
            num_deletions += (value != -1);
        }
        deletions.clear();

        assert(static_cast<int64_t>(gate->m_cardinality) - num_deletions >= 0 && "Negative cardinality");
        if(num_deletions > 0){
//            COUT_DEBUG("lock: " << gate->lock_id() << ", deletions: " << num_deletions);
            gate->m_cardinality -= num_deletions;
            task->m_plan.m_cardinality_before -= num_deletions;
            // m_instance->m_cardinality -= num_deletions; // BUG: do_remove already updates the global cardinality of the pma
        }
    }

    auto num_insertions = async_queue->insertions().size();
    if(num_insertions == 0){ // done
        delete async_queue; async_queue = nullptr;
    } else {
        task->m_blkld_elts.push_back(async_queue);
    }

    return num_insertions;
}

bool RebalancingMaster::busy() const {
    return !m_todo.empty() || !m_executing.empty();
}

void RebalancingMaster::debug_validate_launch_task_cardinality(RebalancingTask* task) const{
#if !defined(NDEBUG)
    assert(task != nullptr && "Null pointer");
    assert(task != nullptr && "Null pointer");
    assert(task->ready_for_execution() && "Cannot execute this task yet");
    assert(task->is_rebalancing_window_computed() && "The final window to rebalance has not been computed yet");
    assert(task->m_plan.m_operation == RebalanceOperation::RESIZE || task->m_plan.m_operation == RebalanceOperation::REBALANCE); // It cannot be REBALANCE_RESIZE at this stage

    const bool is_resize = (task->m_plan.m_operation == RebalanceOperation::RESIZE);

    // locks cardinality
    int64_t cardinality_locks = 0;
    for(int64_t lock_id = task->get_lock_start(); lock_id < task->get_lock_end(); lock_id++){
        assert(task->m_ptr_locks[lock_id].m_num_active_threads == 0 && "This gate should be fully closed");
        cardinality_locks += task->m_ptr_locks[lock_id].m_cardinality;
    }

    // segments cardinality
    int64_t cardinality_segments = 0;
    const int64_t segment_max_capacity = m_instance->m_storage.m_segment_capacity;
    for(int64_t segment_id = task->get_window_start(); segment_id < task->get_window_end(); segment_id++){
        int64_t cardinality_segment = m_instance->m_storage.m_segment_sizes[segment_id];
        assert(cardinality_segment <= segment_max_capacity && "The segment cardinality is greater than its maximum capacity");
        cardinality_segments += cardinality_segment;
    }

    if(cardinality_locks != cardinality_segments || cardinality_locks != task->m_plan.get_cardinality_before()){
#if defined(DEBUG)
        for(size_t i = task->get_lock_start(); i < task->get_lock_end(); i++){
            COUT_DEBUG("cardinality gate[" << i << "]: " << task->m_ptr_locks[i].m_cardinality);
        }
#endif

        COUT_DEBUG_FORCE("--- CARDINALITY MISMATCH: task: " << task << ", cardinality gates: " << cardinality_locks << ", cardinality segments: " << cardinality_segments << ", cardinality plan (before): " << task->m_plan.get_cardinality_before());

        assert(false && "cardinality mismatch");
    }

    if(is_resize){
        COUT_DEBUG("[resize] storage number of locks: " << m_instance->get_number_locks() << ", task #locks: " << task->get_lock_length());
        assert(task->get_lock_start() == 0 && m_instance->get_number_locks() == task->get_lock_length());
        COUT_DEBUG("[resize] global cardinality: " << m_instance->m_cardinality << ", task cardinality (after): " << task->m_plan.get_cardinality_after());

        if(m_instance->m_cardinality != task->m_plan.get_cardinality_before())
            COUT_DEBUG_FORCE("--- CARDINALITY MISMATCH RESIZE: task: " << task << ", cardinality plan (before): " << task->m_plan.get_cardinality_before() << ", cardinality pma: " << m_instance->m_cardinality);
        assert(m_instance->m_cardinality == task->m_plan.get_cardinality_before() && "Cardinality mismatch"); // as above
    }

    // Bulk Loading cardinality
    size_t bulk_loading_cardinality = 0;
    for(size_t i = 0; i < task->m_blkld_elts.size(); i++){
        assert(task->m_blkld_elts[i]->deletions().empty() && "There should be no pending deletions");
        assert(!task->m_blkld_elts[i]->insertions().empty() && "There should be at least one pending insertion");
        bulk_loading_cardinality += task->m_blkld_elts[i]->insertions().size();
    }
    if(task->m_plan.get_cardinality_before() + bulk_loading_cardinality != task->m_plan.get_cardinality_after()){
        COUT_DEBUG_FORCE("--- CARDINALITY MISMATCH BULK LOADING: before: " << task->m_plan.get_cardinality_before() << ", after: " << task->m_plan.get_cardinality_after() << ", bulk loader cardinality: " << bulk_loading_cardinality);
        assert(false && "Cardinality mismatch"); // as above
    }

    COUT_DEBUG("Cardinality check OK");
#endif
}

string RebalancingMaster::InternalTask::to_string() const {
    stringstream stream;
    switch(m_type){
    case Type::Invalid:
        stream << "invalid"; break;
    case Type::Rebalance:
        stream << "rebalance gate: " << m_payload; break;
    case Type::TaskDone: {
        stream << "task completed: " << reinterpret_cast<RebalancingTask*>(m_payload); break;
    } break;
    case Type::ClientExit:
        stream << "gate unlocked: " << m_payload; break;
    case Type::Stop:
        stream << "terminate"; break;
    case Type::Wait2Complete:
        stream << "wait2complete"; break;
    default:
        stream << "???";
    }

    return stream.str();
}

} // namespace
