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

#include "rebalancing_worker.hpp"

#include <cassert>
#include <chrono> // debug only
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>
#include <vector> // check all

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "rma/common/buffered_rewired_memory.hpp"
#include "rma/common/rewired_memory.hpp"
#include "gate.hpp"
#include "packed_memory_array.hpp"
#include "rebalancing_master.hpp"
#include "rebalancing_pool.hpp"
#include "storage.hpp"
#include "thread_context.hpp"

using namespace common;
using namespace data_structures::rma::common;
using namespace std;

namespace data_structures::rma::batch_processing {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingWorker::" << __FUNCTION__ << "] [" << get_thread_id() << ", worker_id: " << m_worker_id << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

// Check the content of the input & output arrays, whether the same items in the array
// are present after a rebalancing
//#define DEBUG_CONTENT
// the macro DEBUG implicitly defines DEBUG_CONTENT
#if defined(DEBUG) & !defined(DEBUG_CONTENT)
#define DEBUG_CONTENT
#endif
#if defined(DEBUG_CONTENT)
thread_local std::vector<std::tuple<int64_t, int64_t, int64_t>> _debug_items;
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

static RebalancingTask* const FLAG_STOP = reinterpret_cast<RebalancingTask*>(0x1);

RebalancingWorker::RebalancingWorker() : m_task(nullptr), m_worker_id(-1) {

}

RebalancingWorker::~RebalancingWorker() {
    stop();
}

void RebalancingWorker::start() {
    COUT_DEBUG("Starting...");
    scoped_lock<mutex> lock(m_mutex);
    assert(m_task == nullptr && "There should be no task attached");
    if(m_handle.joinable()) RAISE_EXCEPTION(Exception, "Already on execution");
    m_handle = thread(&RebalancingWorker::main_thread, this);
}

void RebalancingWorker::stop(){
    COUT_DEBUG("Stopping...");
    unique_lock<mutex> lock(m_mutex);
    assert(m_task == nullptr && "There should be no task attached");
    if(!m_handle.joinable()) return; // already stopped
    m_task = FLAG_STOP;
    lock.unlock();
    m_condition_variable.notify_one();
    m_handle.join(); // wait for the thread to complete
    m_task = nullptr;
}

void RebalancingWorker::execute(RebalancingTask* task){
    execute0(task, 0);
}

void RebalancingWorker::execute0(RebalancingTask* task, int64_t worker_id){
    assert(worker_id >= 0 && "Invalid worker ID");
    unique_lock<mutex> lock(m_mutex);
    if(m_task != nullptr) RAISE_EXCEPTION(Exception, "A task is already on execution.");
    m_task = task;
    m_worker_id = worker_id;
    m_condition_variable.notify_one();
    lock.unlock();
}

void RebalancingWorker::main_thread() {
    COUT_DEBUG("Started");
    set_thread_name(string("RB Worker ") + to_string(get_thread_id()));

#if defined(HAVE_LIBNUMA)
    pin_thread_to_cpu(0, /* verbose */ false);
#endif

    unique_lock<mutex> lock(m_mutex);
    while(true){
        m_condition_variable.wait(lock, [this](){ return m_task != nullptr; });
        if(m_task == FLAG_STOP) break;
        assert(m_task != nullptr && "Precondition not satisfied: a task should have been set");
        do_execute();

        // ptr to the thread pool
        RebalancingTask* submit_task_done = (m_worker_id == 0) ? m_task : nullptr;
        RebalancingMaster* master = m_task->m_master;
        RebalancingPool& thread_pool = master->thread_pool();

        // cleanup its state for the next task
        m_task = nullptr;
        m_worker_id = -1;

        thread_pool.release(this); // done
        if(submit_task_done != nullptr){
            master->task_done(submit_task_done);
        }
    }

    COUT_DEBUG("Stopped");
}

/*****************************************************************************
 *                                                                           *
 *   Execute the task                                                        *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::do_execute() {
    assert(m_task != nullptr && "Task not provided");
    COUT_DEBUG("Task in execution: " << m_task);

    if(m_worker_id == 0){
        IF_PROFILING( RebalancingTimer timer_worker_total { m_task->m_statistics.m_worker_total_time } );

        sort_blkload_elts();
        debug_content_before();

        // a lock/gate can be smaller than an extent. For instance a gate is 4 segments, while an extent up to 16.
        // In this case just use a single worker to rebalance/resize the involved gate(s).
        if(m_task->get_extent_length() == 0){ // we cannot rewire anything
            do_execute_single();
        } else {
            make_subtasks(); // create the list of subtasks

            m_task->m_active_workers = 1; // myself
            do_execute_queue();

            // wait for all workers to complete
            if(m_task->m_active_workers > 0){ // restrict the scope
                unique_lock<mutex> lock(m_task->m_workers_mutex);
                // it may happen that the variable ``m_task->m_active_workers'' is decreased by another worker
                // just after the guard is checked and just before the condition variable is acquired. In this case
                // sleep for 1 millisecond, the next time the guard is checked again, it will be false.
                while(m_task->m_active_workers > 0){ m_task->m_workers_condvar.wait_for(lock, 1ms); } // 1 millisecond
            }
            assert(m_task->m_subtasks.empty() && "All subtasks should have been executed");
        }

        // Finish by setting the segment cardinalities of the window just rebalanced
        update_segment_cardinalities();

        debug_content_after();

        // Remove all elts from the bulk loading queues
        clear_blkload_queues();
    } else { // worker_id > 0
        do_execute_queue();
    }

    COUT_DEBUG("Done");
}

/*****************************************************************************
 *                                                                           *
 *   Execute a single pass                                                   *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::do_execute_single(){
    IF_PROFILING( auto t0 = chrono::steady_clock::now() );

    switch(m_task->m_plan.m_operation){
    case RebalanceOperation::REBALANCE: {
        spread_local();
    } break;
    case RebalanceOperation::RESIZE_REBALANCE: {
        assert(0 && "Wrong branch: this kind of operation should have been performed through rewiring & the subtasks mechanism");
    } break;
    case RebalanceOperation::RESIZE: {
        auto* input_storage = &(m_task->m_pma->m_storage);
        auto* output_storage = m_task->m_ptr_storage;
        BulkLoadingIterator loader { m_task };

        resize(/* input storage: */ input_storage, /* output storage: */ output_storage,
               /* input start position: */ input_storage->m_segment_capacity - input_storage->m_segment_sizes[0],
               /* input end position (just for boundary check): */ input_storage->m_number_segments * input_storage->m_segment_capacity,
               /* bulk loading loader */ loader,
               /* output segment id: */ 0, /* output number of segments: */ m_task->m_plan.m_window_length,
               /* total cardinality: */ m_task->m_plan.get_cardinality_after()
        );
    } break;
    default:
        assert(0 && "Invalid case");
    }

    IF_PROFILING( auto t1 = chrono::steady_clock::now() );
    IF_PROFILING( m_task->m_statistics.m_worker_task_exec_time.push_back( (int64_t) chrono::duration_cast<chrono::microseconds>(t1 - t0).count() ));
}

/*****************************************************************************
 *                                                                           *
 *   Execute from the queue                                                  *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::make_subtasks(){
    constexpr size_t EXTENTS_PER_SUBTASK = 4; // at least 8 MB => 128k slots
    Storage& storage_input = m_task->m_pma->m_storage;
    const int64_t segments_per_extent = storage_input.get_segments_per_extent();
    const size_t segment_capacity = storage_input.m_segment_capacity;
    const int64_t window_end_before = m_task->m_plan.m_operation == RebalanceOperation::REBALANCE ? m_task->get_window_end() : storage_input.m_number_segments;
    InputIterator position { storage_input, m_task->get_window_start(), window_end_before };
    const int64_t num_subtasks = max<int64_t>(m_task->get_extent_length() / EXTENTS_PER_SUBTASK, 1); // at least one subtask
    BulkLoadingIterator loader { m_task };


    if(num_subtasks == 1){
        RebalancingTask::SubTask subtask;
        subtask.m_input_position_start = position.get_absolute_position_start();
        subtask.m_input_position_end = position.get_absolute_position_end();
        subtask.m_input_extent_start = m_task->get_extent_start();
        subtask.m_input_extent_end = window_end_before / segments_per_extent;
        subtask.m_output_extent_start = m_task->get_extent_start();
        subtask.m_output_extent_end = m_task->get_extent_start() + m_task->get_extent_length();
        subtask.m_blkload_start = loader.get_absolute_position_start();
        subtask.m_blkload_end = loader.get_absolute_position_end();
        subtask.m_cardinality = m_task->m_plan.get_cardinality_after();
        COUT_DEBUG("Single subtask: " << subtask);
        m_task->m_subtasks.push_back(subtask);
        IF_PROFILING( m_task->m_statistics.m_worker_num_subtaks = 1 );
    } else {
        IF_PROFILING( RebalancingTimer timer { m_task->m_statistics.m_worker_make_subtasks_time } );

#if defined(DEBUG_CONTENT) // we need the vector _debug_items to double check whether the created intervals are valid
        int64_t debug_cardinality_count = 0;
        int64_t debug_loader_last = 0;
#endif

        // extents per subtask
        assert(m_task->get_extent_length() > 1 && "Otherwise it should create only a single subtask");
        int64_t extents_per_subtask = m_task->get_extent_length() / num_subtasks;
        int64_t num_odd_subtasks = m_task->get_extent_length() % num_subtasks;

        // cardinality per extent
        int64_t cardinality_per_segment = m_task->m_plan.get_cardinality_after() / m_task->get_window_length();
        int64_t num_odd_segments = m_task->m_plan.get_cardinality_after() % m_task->get_window_length();

        for(int64_t subtask_id = 0; subtask_id < num_subtasks; subtask_id++){
            RebalancingTask::SubTask subtask;
            // output window
            subtask.m_output_extent_start = (subtask_id == 0) ? m_task->get_extent_start() : m_task->m_subtasks.back().m_output_extent_end;
            int64_t subtask_num_extents = extents_per_subtask + (subtask_id < num_odd_subtasks);
            assert(subtask_num_extents >= 1);
            subtask.m_output_extent_end = subtask.m_output_extent_start + subtask_num_extents;

            // cardinality
            int64_t subtask_num_segments = subtask_num_extents * segments_per_extent;
            const int64_t subtask_cardinality_leftover = min(num_odd_segments, subtask_num_segments);
            num_odd_segments -= subtask_cardinality_leftover;
            subtask.m_cardinality = cardinality_per_segment * subtask_num_segments + subtask_cardinality_leftover;

            // input
            position.fetch_next_chunk();
            COUT_DEBUG("subtask_id: " << subtask_id << ", initial position: " << position);
            subtask.m_input_position_start = position.get_absolute_position_current();
            subtask.m_input_extent_start = subtask.m_input_position_start / segment_capacity / segments_per_extent;
            position += subtask.m_cardinality;

            // merge the elements with those in the bulk loader
            COUT_DEBUG("subtask_id: " << subtask_id << ", after cardinality increment: " << position);
            subtask.m_blkload_start = loader.get_absolute_position_current();
            position--; // move one step back
            COUT_DEBUG("subtask_id: " << subtask_id << ", before blk loading: " << position);
            while(position.get_key() > loader.get().first){
                COUT_DEBUG("subtask_id: " << subtask_id << ", move back: " << position << ", key input: " << position.get_key() << ", key blk: " << loader.get().first);
                position--;
                loader++;
            }
            COUT_DEBUG("subtask_id: " << subtask_id << ", after blk loading: " << position << ", key input: " << position.get_key() << ", key blk: " << loader.get().first);
            position++; // move one step forward again
            COUT_DEBUG("subtask_id: " << subtask_id << ", final position: " << position);
            subtask.m_blkload_end = loader.get_absolute_position_current();
            assert(subtask.m_blkload_end >= subtask.m_blkload_start);

            subtask.m_input_position_end = position.get_absolute_position_current();
            subtask.m_input_extent_end = subtask.m_input_position_end / segment_capacity / segments_per_extent;

            COUT_DEBUG("Multiple subtask #" << subtask_id << ": " << subtask);
#if defined(DEBUG_CONTENT)
            assert(_debug_items.size() >= debug_cardinality_count + subtask.m_cardinality && "There are not enough elements");

            int64_t _debug_loader_start = -1;
            int64_t _debug_loader_count = 0;
            int64_t _debug_items_start_position = -1;
            int64_t _debug_items_count = 0;
            for(int64_t _debug_index = debug_cardinality_count, end = debug_cardinality_count + subtask.m_cardinality; _debug_index < end; _debug_index++){
                int64_t indx = get<2>(_debug_items[_debug_index]);
//                COUT_DEBUG("[" << _debug_index << "] key: " << get<0>(_debug_items[_debug_index]) << ", value: " << get<1>(_debug_items[_debug_index]) << ", index: " << indx);
                if(indx < 0){
                    if(_debug_loader_start == -1) { _debug_loader_start = (-indx) -1; }
                    _debug_loader_count++;
                } else {
                    if(_debug_items_start_position == -1) _debug_items_start_position = indx;
                    _debug_items_count++;
                }
            }
            if(_debug_loader_start < 0) {
                _debug_loader_start = debug_loader_last; // reset to 0
                assert(_debug_loader_count == 0);
            }

            COUT_DEBUG("expected start position: " << _debug_items_start_position << ", count: " << _debug_items_count << ", blkload: [" << _debug_loader_start << ", " << _debug_loader_start + _debug_loader_count << ")");

            // assertions, for the input start position, it can be:
            // a) 0: when subtask_id == 0, that is, it is not explicitly set
            // b)_debug_items_start_position: ideally they should match
            // c) start == end: if _debug_items_start_position == -1, the input is empty and start == end
            assert(/* a */ (subtask_id == 0 && subtask.m_input_position_start == 0) ||
                   /* b */ subtask.m_input_position_start == _debug_items_start_position ||
                   /* c */ subtask.m_input_position_start == subtask.m_input_position_end);

            // bulk loading, they should straightforwardly match what we counted above

            assert(subtask.m_blkload_start == _debug_loader_start);
            assert(subtask.m_blkload_end == subtask.m_blkload_start + _debug_loader_count);

            debug_cardinality_count += subtask.m_cardinality;
            debug_loader_last = _debug_loader_start + _debug_loader_count;
#endif

            m_task->m_subtasks.push_back(subtask);
            IF_PROFILING( m_task->m_statistics.m_worker_num_subtaks++ );
        }

#if defined(DEBUG_CONTENT)
        assert(debug_cardinality_count == m_task->m_plan.get_cardinality_after() && "Cardinality mismatch");
#endif

        // reverse the order of the queue, workers fetch from the latest to the first
        std::reverse(begin(m_task->m_subtasks), end(m_task->m_subtasks));
    }
}

void RebalancingWorker::do_execute_queue(){
    int64_t next_worker_id = 1;
    const bool is_rebalance = m_task->m_plan.m_operation == RebalanceOperation::REBALANCE || m_task->m_plan.m_operation == RebalanceOperation::RESIZE_REBALANCE;

    int64_t subtask_protect_extent = -1; // -1 => not installed in the queue task->m_input_watermarks
    int64_t input_extent_watermark = std::numeric_limits<int64_t>::max();
    IF_PROFILING( vector<uint64_t> execution_times );

    while(true){
        RebalancingTask::SubTask subtask;

        { // fetch the next subtask to execute
            unique_lock<mutex> lock(m_task->m_workers_mutex);
            if(m_task->m_subtasks.empty()){
                break; // terminate the while loop
            } else {
                subtask = m_task->m_subtasks.back();
                m_task->m_subtasks.pop_back();
                COUT_DEBUG("Subtask fetched: " << subtask);

                if(is_rebalance){
                    if(subtask_protect_extent >= 0){ // remove the previous watermark
                        auto it = std::find_if(begin(m_task->m_input_watermarks), end(m_task->m_input_watermarks), [subtask_protect_extent](int64_t candidate){ return subtask_protect_extent == candidate; });
                        assert(it != end(m_task->m_input_watermarks) && "Not found");
                        m_task->m_input_watermarks.erase(it);
                    }

                    // install the next watermark
                    subtask_protect_extent = subtask.m_input_extent_start;
                    m_task->m_input_watermarks.push_back(subtask_protect_extent);
                    input_extent_watermark = *(std::min_element(begin(m_task->m_input_watermarks), end(m_task->m_input_watermarks)));
                    COUT_DEBUG("Subtask input watermark (extent): " << subtask_protect_extent);
                }
            }

            // try to obtain more workers to work on this task
            if(m_worker_id == 0 && m_task->m_subtasks.size() > 0){
                std::vector<RebalancingWorker*> workers = m_task->m_master->thread_pool().acquire(m_task->m_subtasks.size());
                for(size_t i = 0; i < workers.size(); i++){
                    workers[i]->execute0(m_task, next_worker_id++);
                    m_task->m_active_workers++;
                }
            }
        }

        reclaim_past_extents(input_extent_watermark); // previous iteration
        IF_PROFILING( auto t0 = chrono::steady_clock::now() );
        do_execute_subtask(subtask, input_extent_watermark);
        IF_PROFILING( execution_times.push_back( chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - t0).count() ));
    }

    // finish rewiring the last extents
    if(is_rebalance && subtask_protect_extent >= 0 /* it may well possible that this worker was not able to fetch any task */){
        { // restrict the scope
            unique_lock<mutex> lock(m_task->m_workers_mutex);

            // remove the last watermark
            auto it = std::find_if(begin(m_task->m_input_watermarks), end(m_task->m_input_watermarks), [subtask_protect_extent](int64_t candidate){ return subtask_protect_extent == candidate; });
            assert(it != end(m_task->m_input_watermarks) && "Not found");
            m_task->m_input_watermarks.erase(it);
            subtask_protect_extent = -1;

            assert(m_task->m_subtasks.empty() && "Otherwise this worker should still be in the while loop");
            while(!m_task->m_input_watermarks.empty()){ m_task->m_workers_condvar.wait(lock); }
        }

         reclaim_past_extents(std::numeric_limits<int64_t>::max());
    }
    assert(m_extents_to_rewire.empty());

    IF_PROFILING( { unique_lock<mutex> lock(m_task->m_workers_mutex); for(auto time : execution_times) m_task->m_statistics.m_worker_task_exec_time.push_back(time); })

    m_task->m_active_workers--;
    m_task->m_workers_condvar.notify_all();

    IF_PROFILING( if(m_worker_id == 0) { m_task->m_statistics.m_worker_num_threads = next_worker_id; } );
}

void RebalancingWorker::do_execute_subtask(RebalancingTask::SubTask& subtask, int64_t input_extent_watermark) {
    BulkLoadingIterator loader { m_task, (size_t) subtask.m_blkload_start, (size_t) subtask.m_blkload_end };

    switch(m_task->m_plan.m_operation){
    case RebalanceOperation::REBALANCE:
    case RebalanceOperation::RESIZE_REBALANCE: {
        rebalance_rewire(m_task->m_ptr_storage, input_extent_watermark,
                /* input positions */ subtask.m_input_position_start, subtask.m_input_position_end,
                /* bulk loader */ loader,
                /* output extent start */ subtask.m_output_extent_start,
                /* output extent length */ subtask.m_output_extent_end - subtask.m_output_extent_start,
                /* final cardinality (input + bulk loader) */ subtask.m_cardinality);
    } break;
    case RebalanceOperation::RESIZE: {
        resize(&(m_task->m_pma->m_storage), m_task->m_ptr_storage, subtask.m_input_position_start, subtask.m_input_position_end,
                /* bulk loader */ loader,
                /* output window start */ subtask.m_output_extent_start * m_task->m_ptr_storage->get_segments_per_extent(),
                /* output window length */ (subtask.m_output_extent_end - subtask.m_output_extent_start) * m_task->m_ptr_storage->get_segments_per_extent(),
                /* final cardinality */ subtask.m_cardinality);
    } break;
    default:
        assert(0 && "Invalid case");
    }
}

/*****************************************************************************
 *                                                                           *
 *  Spread with two copies                                                   *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::spread_local(){
    auto& plan = m_task->m_plan;
    COUT_DEBUG("window_start: " << plan.m_window_start << ", window_length: " << plan.m_window_length << ", cardinality: " << plan.get_cardinality_after());

    // first copy all elements aside
    auto& memory_pool = m_task->m_pma->memory_pool();
    auto fn_deallocate = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> ptr_workspace_keys { memory_pool.allocate<int64_t>(plan.get_cardinality_before()), fn_deallocate };
    unique_ptr<int64_t, decltype(fn_deallocate)> ptr_workspace_values { memory_pool.allocate<int64_t>(plan.get_cardinality_before()), fn_deallocate };
    int64_t* __restrict workspace_keys = ptr_workspace_keys.get();
    int64_t* __restrict workspace_values = ptr_workspace_values.get();
    uint16_t* __restrict input_cardinalities = m_task->m_ptr_storage->m_segment_sizes;

    // copy all input elements into the workspace
    assert(plan.m_window_start % 2 == 0 && "Expected to start from an even segment");
    assert(plan.m_window_length % 2 == 0 && "Expected to rebalance an even number (a power of 2) of segments");
    int64_t* __restrict keys = m_task->m_ptr_storage->m_keys;
    int64_t* __restrict values = m_task->m_ptr_storage->m_values;
    const int64_t segment_capacity = m_task->m_pma->m_storage.m_segment_capacity;
    int64_t workspace_index = 0;
    for(int64_t input_segment_id = plan.m_window_start, end = plan.m_window_start + plan.m_window_length; input_segment_id < end; input_segment_id+= 2){
        int64_t input_sz_lhs = input_cardinalities[input_segment_id];
        int64_t input_sz_rhs = input_cardinalities[input_segment_id +1];
        int64_t input_sz = input_sz_lhs + input_sz_rhs;
        int64_t input_displacement = (input_segment_id +1) * segment_capacity - input_sz_lhs;
        memcpy(workspace_keys + workspace_index, keys + input_displacement, input_sz * sizeof(keys[0]));
        memcpy(workspace_values + workspace_index, values + input_displacement, input_sz * sizeof(values[0]));
        workspace_index += input_sz;
    }
    const int64_t workspace_end = workspace_index;

    BulkLoadingIterator loader { m_task };
    std::pair<int64_t, int64_t> loader_elt = loader.get();
    assert((plan.get_cardinality_after() == loader.cardinality() + plan.get_cardinality_before()) && "Cardinality mismatch");

    const size_t num_elts_per_segment = plan.get_cardinality_after() / plan.m_window_length;
    const size_t num_odd_segments = plan.get_cardinality_after() % plan.m_window_length;
    size_t i = 0; // iteration index

    // copy all elements from the workspace to their final positions in the storage
    workspace_index = 0;
    for(int64_t output_segment_id = plan.m_window_start, end = plan.m_window_start + plan.m_window_length; output_segment_id < end; output_segment_id += 2){
        int64_t output_sz_lhs = num_elts_per_segment + (i < num_odd_segments);
        int64_t output_sz_rhs = num_elts_per_segment + ((i+1) < num_odd_segments);
        int64_t output_sz = output_sz_lhs + output_sz_rhs;
        int64_t output_displacement = (output_segment_id +1) * segment_capacity - output_sz_lhs;
        if(/*input_sz = */ (plan.get_cardinality_before() - workspace_index) >= output_sz && loader_elt.first >= workspace_keys[workspace_index + output_sz -1]){
            memcpy(keys + output_displacement, workspace_keys + workspace_index, output_sz * sizeof(keys[0]));
            memcpy(values + output_displacement, workspace_values + workspace_index, output_sz * sizeof(values[0]));
            workspace_index += output_sz;;
        } else { // merge
            for(int64_t k = 0; k < output_sz; k++){
                if(workspace_index < workspace_end && workspace_keys[workspace_index] <= loader_elt.first){
                    keys[output_displacement +k] = workspace_keys[workspace_index];
                    values[output_displacement +k] = workspace_values[workspace_index];
                    workspace_index++;
                } else {
                    keys[output_displacement +k] = loader_elt.first;
                    values[output_displacement +k] = loader_elt.second;
                    loader++;
                    loader_elt = loader.get();
                }
            }

        }

        set_separator_key(output_segment_id, keys[output_displacement]);
        set_separator_key(output_segment_id +1, keys[output_displacement + output_sz_lhs]);

        i+=2; // i = 0, 2, 4, ...
    }
}

/*****************************************************************************
 *                                                                           *
 *  Spread with rewiring                                                     *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::rebalance_rewire(Storage* storage, int64_t input_extent_watermark, int64_t input_position_start, int64_t input_position_end, BulkLoadingIterator& loader, int64_t output_extent_start, int64_t output_extent_length, size_t cardinality) {
    const int64_t segment_capacity = storage->m_segment_capacity;
    const int64_t segments_per_extent = storage->get_segments_per_extent();
    const int64_t cardinality_per_segment = cardinality / (output_extent_length * segments_per_extent);
    int64_t num_odd_segments = cardinality % (output_extent_length * segments_per_extent);

    for(int64_t i = 0; i < output_extent_length; i++){
        int64_t extent_id = output_extent_start + i;
        const bool use_rewiring = (extent_id >= input_extent_watermark);

        int64_t cardinality_leftover = min(num_odd_segments, segments_per_extent);
        num_odd_segments -= cardinality_leftover;
        const size_t extent_cardinality = (cardinality_per_segment * segments_per_extent) + cardinality_leftover;

        if(!use_rewiring){
            COUT_DEBUG("extent: " << extent_id << ", spread without rewiring, watermark=" << input_extent_watermark);
            int64_t offset = extent_id * segments_per_extent * segment_capacity;

            spread_rewire(storage,
                    /*in/out*/ input_position_start, /* in */ input_position_end,
                    /* bulk loading loader */ loader,
                    /* destination */ storage->m_keys + offset, storage->m_values + offset,
                    /* extent id */ extent_id,
                    /* cardinality */ extent_cardinality);
        } else {
            COUT_DEBUG("extent: " << extent_id << ", spread with rewiring, watermark=" << input_extent_watermark);

            // get some space from the rewiring facility
            int64_t *buffer_keys{nullptr}, *buffer_values{nullptr};
            { // mutual exclusion
                scoped_lock<SpinLock> lock(storage->m_mutex);
                buffer_keys = (int64_t*) storage->m_memory_keys->acquire_buffer();
                buffer_values = (int64_t*) storage->m_memory_values->acquire_buffer();
            }
            m_extents_to_rewire.push_back(Extent2Rewire{extent_id, buffer_keys, buffer_values});

            spread_rewire(storage,
                    /*in/out*/ input_position_start, /* in */ input_position_end,
                    /* bulk loading loader */ loader,
                    /* destination */ buffer_keys, buffer_values,
                    /* extent id */ extent_id,
                    /* cardinality */ extent_cardinality);
        }
    }
}

void RebalancingWorker::spread_rewire(Storage* storage, int64_t& input_position_start, const int64_t input_position_end, BulkLoadingIterator& loader, int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, size_t cardinality){
//    COUT_DEBUG("[destination] keys: " << destination_keys << ", values: " << destination_values << ", extent_id: " << extent_id << ", position: " << input_position_start);
    constexpr int64_t INT64_T_MAX = std::numeric_limits<int64_t>::max();
    decltype(storage->m_segment_sizes) __restrict segment_sizes = storage->m_segment_sizes;
    const size_t segments_per_extent = storage->get_segments_per_extent();
    const size_t segment_capacity = storage->m_segment_capacity;
    const size_t segment_base = extent_id * segments_per_extent;
    int64_t input_idx {0}, input_run_sz {0}, input_initial_displacement{0};
    int64_t input_segment_id = (input_position_start / (2* segment_capacity)) *2; // even segment
    if(/* current position */ input_segment_id * segment_capacity < input_position_end){
        input_idx = input_position_start - ((input_segment_id +1) * segment_capacity - segment_sizes[input_segment_id]);
        input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
        COUT_DEBUG("input_position_start: " << input_position_start << ", input_position_end: " << input_position_end << ", input_idx: " << input_idx << ", input_run_sz: " << input_run_sz);

        if(input_idx == input_run_sz){
            input_run_sz = 0;
            do {
                input_segment_id += 2;
                if(input_segment_id * segment_capacity >= input_position_end) break; // all input segments are empty...
                input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];

            } while(input_run_sz == 0);
            input_idx = 0;
        }
        if(input_run_sz > 0) input_initial_displacement = (input_segment_id +1) * segment_capacity - segment_sizes[input_segment_id];
    }
    int64_t* __restrict input_keys = storage->m_keys + input_initial_displacement;
    int64_t* __restrict input_values = storage->m_values + input_initial_displacement;
//    COUT_DEBUG("initial_displacement: " << input_initial_displacement << ", first key: " << input_keys[0]);

    COUT_DEBUG("extent: " << extent_id << ", initial segment: " << input_segment_id << ", run sz: " << input_run_sz << ", displacement: " << input_initial_displacement);
    const size_t cardinality_per_segment = cardinality / segments_per_extent;
    const size_t num_odd_segments = cardinality % segments_per_extent;

    // the loader should already be ready
    auto blkelt = loader.get();

    // left to right
    for(size_t output_segment_id = 0; output_segment_id < segments_per_extent; output_segment_id+=2){
        const size_t output_run_sz_lhs = cardinality_per_segment + (output_segment_id < num_odd_segments);
        const size_t output_run_sz_rhs = cardinality_per_segment + (output_segment_id+1 < num_odd_segments);
        const size_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= 2 * segment_capacity);
        const size_t output_displacement = output_segment_id * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* __restrict output_keys = destination_keys + output_displacement;
        int64_t* __restrict output_values = destination_values + output_displacement;

        int64_t k = 0; // output position

        COUT_DEBUG("output_segment_id: " << output_segment_id << ", output_run_sz: " << output_run_sz << ", input_segment_id: " << input_segment_id << ", input_index: " << input_idx << "/" << input_run_sz); // << " (left: " << segment_sizes[input_segment_id] << ", right: " << segment_sizes[input_segment_id +1] << ")");

        // merge the elts from the loader and the input
        while(k < output_run_sz && input_idx < input_run_sz && blkelt.first < INT64_T_MAX){
            if(input_keys[input_idx] < blkelt.first){ // merge from the input
                output_keys[k] = input_keys[input_idx];
                output_values[k] = input_values[input_idx];
                input_idx++;

                // fetch the next input sequence
                if(input_idx >= input_run_sz){
                    input_run_sz = 0;
                    do {
                        input_segment_id += 2; // move to the next even segment
                        if(/* current position */ input_segment_id * segment_capacity >= input_position_end) break; // okay, all input segments are empty
                        size_t input_displacement = (input_segment_id +1) * segment_capacity - segment_sizes[input_segment_id];
                        input_keys = storage->m_keys + input_displacement;
                        input_values = storage->m_values + input_displacement;
                        input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
                    } while(input_run_sz == 0);
                    input_idx = 0;
                }
            } else { // merge from the bulk loader
                output_keys[k] = blkelt.first;
                output_values[k] = blkelt.second;
                loader++;
                blkelt = loader.get();
            }

            k++;
        }

        // loader depleted, only merge from the input
        while(k < output_run_sz && input_idx < input_run_sz){
            int64_t output_slots = output_run_sz - k;
            int64_t input_slots = input_run_sz - input_idx;
            int64_t cpy1 = min(input_slots, output_slots);

            memcpy(output_keys + k, input_keys + input_idx, cpy1 * sizeof(output_keys[0]));
            memcpy(output_values + k, input_values + input_idx, cpy1 * sizeof(output_keys[0]));

            input_idx += cpy1;
            k += cpy1;

            // fetch the next input sequence
            if(input_idx >= input_run_sz){
                input_run_sz = 0;
                do {
                    input_segment_id += 2; // move to the next even segment
                    if(/* current position */ input_segment_id * segment_capacity >= input_position_end) break; // okay, all input segments are empty
                    size_t input_displacement = (input_segment_id +1) * segment_capacity - segment_sizes[input_segment_id];
                    input_keys = storage->m_keys + input_displacement;
                    input_values = storage->m_values + input_displacement;
                    input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
                } while(input_run_sz == 0);
                input_idx = 0;
            }
        }

        // input depleted, only merge from the loader
        while(k < output_run_sz && blkelt.first < INT64_T_MAX){
            output_keys[k] = blkelt.first;
            output_values[k] = blkelt.second;
            loader++;
            blkelt = loader.get();
            k++;
        }

        // done ?
        assert(k == output_run_sz && "Still elements to copy?");
        set_separator_key(segment_base + output_segment_id, output_keys[0]);
        set_separator_key(segment_base + output_segment_id + 1, output_keys[output_run_sz_lhs]);
    }

    // update the final position
    input_position_start = /* all inputs ahead are empty ? */ input_run_sz == 0 ?
            /* eof */ input_position_end :
            input_keys - storage->m_keys + input_idx;
    COUT_DEBUG("final_position: " << input_position_start << ", input segment: " << input_segment_id << ", index: " << input_idx << "/" << input_run_sz);
}


/*****************************************************************************
 *                                                                           *
 *   Resize                                                                  *
 *                                                                           *
 *****************************************************************************/
// left 2 right
void RebalancingWorker::resize(const Storage* input, Storage* output, int64_t input_position_start, int64_t input_position_end, BulkLoadingIterator& loader, int64_t output_window_start, int64_t output_window_length, size_t cardinality) {
    assert(input != nullptr && output != nullptr);
    assert(input->m_segment_capacity == output->m_segment_capacity && "Incompatible storages");
    assert(output_window_start % 2 == 0 && "Expected an even entry point");
    assert((output_window_length % 2 == 0 || (output_window_start == 0 && output_window_length == 1)) && "Expected an even number of segments");
    constexpr int64_t INT64_T_MAX = std::numeric_limits<int64_t>::max(); // otherwise it confuses Eclipse :/
    COUT_DEBUG("input_position_start: " << input_position_start);

    // input
    const int64_t segment_capacity = input->m_segment_capacity;
    uint16_t* __restrict input_sizes = input->m_segment_sizes;
    int64_t input_initial_displacement {0}, input_run_sz {0};
    int64_t input_segment_id = (input_position_start / (2* segment_capacity)) *2; // even segment
    if(/* current position */ input_segment_id * segment_capacity < input_position_end){
        input_initial_displacement = input_position_start;
        input_run_sz = (input_segment_id +1) * segment_capacity + input_sizes[input_segment_id +1] - input_position_start;
        while(input_run_sz == 0){ // fetch the first non empty chunk
            input_segment_id += 2; // move to the next even segment
            if(/* current position */ input_segment_id * segment_capacity >= input_position_end) break; // okay, all input segments are empty
            input_initial_displacement = (input_segment_id +1) * segment_capacity - input_sizes[input_segment_id];
            input_run_sz = input_sizes[input_segment_id] + input_sizes[input_segment_id +1];
        }
    }
    int64_t input_idx = 0;
    int64_t* __restrict input_keys = input->m_keys + input_initial_displacement;
    int64_t* __restrict input_values = input->m_values + input_initial_displacement;

    // the loader should already be ready
    auto blkelt = loader.get();

    // output
    int64_t* __restrict output_base_keys = output->m_keys + output_window_start * output->m_segment_capacity;
    int64_t* __restrict output_base_values = output->m_values + output_window_start * output->m_segment_capacity;

    // cardinality per segment
    const int64_t cardinality_per_segment = cardinality / output_window_length;
    const int64_t num_odd_segments = cardinality % output_window_length;

    for(int64_t segment_id = 0; segment_id < output_window_length; segment_id+=2){
        const int64_t output_run_sz_lhs = cardinality_per_segment + (segment_id < num_odd_segments);
        const int64_t output_run_sz_rhs = output_window_length == 1 ? 0 : cardinality_per_segment + ((segment_id+1) < num_odd_segments);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= 2 * segment_capacity);
        size_t output_displacement = segment_id * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* __restrict output_keys = output_base_keys + output_displacement;
        int64_t* __restrict output_values = output_base_values + output_displacement;
        int64_t k = 0; // output position

        // merge the elts from the loader and the input
        while(k < output_run_sz && input_idx < input_run_sz && blkelt.first < INT64_T_MAX){
            if(input_keys[input_idx] < blkelt.first){ // merge from the input
                output_keys[k] = input_keys[input_idx];
                output_values[k] = input_values[input_idx];
                input_idx++;

                // fetch the next input sequence
                if(input_idx >= input_run_sz){
                    input_run_sz = 0; // reset
                    do {
                        input_segment_id += 2; // move to the next even segment
                        if(/* current position */ input_segment_id * segment_capacity >= input_position_end) break; // okay, all input segments are empty
                        size_t input_displacement = (input_segment_id +1) * segment_capacity - input_sizes[input_segment_id];
                        input_keys = input->m_keys + input_displacement;
                        input_values = input->m_values + input_displacement;
                        input_run_sz = input_sizes[input_segment_id] + input_sizes[input_segment_id +1];
                    } while(input_run_sz == 0);
                    input_idx = 0;
                }
            } else { // merge from the bulk loader
                output_keys[k] = blkelt.first;
                output_values[k] = blkelt.second;
                loader++;
                blkelt = loader.get();
            }

            k++;
        }

        // loader depleted, only merge from the input
        while(k < output_run_sz && input_idx < input_run_sz){
            int64_t output_slots = output_run_sz - k;
            int64_t input_slots = input_run_sz - input_idx;
            int64_t cpy1 = min(input_slots, output_slots);
            memcpy(output_keys + k, input_keys + input_idx, cpy1 * sizeof(output_keys[0]));
            memcpy(output_values + k, input_values + input_idx, cpy1 * sizeof(output_keys[0]));

            input_idx += cpy1;
            k += cpy1;

            // fetch the next input sequence
            if(input_idx >= input_run_sz){
                input_run_sz = 0; // reset
                do {
                    input_segment_id += 1 + (input_segment_id % 2 == 0); // move to the next even segment
                    if(/* current position */ input_segment_id * segment_capacity >= input_position_end) break; // okay, all input segments are empty
                    assert(input_segment_id < input->m_number_segments);
                    size_t input_displacement = (input_segment_id +1) * segment_capacity - input_sizes[input_segment_id];
                    input_keys = input->m_keys + input_displacement;
                    input_values = input->m_values + input_displacement;
                    input_run_sz = input_sizes[input_segment_id] + input_sizes[input_segment_id +1];
                } while(input_run_sz == 0);
                input_idx = 0;
            }
        }

        // input depleted, only merge from the loader
        while(k < output_run_sz && blkelt.first < INT64_T_MAX){
            output_keys[k] = blkelt.first;
            output_values[k] = blkelt.second;
            loader++;
            blkelt = loader.get();
            k++;
        }

        // done ?
        assert(k == output_run_sz && "Still elements to copy?");
        set_separator_key(output_window_start + segment_id, output_keys[0]);
        if(output_window_length > 1) // it covers the case of a downsize with only 1 segment at the end
            set_separator_key(output_window_start + segment_id + 1, output_keys[output_run_sz_lhs]);
    }

    COUT_DEBUG("final position: " << (input_keys - input->m_keys + input_idx));
}

/*****************************************************************************
 *                                                                           *
 *   Index                                                                   *
 *                                                                           *
 *****************************************************************************/

void RebalancingWorker::set_separator_key(uint64_t segment_id, int64_t key){
    auto segments_per_lock = m_task->m_pma->get_segments_per_lock();
    int64_t lock_id = segment_id / segments_per_lock;
    int64_t lock_offset = segment_id % segments_per_lock;

    // Assume the lock to the gate has already been acquired
    Gate* gate = m_task->m_ptr_locks + lock_id;

    // Propagate to the index ?
    if(lock_offset == 0){
        if(lock_id > 0){
            m_task->m_ptr_index->set_separator_key(lock_id, key);
            if(gate->m_fence_low_key != key){
                gate->m_fence_low_key = key;
                // fence key for the previous gate
                m_task->m_ptr_locks[lock_id -1].m_fence_high_key = key -1;
            }
        } else { // a bit of a hack...
            m_task->m_ptr_index->set_separator_key(0, numeric_limits<int64_t>::min());
        }
    } else {
        gate->set_separator_key(segment_id, key);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Bulk loading                                                            *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::sort_blkload_elts(){
    IF_PROFILING( RebalancingTimer timer { m_task->m_statistics.m_worker_sort_time } );

    // sort the list of vectors
    std::sort(begin(m_task->m_blkld_elts), end(m_task->m_blkld_elts), [](auto v1, auto v2){
       assert(! v1->empty() && ! v2->empty() );
       return v1->insertions()[0] < v2->insertions()[0]; // just compare the first elt
    });

    // sort the single vectors
    for(size_t i = 0; i < m_task->m_blkld_elts.size(); i++){
        auto& vect = m_task->m_blkld_elts[i]->insertions();
        std::sort(std::begin(vect), std::end(vect));
    }
}

void RebalancingWorker::clear_blkload_queues(){
    IF_PROFILING( RebalancingTimer timer { m_task->m_statistics.m_worker_clear_blkload_queues } );
    for(size_t i = 0, sz = m_task->m_blkld_elts.size(); i < sz; i++){
        delete m_task->m_blkld_elts[i]; m_task->m_blkld_elts[i] = nullptr;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Rewiring                                                                *
 *                                                                           *
 *****************************************************************************/

void RebalancingWorker::reclaim_past_extents(int64_t input_extent_watermark){
    if(m_extents_to_rewire.empty() || m_extents_to_rewire.front().m_extent_id >= input_extent_watermark) return;

    Storage* storage = m_task->m_ptr_storage;
    int64_t segments_per_extent = storage->get_segments_per_extent();
    int64_t segment_capacity = storage->m_segment_capacity;

    scoped_lock<SpinLock> lock(storage->m_mutex);
    do {
        auto& metadata = m_extents_to_rewire.front();
        auto extent_id = metadata.m_extent_id;
        auto offset_dst = extent_id * segments_per_extent * segment_capacity;
        auto keys_dst = storage->m_keys + offset_dst;
        auto keys_src = metadata.m_buffer_keys;
        auto values_dst = storage->m_values + offset_dst;
        auto values_src = metadata.m_buffer_values;
        m_extents_to_rewire.pop_front();
//        COUT_DEBUG("reclaim buffers for extent: " << metadata.m_extent_id << ", keys: " << keys_src << ", values: " << values_src);
        storage->m_memory_keys->swap_and_release(keys_dst, keys_src);
        storage->m_memory_values->swap_and_release(values_dst, values_src);
    } while (!m_extents_to_rewire.empty() && m_extents_to_rewire.front().m_extent_id < input_extent_watermark);
}

/*****************************************************************************
 *                                                                           *
 *   Segment cardinalities                                                   *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::update_segment_cardinalities() {
    IF_PROFILING( RebalancingTimer timer { m_task->m_statistics.m_worker_segment_cards } );
    uint16_t* __restrict cardinalities_shifted = m_task->m_ptr_storage->m_segment_sizes + m_task->get_window_start();
    Gate* __restrict locks = m_task->m_ptr_locks;

    int64_t cardinality_per_segment = m_task->m_plan.get_cardinality_after() / m_task->get_window_length();
    int64_t num_odd_segments = m_task->m_plan.get_cardinality_after() % m_task->get_window_length();
    auto lock_start = m_task->get_lock_start();
    auto num_segments_per_lock = std::min<int64_t>(m_task->m_ptr_storage->m_number_segments, m_task->m_pma->get_segments_per_lock());

    int64_t segment_id = 0;
    for(int64_t i = 0, num_locks = m_task->get_lock_length(); i < num_locks; i++){
        int64_t lock_id = lock_start + i;
        int64_t lock_cardinality = 0;

        for(int64_t j = 0; j < num_segments_per_lock; j++){
            int64_t tpma_card = cardinality_per_segment + (segment_id < num_odd_segments);
            lock_cardinality += tpma_card;
            cardinalities_shifted[segment_id] = (uint16_t) tpma_card;
            segment_id++;
        }

        locks[lock_id].m_cardinality = lock_cardinality;
    }

    // assume all elements from the bulk loader have been inserted..
    assert(m_task->m_plan.m_cardinality_change >= 0 && "We shouldn't handle deletes here");
    m_task->m_pma->m_cardinality += m_task->m_plan.m_cardinality_change;
}


/*****************************************************************************
 *                                                                           *
 *   InputIterator                                                           *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[InputIterator::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }

RebalancingWorker::InputIterator::InputIterator(const Storage& storage, int64_t window_start, int64_t window_end) : m_storage(storage){
    int64_t window_last = window_end -1;

    while(window_last >= window_start && storage.m_segment_sizes[window_last] == 0){ window_last--; } // ignore empty segments
    if( storage.m_segment_sizes[window_last] == 0 ){
        m_start = m_end = m_position = 0;
    } else {
        if(window_last % 2 == 0){
            m_end = storage.m_segment_capacity * (window_last +1);
        } else {
            m_end = storage.m_segment_capacity * window_last + storage.m_segment_sizes[window_last];
        }
    }

    while(window_start <= window_last && storage.m_segment_sizes[window_start] == 0){ window_start++; } // ignore empty segments

    assert(storage.m_segment_sizes[window_start] > 0 && "This case should have already been covered with window_end");
    if(window_start % 2 == 0){
        m_start = storage.m_segment_capacity * (window_start +1) - storage.m_segment_sizes[window_start];
    } else {
        m_start = storage.m_segment_capacity * window_start;
    }

    COUT_DEBUG("start: " << m_start << ", end: " << m_end);
    m_position = m_start;
    m_feature = MarkerPosition::START;
}

int64_t RebalancingWorker::InputIterator::get_key() const {
    constexpr auto MAX = numeric_limits<int64_t>::max();
    constexpr auto MIN = numeric_limits<int64_t>::min();

    if(m_overflow > 0 || m_position >= m_end)
        return MAX;
    else if (m_overflow < 0)
        return MIN;

    // aliases
    const int64_t segment_capacity = m_storage.m_segment_capacity;
    const uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;

    if(m_feature == MarkerPosition::END){
        int64_t segment_id = ((m_position -1) / (2* segment_capacity)) *2; // even segment
        segment_id += 2;

        // because m_position < m_end, we are guaranteed to reach a non empty segment inside the given window
        while(cardinalities[segment_id] + cardinalities[segment_id +1] == 0){ segment_id += 2; } // first non empty chunk

        int64_t segment_start = (segment_id +1) * segment_capacity - cardinalities[segment_id];
        return m_storage.m_keys[segment_start];
    } else {
        return m_storage.m_keys[m_position];
    }
}

void RebalancingWorker::InputIterator::move(int64_t shift){
    if(shift >= 0){
        move_forwards_by(shift);
    } else { // shift < 0
        move_backwards_by(-shift);
    }
}

void RebalancingWorker::InputIterator::move_forwards_by(int64_t shift){
    assert(shift >= 0);

    // handle underflow and overflow
    if(m_overflow < 0){
        int64_t credit = std::min(-m_overflow, shift);
        m_overflow += credit;
        shift -= credit;
        if(m_overflow < 0) return; // still in overflow
        m_position = m_start;
    } else if(m_overflow > 0) {
        m_overflow += shift;
        return;
    }
    assert(m_overflow == 0 && "It should have been handled by the cases above");
    if(shift == 0) return; // nothing to do here

    // aliases
    const int64_t segment_capacity = m_storage.m_segment_capacity;
    const uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;

    fetch_next_chunk();

    if(m_position >= m_end) {
        assert(m_feature == MarkerPosition::END);
        assert(m_position == m_end);
        m_overflow = shift;
        return; // depleted
    }

    int64_t segment_id = (m_position / (2* segment_capacity)) *2; // even segment
    const int64_t segment_start = (segment_id +1) * segment_capacity - cardinalities[segment_id];
    int64_t leftover = cardinalities[segment_id] + cardinalities[segment_id +1] - (m_position - segment_start);
    while(shift > 0){
        if(leftover - shift >= 0){
            m_position += shift;
            m_feature = (leftover == shift) ? MarkerPosition::END : MarkerPosition::MIDDLE;
            shift = 0; // done
        } else {
            shift -= leftover;
            m_position += leftover;

            if(m_position >= m_end){ // reached the end of the cursor
                m_position = m_end;
                m_overflow = shift; // depleted
                m_feature = MarkerPosition::END;
                shift = 0; // done
            } else {
                segment_id += 2;
                m_position = (segment_id +1) * segment_capacity - cardinalities[segment_id];
                leftover = cardinalities[segment_id] + cardinalities[segment_id +1];
            }
        }
    }

    assert(m_start <= m_position);
    assert(m_position <= m_end);
}

void RebalancingWorker::InputIterator::move_backwards_by(int64_t shift){
    assert(shift > 0);

    // handle underflow and overflow
    if(m_overflow < 0){
        m_overflow -= shift;
        return;
    } else if(m_overflow > 0) {
        int64_t credit = min(shift, m_overflow);
        m_overflow -= credit;
        shift -= credit;
        if(m_overflow > 0) return; // still in overflow
        m_position = m_end;
    }
    assert(m_overflow == 0 && "It should have been handled by the cases above");
    if(shift == 0) return;

    if(m_position == 0){
        m_overflow = -shift;
        m_feature = MarkerPosition::START;
        return;
    }

    // aliases
    const int64_t segment_capacity = m_storage.m_segment_capacity;
    const uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;

    int64_t segment_id { 0 };
    if(m_feature == MarkerPosition::END){
        segment_id = ((m_position -1) / (2* segment_capacity)) *2; // even segment
        while(segment_id >= 0 && cardinalities[segment_id] == 0 && cardinalities[segment_id +1] == 0){
            segment_id -= 2;
        }
        if(segment_id < 0) {
            m_position = m_start;
            m_overflow = -shift;
            m_feature = MarkerPosition::START;
            return;
        }

        m_position = (segment_id +1) * segment_capacity + cardinalities[segment_id +1] -1;
        m_feature = cardinalities[segment_id +1] + cardinalities[segment_id] == 0 ? MarkerPosition::START : MarkerPosition::MIDDLE;
        shift-=1;
    } else {
        segment_id = (m_position / (2* segment_capacity)) *2; // even segment
    }
    assert(segment_id < m_storage.m_number_segments);

    int64_t segment_start = (segment_id +1) * segment_capacity - cardinalities[segment_id];
    int64_t leftover = m_position - segment_start;

    while(shift > 0){
        if(leftover >= shift){
            m_position -= shift;
            bool at_start = (m_position == segment_start);
            m_feature = at_start ? MarkerPosition::START : MarkerPosition::MIDDLE;
            shift = 0; // done
        } else {
            shift -= leftover;
            m_position -= leftover;

            if(m_position <= m_start){ // reached the end of the cursor
                m_position = m_start;
                m_overflow = -shift; // depleted
                m_feature = MarkerPosition::START;
                shift = 0; // done
            } else {
                segment_id -= 2;
                segment_start = (segment_id +1) * segment_capacity - cardinalities[segment_id];
                leftover = cardinalities[segment_id] + cardinalities[segment_id +1];
                m_position = segment_start + leftover;
            }
        }
    }

    assert(m_start <= m_position);
    assert(m_position <= m_end);
}

void RebalancingWorker::InputIterator::fetch_next_chunk(){
    if(m_feature != MarkerPosition::END || m_position >= m_end) return;
    const int64_t segment_capacity = m_storage.m_segment_capacity;
    int64_t segment_id = ((m_position -1) / (2* segment_capacity)) *2; // even segment
    segment_id += 2;
    m_position = (segment_id +1) * segment_capacity - m_storage.m_segment_sizes[segment_id];
    m_feature = MarkerPosition::START;
}

int64_t RebalancingWorker::InputIterator::get_absolute_position_start() const{ return m_start; }
int64_t RebalancingWorker::InputIterator::get_absolute_position_current() const { return m_position; }
int64_t RebalancingWorker::InputIterator::get_absolute_position_end() const { return m_end; }

void RebalancingWorker::InputIterator::dump(std::ostream& out) const{
    out << "[position: " << get_absolute_position_current() << ", overflow: " << m_overflow << ", state: ";
    switch(m_feature){
    case MarkerPosition::START: out << "START"; break;
    case MarkerPosition::MIDDLE: out << "MIDDLE"; break;
    case MarkerPosition::END: out << "END"; break;
    }
    out << "]";
}

std::ostream& operator<<(std::ostream& out, const RebalancingWorker::InputIterator& subtask) {
    subtask.dump(out);
    return out;
}

#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingWorker::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }

/*****************************************************************************
 *                                                                           *
 *   BulkLoadingIterator                                                     *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[BulkLoadingIterator::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }

RebalancingWorker::BulkLoadingIterator::BulkLoadingIterator(const RebalancingTask* task) :
    BulkLoadingIterator(task, 0, task->m_plan.get_cardinality_after() - task->m_plan.get_cardinality_before()){ }
RebalancingWorker::BulkLoadingIterator::BulkLoadingIterator(const RebalancingTask* task, size_t start, size_t end) : m_vectors(task->m_blkld_elts){
    assert(start <= end && "Invalid range");

    // start & end positions
    abs2vect(m_vectors, start, &m_vect_start, &m_pos_start);
    abs2vect(m_vectors, end, &m_vect_last, &m_pos_end);

    // current position
    m_vect_pos = m_vect_start;
    m_position = m_pos_start;

    assert((empty() || (0 <= m_vect_start && m_vect_last < m_vectors.size())) && "Invalid range");

    m_cardinality = 0;
    if(!empty()){
        for(size_t i = m_vect_start; i <= m_vect_last; i++){
            m_cardinality += m_vectors[i]->insertions().size();
        }
        // remove the leading and trailing elements
        m_cardinality -= m_pos_start;
        m_cardinality -= (m_vectors[m_vect_last]->insertions().size() - m_pos_end);
    }
}
void RebalancingWorker::BulkLoadingIterator::abs2vect(const element_list_t& vectors, int64_t absolute_position, int64_t* out_vect_id, int64_t* out_vect_offset) {
    assert(out_vect_id != nullptr && out_vect_offset != nullptr);
    int64_t vect_id = 0;
    int64_t vect_offset = 0;

    while(absolute_position > 0 && vect_id < static_cast<int64_t>(vectors.size())){
        if(absolute_position >= vectors[vect_id]->insertions().size()){
            absolute_position -= vectors[vect_id]->insertions().size();
            vect_id++;
        } else { // absolute_position <= | vector size |
            vect_offset = absolute_position;
            absolute_position = 0; // terminate the loop
        }
    }

    if(vect_id >= static_cast<int64_t>(vectors.size())){ // out of range
        *out_vect_id = (vectors.size() > 0) ? vectors.size() -1 : 0;
        *out_vect_offset = (vectors.size() > 0) ? vectors[vectors.size() -1]->insertions().size() : 0;
    } else {
        *out_vect_id = vect_id;
        *out_vect_offset = vect_offset;
    }
}
int64_t RebalancingWorker::BulkLoadingIterator::vect2abs(const element_list_t& vectors, int64_t final_vect_id, int64_t final_vect_offset) {
    if(final_vect_id < 0 || vectors.size() == 0) return 0;
    if(final_vect_id > vectors.size()) {
        final_vect_id = vectors.size();
        final_vect_offset = 0;
    }

    int64_t absolute_position = 0;
    int64_t vect_id = 0;
    while(vect_id < final_vect_id){
        absolute_position += vectors[vect_id]->insertions().size();
        vect_id++;
    }

    absolute_position += final_vect_offset;

    return absolute_position;
}
std::pair<int64_t, int64_t> RebalancingWorker::BulkLoadingIterator::get() const {
    if(is_ahead()) return make_pair(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
    if(is_behind()) return make_pair(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());

    assert(m_vect_pos > m_vect_start || (m_vect_pos == m_vect_start && m_position >= m_pos_start));
    assert(m_vect_pos < m_vect_last || (m_vect_pos == m_vect_last && m_position < m_pos_end));

    // current elt to fetch
    return m_vectors[m_vect_pos]->insertions().operator[](m_position);
}
size_t RebalancingWorker::BulkLoadingIterator::cardinality() const { return m_cardinality; }
bool RebalancingWorker::BulkLoadingIterator::empty() const { return m_vect_start == m_vect_last && m_pos_start == m_pos_end; }
bool RebalancingWorker::BulkLoadingIterator::is_ahead() const { return empty() || m_vect_pos > m_vect_last || (m_vect_pos == m_vect_last && m_position >= m_pos_end); }
bool RebalancingWorker::BulkLoadingIterator::is_behind() const { return m_vect_pos < m_vect_start || (m_vect_pos == m_vect_start && m_position < m_pos_start); }
int64_t RebalancingWorker::BulkLoadingIterator::get_absolute_position_start() const{ return vect2abs(m_vectors, m_vect_start, m_pos_start); }
int64_t RebalancingWorker::BulkLoadingIterator::get_absolute_position_current() const { return vect2abs(m_vectors, m_vect_pos, m_position); }
int64_t RebalancingWorker::BulkLoadingIterator::get_absolute_position_end() const { return vect2abs(m_vectors, m_vect_last, m_pos_end); }
void RebalancingWorker::BulkLoadingIterator::operator++(int){
    if(is_ahead()) {
        return; /* nop */
    } else if(is_behind()){
        m_vect_pos = m_vect_start;
        m_position = m_pos_start;
    } else {
        m_position++;

        if(m_position >= m_vectors[m_vect_pos]->insertions().size()){
            m_vect_pos++;
            m_position = 0;
        }
    }
}
void RebalancingWorker::BulkLoadingIterator::operator--(int){
    if(is_behind() || empty()){ return; /* nop */ };

    if (is_ahead()){
        m_vect_pos = m_vect_last;
        m_position = m_pos_end;
    }

    m_position--;
    if(m_position < 0){
        m_vect_pos--;
        if(m_vect_pos >= m_vect_start){
            assert(m_vectors[m_vect_pos]->insertions().size() > 0 && "Empty vector");
            m_position = m_vectors[m_vect_pos]->insertions().size() -1;
        }
    }
}

#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingWorker::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }

/*****************************************************************************
 *                                                                           *
 *   Debug only                                                              *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::debug_content_before() {
#if defined(DEBUG_CONTENT)
    assert(m_task != nullptr && "Null task");
    assert(_debug_items.empty() && "The vector should have been freed by the previous iteration && no multiple workers should operate on the same thread_local variable");

    // input storage
    Storage& storage = m_task->m_pma->m_storage;
    const int64_t window_start = m_task->get_window_start();
    assert(window_start % 2 == 0 && "Expected an even segment");
    const int64_t window_end = m_task->m_plan.m_operation == RebalanceOperation::REBALANCE ? m_task->get_window_end() : storage.m_number_segments; // excl
    assert(window_end % 2 == 0 && "Expected an even segment");
    int64_t* __restrict keys = storage.m_keys;
    int64_t* __restrict values = storage.m_values;

    // Bulk Loader
    BulkLoadingIterator loader { m_task };
    auto blkelt = loader.get();
    int64_t blkload_pos = 0; // negative values

    // store the elements one by one in the vector _debug_items
    for(int64_t segment_id = window_start; segment_id < window_end; segment_id += 2){
        const int64_t position_start = (segment_id +1) * storage.m_segment_capacity - storage.m_segment_sizes[segment_id]; // incl.
        const int64_t position_end = position_start + storage.m_segment_sizes[segment_id] + storage.m_segment_sizes[segment_id +1]; // excl.

//        COUT_DEBUG("segment id: " << segment_id << ", cardinalities: " << storage.m_segment_sizes[segment_id] << " + " << storage.m_segment_sizes[segment_id +1] << ", position start: " << position_start << ", position_end: " << position_end << ", first key: " << keys[position_start] << ", last_key: " << keys[position_end -1]);
//        COUT_DEBUG("cardinality [" << segment_id << "]: " << storage.m_segment_sizes[segment_id] );
//        COUT_DEBUG("cardinality [" << segment_id +1 << "]: " << storage.m_segment_sizes[segment_id +1] );

        ino64_t i = position_start;
        while(i < position_end){
            if(keys[i] < blkelt.first){
                _debug_items.emplace_back(keys[i], values[i], i);
                i++;
            } else {
                _debug_items.emplace_back(blkelt.first, blkelt.second, --blkload_pos); // -1, -2, -3, ...
                loader++;
                blkelt = loader.get();
            }
        }
    }

    while(blkelt.first < std::numeric_limits<int64_t>::max()){
        _debug_items.emplace_back(blkelt.first, blkelt.second, --blkload_pos);
        loader++;
        blkelt = loader.get();
    }
#endif
}

void RebalancingWorker::debug_content_after(){
#if defined(DEBUG_CONTENT)
    if(m_task->m_plan.get_cardinality_after() == 0 ) return; // the PMA is empty

    assert(m_task != nullptr && "Null task");
    assert(!_debug_items.empty() && "Empty vector, have you called debug_content_before() at the start of the rebalancing procedure?");

    // Storage
    Storage& storage = *(m_task->m_ptr_storage);
    const int64_t window_start = m_task->get_window_start();
    assert(window_start % 2 == 0 && "Expected an even segment");
    const int64_t window_end = m_task->get_window_end();
    assert(window_end % 2 == 0 && "Expected an even segment");
    int64_t* __restrict keys = storage.m_keys;
    int64_t* __restrict values = storage.m_values;

//    // Report the segment cardinalities
//    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id++){
//        COUT_DEBUG_FORCE("cardinality[" << segment_id << "]: " << storage.m_segment_sizes[segment_id]);
//    }

    // Read one by one the elements in the vector _debug_items
    uint64_t j = 0;
    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id += 2){
        const int64_t position_start = (segment_id +1) * storage.m_segment_capacity - storage.m_segment_sizes[segment_id]; // incl.
        const int64_t position_end = position_start + storage.m_segment_sizes[segment_id] + storage.m_segment_sizes[segment_id +1]; // excl.

        for(int64_t i = position_start; i < position_end; i++){
            assert(j < _debug_items.size() && "Too many items the output storage");
            auto key_before = std::get<0>(_debug_items[j]);
            auto value_before = std::get<1>(_debug_items[j]);
            auto key_after = keys[i];
            auto value_after = values[i];

            if(key_before != key_after || value_before != value_after){
                COUT_DEBUG_FORCE("Item mismatch at position " << j << ": before: <" << key_before << ", " << value_before << ">, after: <" << key_after << ", " << value_after << "> (see below)");

                uint64_t k = 0;
                const int64_t segments_per_extent = storage.get_segments_per_extent();
                for(uint64_t segment_id = window_start; segment_id < window_end; segment_id += 2){
                    const int64_t position_start = (segment_id +1) * storage.m_segment_capacity - storage.m_segment_sizes[segment_id]; // incl.
                    const int64_t position_end = position_start + storage.m_segment_sizes[segment_id] + storage.m_segment_sizes[segment_id +1]; // excl.
                    for(int64_t i = position_start; i < position_end; i++){
                            COUT_DEBUG_FORCE("[" << k << "] input: <" << get<0>(_debug_items[k]) << ", " << get<1>(_debug_items[k]) << ">, "
                                    "input position: " << get<2>(_debug_items[k]) << ", " <<
                                    "output: <" << keys[i] << ", " << values[i] << ">, output position: " << i << ", "
                                    "output segment: " << i / storage.m_segment_capacity << ", output extent: " << (i / storage.m_segment_capacity / segments_per_extent) );
                        k++;
                    }
                }

                COUT_DEBUG_FORCE("Item mismatch at position " << j << ": before: <" << key_before << ", " << value_before << ">, after: <" << key_after << ", " << value_after << "> (see above)");
            }
            assert(key_before == key_after && "Key mismatch");
            assert(value_before == value_after && "Value mismatch");
            j++;
        }
    }

    assert(j == _debug_items.size() && "We didn't visit some of the items from the input storage");

    // Done
    _debug_items.clear();
#endif
}

} // namespace
