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
#include <mutex>
#include <thread>
#include <vector> // check all

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "rma/common/buffered_rewired_memory.hpp"
#include "rma/common/detector.hpp"
#include "rma/common/rewired_memory.hpp"
#include "gate.hpp"
#include "packed_memory_array.hpp"
#include "rebalancing_master.hpp"
#include "rebalancing_pool.hpp"
#include "rebalancing_task.hpp"
#include "storage.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::one_by_one {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingWorker::" << __FUNCTION__ << "] [" << this_thread::get_id() << ", worker_id: " << m_worker_id << "] " << msg << std::endl; }
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
thread_local std::vector<std::pair<int64_t, int64_t>> _debug_items;
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
        debug_content_before();

        // Run the APMA algorithm
        m_task->m_pma->rebalance_run_apma(m_task->m_plan, /* fill segments ? */ false, /* storage previous size */ m_task->m_num_locks * m_task->m_pma->get_segments_per_lock());

        // Update the detector
        if(m_task->m_plan.m_operation == RebalanceOperation::RESIZE_REBALANCE || m_task->m_plan.m_operation == RebalanceOperation::RESIZE){
            m_task->m_pma->m_detector.resize(m_task->get_window_length());
        }

        if(m_task->m_plan.m_window_length < m_task->m_ptr_storage->get_segments_per_extent()){
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
    switch(m_task->m_plan.m_operation){
    case RebalanceOperation::REBALANCE: {
        spread_local();
    } break;
    case RebalanceOperation::RESIZE_REBALANCE: {
        assert(0 && "Wrong branch: this kind of operation should have been performed through rewiring & the subtasks mechanism");
    } break;
    case RebalanceOperation::RESIZE: {
        RebalancingWorker::PartitionIterator apma_partitions{ m_task->m_plan.m_apma_partitions, 0, 0 };
        auto* input_storage = &(m_task->m_pma->m_storage);
        auto* output_storage = m_task->m_ptr_storage;

        resize(/* input storage: */ input_storage, /* output storage: */ output_storage,
               /* input start position: */ input_storage->m_segment_capacity - input_storage->m_segment_sizes[0],
               /* output segment id: */ 0, /* output number of segments: */ m_task->m_plan.m_window_length,
               /* segment cardinalities: */ apma_partitions
        );
    } break;
    default:
        assert(0 && "Invalid case");
    }
}

/*****************************************************************************
 *                                                                           *
 *   Execute from the queue                                                  *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::make_subtasks(){
    constexpr size_t CARDINALITY_THRESHOLD = 100000;
    Storage& storage_input = m_task->m_pma->m_storage;
    const size_t segments_per_extent = storage_input.get_segments_per_extent();
    const size_t segment_capacity = storage_input.m_segment_capacity;

    // 1. validation step, only executed if !defined(DEBUG)
#if !defined(NDEBUG) || defined(DEBUG)
    size_t lastpos = debug_make_subtasks_validate_apma_partitions();
#endif
    COUT_DEBUG("last position allowed: " << lastpos << " (excl)");


    InputPositionIterator position { m_task->m_pma->m_storage, (m_task->get_window_start() +1) * storage_input.m_segment_capacity - storage_input.m_segment_sizes[m_task->get_window_start()] };
    PartitionIterator apma_partitions { m_task->m_plan.m_apma_partitions };
    int64_t output_segment_id = m_task->get_window_start();

    while(!apma_partitions.end()){
        RebalancingTask::SubTask subtask;
        position.roundup();
        subtask.m_position_start = position;
        subtask.m_input_extent_start = subtask.m_position_start / segment_capacity / segments_per_extent;
        subtask.m_partition_start_id = apma_partitions.partition_id();
        subtask.m_partition_start_offset = apma_partitions.partition_offset();
        subtask.m_output_extent_start = output_segment_id /segments_per_extent;

        // create a subtask containing at least `CARDINALITY_THRESHOLD' elements in input
        int64_t subtask_cardinality = 0;
        while(subtask_cardinality < CARDINALITY_THRESHOLD && !apma_partitions.end()){
            int64_t cardinality = apma_partitions.cardinality_current();
            subtask_cardinality += cardinality;
            apma_partitions.move(1); // next apma partition
            output_segment_id++;

            position += cardinality;
//            COUT_DEBUG("cumulative cardinality: " << subtask_cardinality << ", end position: " << position);
            assert(position <= lastpos);
        }

        int64_t modulus = output_segment_id % segments_per_extent;

        if(modulus > 0){
            int64_t additional_segments = segments_per_extent - modulus;
            for(int i = 0; i < additional_segments; i++){
                int64_t cardinality = apma_partitions.cardinality_current();
                subtask_cardinality += cardinality;
                apma_partitions.move(1); // next apma partition
                output_segment_id++;

                position += cardinality;
                assert(position <= lastpos);
//                COUT_DEBUG("cumulative cardinality: " << subtask_cardinality << ", end position: " << position);
            }
        }
        subtask.m_position_end = position;
        subtask.m_input_extent_end = subtask.m_position_end / segment_capacity / segments_per_extent;
        subtask.m_partition_end_id = apma_partitions.partition_id();
        subtask.m_partition_end_offset = apma_partitions.partition_offset();
        subtask.m_output_extent_end = (output_segment_id -1) /segments_per_extent +1; // exclusive

//        {
//            // Check
//            int64_t segment_id = (subtask.m_position_start / segment_capacity /2) *2;
//            int64_t position = subtask.m_position_start;
//            int64_t stop = std::min<int64_t>((segment_id +1) * segment_capacity + m_task->m_pma->m_storage.m_segment_sizes[segment_id], subtask.m_position_end);
//            int64_t cardinality = 0;
//            while(position < subtask.m_position_end){
//                while(position < stop){
//                    if(cardinality > 0) cout << ", ";
//                    cout << "[" << cardinality << "@" << position << "] " << m_task->m_pma->m_storage.m_keys[position];
//                    position++;
//                    cardinality++;
//                }
//
//                segment_id += 2;
//                if(segment_id * segment_capacity < subtask.m_position_end){
//                    position = (segment_id +1) * segment_capacity - m_task->m_pma->m_storage.m_segment_sizes[segment_id];
//                    stop = position + m_task->m_pma->m_storage.m_segment_sizes[segment_id] + m_task->m_pma->m_storage.m_segment_sizes[segment_id +1];
//                    stop = std::min<int64_t>(stop, subtask.m_position_end);
//                }
//            }
//            cout << "\n";
//            COUT_DEBUG("final cardinality: " << cardinality);
//        }
//
        COUT_DEBUG(subtask << ", cardinality: " << subtask_cardinality);
        m_task->m_subtasks.push_back(subtask);
    }
}

void RebalancingWorker::do_execute_queue(){
    int64_t next_worker_id = 1;
    const bool is_rebalance = m_task->m_plan.m_operation == RebalanceOperation::REBALANCE || m_task->m_plan.m_operation == RebalanceOperation::RESIZE_REBALANCE;

    int64_t subtask_protect_extent = -1;
    int64_t input_extent_watermark = -1;

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
                    subtask_protect_extent = subtask.m_input_extent_end;
                    m_task->m_input_watermarks.push_back(subtask_protect_extent);
                    input_extent_watermark = *(std::max_element(begin(m_task->m_input_watermarks), end(m_task->m_input_watermarks)));
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
        do_execute_subtask(subtask, input_extent_watermark);
    }

    // finish rewiring the last extents
    if(is_rebalance && subtask_protect_extent >= 0 /* it may well possible that this worker was not able to fetch any task */){
        { // restrict the scope
            unique_lock<mutex> lock(m_task->m_workers_mutex);

            // remove the last watermark
            if(subtask_protect_extent >= 0){
                auto it = std::find_if(begin(m_task->m_input_watermarks), end(m_task->m_input_watermarks), [subtask_protect_extent](int64_t candidate){ return subtask_protect_extent == candidate; });
                assert(it != end(m_task->m_input_watermarks) && "Not found");
                m_task->m_input_watermarks.erase(it);
                subtask_protect_extent = -1;
            }

            assert(m_task->m_subtasks.empty() && "Otherwise this worker should still be in the while loop");
            while(!m_task->m_input_watermarks.empty()){ m_task->m_workers_condvar.wait(lock); }
        }

         reclaim_past_extents(-1);
    }
    assert(m_extents_to_rewire.empty());

    m_task->m_active_workers--;
    m_task->m_workers_condvar.notify_all();
}

void RebalancingWorker::do_execute_subtask(RebalancingTask::SubTask& subtask, int64_t input_extent_watermark) {
    switch(m_task->m_plan.m_operation){
    case RebalanceOperation::REBALANCE:
    case RebalanceOperation::RESIZE_REBALANCE: {
        RebalancingWorker::PartitionIterator apma_partitions{ m_task->m_plan.m_apma_partitions, subtask.m_partition_end_id, subtask.m_partition_end_offset };
        apma_partitions.move(-2);

        rebalance_rewire(m_task->m_ptr_storage, input_extent_watermark, subtask.m_position_end,
                subtask.m_output_extent_start, subtask.m_output_extent_end - subtask.m_output_extent_start,
                apma_partitions);
    } break;
    case RebalanceOperation::RESIZE: {
        RebalancingWorker::PartitionIterator apma_partitions{ m_task->m_plan.m_apma_partitions, subtask.m_partition_start_id, subtask.m_partition_start_offset };
        resize(&(m_task->m_pma->m_storage), m_task->m_ptr_storage, subtask.m_position_start,
                /* initial segment */ subtask.m_output_extent_start * m_task->m_ptr_storage->get_segments_per_extent(),
                /* number of segments */ (subtask.m_output_extent_end - subtask.m_output_extent_start) * m_task->m_ptr_storage->get_segments_per_extent(),
                apma_partitions);
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
    assert(!plan.is_insert() && "Insertion not supported in this method");

    // first copy all elements aside
    auto& memory_pool = m_task->m_pma->memory_pool();
    auto fn_deallocate = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> ptr_workspace_keys { memory_pool.allocate<int64_t>(plan.get_cardinality_after()), fn_deallocate };
    unique_ptr<int64_t, decltype(fn_deallocate)> ptr_workspace_values { memory_pool.allocate<int64_t>(plan.get_cardinality_after()), fn_deallocate };
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

//    // input
//    for(size_t i = 0; i < workspace_index; i++){
//        cout << "input[" << i << "]: " << workspace_keys[i] << "\n";
//    }

    // copy all elements from the workspace to their final positions in the storage
    workspace_index = 0;
    RebalancingWorker::PartitionIterator apma_partitions{ plan.m_apma_partitions, 0, 0 };
    for(int64_t output_segment_id = plan.m_window_start, end = plan.m_window_start + plan.m_window_length; output_segment_id < end; output_segment_id += 2){
        int64_t output_sz_lhs = apma_partitions.cardinality_current();
        int64_t output_sz_rhs = apma_partitions.cardinality_next();
        int64_t output_sz = output_sz_lhs + output_sz_rhs;
        int64_t output_displacement = (output_segment_id +1) * segment_capacity - output_sz_lhs;
        memcpy(keys + output_displacement, workspace_keys + workspace_index, output_sz * sizeof(keys[0]));
        memcpy(values + output_displacement, workspace_values + workspace_index, output_sz * sizeof(values[0]));

//        cout << "segment: " << output_segment_id << "\n";
//        for(size_t i = 0; i < output_sz; i++){
//            cout << "output[" << i << "]: " << keys[output_displacement +i] << "\n";
//        }

        set_separator_key(output_segment_id, workspace_keys[workspace_index]);
        set_separator_key(output_segment_id +1, workspace_keys[workspace_index + output_sz_lhs]);
        workspace_index += output_sz;
        apma_partitions.move(+2); // move ahead
    }
}

/*****************************************************************************
 *                                                                           *
 *  Spread with rewiring                                                     *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::rebalance_rewire(Storage* storage, int64_t input_extent_watermark, int64_t input_position, int64_t extent_start, int64_t extent_length, RebalancingWorker::PartitionIterator& apma_partitions) {
    const int64_t segment_capacity = storage->m_segment_capacity;
    const int64_t segments_per_extent = storage->get_segments_per_extent();

    for(int64_t i = extent_length -1; i>=0; i--){
        int64_t extent_id = extent_start + i;
        const bool use_rewiring = (extent_id <= input_extent_watermark);

        if(!use_rewiring){
//            COUT_DEBUG("without rewiring, extent_id: " << extent_id);
            // no need for rewiring, just spread in place as the source and destination refer to different extents
            int64_t offset = extent_id * segments_per_extent * segment_capacity;
            spread_rewire(storage, /*in/out*/ input_position, storage->m_keys + offset, storage->m_values + offset, extent_id, apma_partitions);
        } else {
            // get some space from the rewiring facility
            int64_t *buffer_keys{nullptr}, *buffer_values{nullptr};
            { // mutual exclusion
                scoped_lock<mutex> lock(storage->m_mutex);
                buffer_keys = (int64_t*) storage->m_memory_keys->acquire_buffer();
                buffer_values = (int64_t*) storage->m_memory_values->acquire_buffer();
            }
            m_extents_to_rewire.push_back(Extent2Rewire{extent_id, buffer_keys, buffer_values});
//            COUT_DEBUG("buffer_keys: " << (void*) buffer_keys << ", buffer values: " << (void*) buffer_values << ", extent: " << extent_id);
            spread_rewire(storage, /*in/out*/ input_position, buffer_keys, buffer_values, extent_id, apma_partitions);
        }
    }
}

void RebalancingWorker::spread_rewire(Storage* storage, int64_t& input_position, int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, RebalancingWorker::PartitionIterator& apma_partitions){
//    COUT_DEBUG("[destination] keys: " << destination_keys << ", values: " << destination_values << ", extent_id: " << extent_id << ", position: " << input_position);
    decltype(storage->m_segment_sizes) __restrict segment_sizes = storage->m_segment_sizes;
    const int64_t segments_per_extent = storage->get_segments_per_extent();
    const size_t segment_capacity = storage->m_segment_capacity;
    const size_t segment_base = extent_id * segments_per_extent;
    int64_t input_segment_id = ((input_position -1) / (2* segment_capacity)) *2; // even segment
    int64_t input_initial_displacement = input_segment_id * segment_capacity + segment_capacity - segment_sizes[input_segment_id];
    int64_t input_run_sz = input_position - input_initial_displacement;
//    COUT_DEBUG("extent: " << extent_id << ", initial segment: " << input_segment_id << ", run sz: " << input_run_sz << ", displacement: " << input_initial_displacement);
    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);

    int64_t* input_keys = storage->m_keys + input_initial_displacement;
    int64_t* input_values = storage->m_values + input_initial_displacement;

    for(int64_t output_segment_id_rel = segments_per_extent -2; output_segment_id_rel >= 0; output_segment_id_rel -= 2){ // relative to the current extent
        const int64_t output_run_sz_lhs = apma_partitions.cardinality_current();
        assert(output_run_sz_lhs < segment_capacity && "LHS Overflow: at least one slot per segment should be free");
        const int64_t output_run_sz_rhs = apma_partitions.cardinality_next();
        assert(output_run_sz_rhs < segment_capacity && "RHS Overflow: at least one slot per segment should be free");
//        COUT_DEBUG("[e: " << extent_id << ", s: " << output_segment_id_rel << "] left: " << output_run_sz_lhs << ", right: " << output_run_sz_rhs);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= (2 * segment_capacity -2));
        const size_t output_displacement = output_segment_id_rel * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* output_keys = destination_keys + output_displacement;
        int64_t* output_values = destination_values + output_displacement;

        while(output_run_sz > 0){
            size_t elements_to_copy = min(output_run_sz, input_run_sz);
            const size_t input_copy_offset = input_run_sz - elements_to_copy;
            const size_t output_copy_offset = output_run_sz - elements_to_copy;
            memcpy(output_keys + output_copy_offset, input_keys + input_copy_offset, elements_to_copy * sizeof(output_keys[0]));
            memcpy(output_values + output_copy_offset, input_values + input_copy_offset, elements_to_copy * sizeof(output_values[0]));
            input_run_sz -= elements_to_copy;
            output_run_sz -= elements_to_copy;

            if(input_run_sz == 0){
                assert(input_segment_id % 2 == 0 && "The input segment should be always an even segment");
                input_segment_id -= 2; // move to the previous even segment
                size_t input_displacement;
                if(input_segment_id >= 0){ // fetch the segment sizes
                    input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
                    // in the assert, the check for `output_run_sz == 0' is necessary as we may end up reading
                    // an invalid segment_id, below the window to rebalance. This can occur when we have exhausted
                    // the output to write.
                    assert(output_run_sz == 0 || (input_run_sz > 0 && input_run_sz <= 2 * segment_capacity));
                    input_displacement = input_segment_id * segment_capacity + segment_capacity - segment_sizes[input_segment_id];
                } else { // underflow
                    input_displacement = 0;
                }
                input_keys = storage->m_keys + input_displacement;
                input_values = storage->m_values + input_displacement;

//#if defined(DEBUG)
//                for(int64_t i = input_run_sz -1; i >= 0; i--){
//                    cout << "input[" << i << "]: <" << input_keys[i] << ", " << input_values[i] << ">\n";
//                }
//#endif
            }
        }

        set_separator_key(segment_base + output_segment_id_rel, output_keys[0]);
        set_separator_key(segment_base + output_segment_id_rel + 1, output_keys[output_run_sz_lhs]);

//        COUT_DEBUG("output segments: " << (segment_base + output_segment_id_rel) << " and " << (segment_base + output_segment_id_rel +1) << "; "
//                "output_run_sz: left=" << output_run_sz_lhs << ", right=" << output_run_sz_rhs << ", first element: " << output_keys[0] << ", last element: " << output_keys[(output_run_sz_lhs + output_run_sz_rhs) -1]);
//        COUT_DEBUG("input_segment_id: " << input_segment_id << ", input_run_sz: " << input_run_sz);

        apma_partitions.move(-2); // backwards
    }

    // update the final position
    input_position = input_keys - storage->m_keys + input_run_sz;
}


/*****************************************************************************
 *                                                                           *
 *   Resize                                                                  *
 *                                                                           *
 *****************************************************************************/
// left 2 right
void RebalancingWorker::resize(const Storage* input, Storage* output, int64_t input_start_position, int64_t output_segment_id, int64_t output_num_segments, RebalancingWorker::PartitionIterator& apma_partitions) {
    assert(input != nullptr && output != nullptr);
    assert(input->m_segment_capacity == output->m_segment_capacity && "Incompatible storages");
    assert(output_segment_id % 2 == 0 && "Expected an even entry point");
    assert(output_num_segments % 2 == 0 && "Expected an even number of segments");

    // input
    const int64_t segment_capacity = input->m_segment_capacity;
    int64_t input_segment_id = input_start_position / segment_capacity;
    int64_t input_offset = input_start_position % segment_capacity;
    int64_t input_run_sz = 0;
    int64_t* __restrict input_keys = input->m_keys + input_start_position;
    int64_t* __restrict input_values = input->m_values + input_start_position;
    uint16_t* __restrict input_sizes = input->m_segment_sizes;
    if(input_segment_id % 2 == 0){ // even segments
        input_run_sz = /* even segment */ segment_capacity - input_offset + /* odd segment */ input_sizes[input_segment_id +1];
    } else { // odd segments
        input_run_sz = input_sizes[input_segment_id] - input_offset;
    }
    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);

    // output
    int64_t* __restrict output_base_keys = output->m_keys + output_segment_id * output->m_segment_capacity;
    int64_t* __restrict output_base_values = output->m_values + output_segment_id * output->m_segment_capacity;
//    uint16_t* __restrict output_sizes = output->m_segment_sizes + output_segment_id;

    for(size_t i = 0; i < output_num_segments; i+=2){
        const int64_t output_run_sz_lhs = apma_partitions.cardinality_current();
        assert(output_run_sz_lhs < segment_capacity && "LHS overflow: at least one slot should be left free");
        const int64_t output_run_sz_rhs = apma_partitions.cardinality_next();
        assert(output_run_sz_rhs < segment_capacity && "RHS overflow: at least one slot should be left free");
//        COUT_DEBUG("[" << i << "] left: " << output_run_sz_lhs << ", right: " << output_run_sz_rhs);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= (2 * segment_capacity -2));
        size_t output_displacement = i * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* __restrict output_keys = output_base_keys + output_displacement;
        int64_t* __restrict output_values = output_base_values + output_displacement;

        while(output_run_sz > 0){
            size_t elements_to_copy = min(output_run_sz, input_run_sz);
            memcpy(output_keys, input_keys, elements_to_copy * sizeof(int64_t));
            memcpy(output_values, input_values, elements_to_copy * sizeof(int64_t));
            input_keys += elements_to_copy; input_values += elements_to_copy;
            output_keys += elements_to_copy; output_values += elements_to_copy;
            input_run_sz -= elements_to_copy;
            output_run_sz -= elements_to_copy;

            if(input_run_sz == 0){
                input_segment_id += 1 + (input_segment_id % 2 == 0); // move to the next even segment
                size_t input_displacement;
                if(input_segment_id < input->m_number_segments){ // fetch the segment sizes
                    assert(input_segment_id % 2 == 0);
                    input_offset = segment_capacity - input_sizes[input_segment_id];
                    input_run_sz = input_sizes[input_segment_id] + input_sizes[input_segment_id +1];
                    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);
                    input_displacement = input_segment_id * segment_capacity + input_offset;
                } else { // overflow
                    input_displacement = input->m_number_segments * segment_capacity;
                }
                input_keys = input->m_keys + input_displacement;
                input_values = input->m_values + input_displacement;
            }
        }

        set_separator_key(output_segment_id + i, output_keys[-output_run_sz_lhs -output_run_sz_rhs]);
        set_separator_key(output_segment_id + i + 1, output_keys[-output_run_sz_rhs]);

        // next APMA partitions
        apma_partitions.move(+2);
    }
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
 *   Rewiring                                                                *
 *                                                                           *
 *****************************************************************************/

void RebalancingWorker::reclaim_past_extents(int64_t input_extent_watermark){
    if(m_extents_to_rewire.empty() || m_extents_to_rewire.front().m_extent_id <= input_extent_watermark) return;

    Storage* storage = m_task->m_ptr_storage;
    int64_t segments_per_extent = storage->get_segments_per_extent();
    int64_t segment_capacity = storage->m_segment_capacity;

    unique_lock<mutex> lock(storage->m_mutex);
    do {
        auto& metadata = m_extents_to_rewire.front();
        auto extent_id = metadata.m_extent_id;
        auto offset_dst = extent_id * segments_per_extent * segment_capacity;
        auto keys_dst = storage->m_keys + offset_dst;
        auto keys_src = metadata.m_buffer_keys;
        auto values_dst = storage->m_values + offset_dst;
        auto values_src = metadata.m_buffer_values;
        m_extents_to_rewire.pop_front();
//        COUT_DEBUG("reclaim buffers for keys: " << keys_src << ", values: " << values_src);
        storage->m_memory_keys->swap_and_release(keys_dst, keys_src);
        storage->m_memory_values->swap_and_release(values_dst, values_src);
    } while (!m_extents_to_rewire.empty() && m_extents_to_rewire.front().m_extent_id > input_extent_watermark);
}

/*****************************************************************************
 *                                                                           *
 *   Segment cardinalities                                                   *
 *                                                                           *
 *****************************************************************************/
void RebalancingWorker::update_segment_cardinalities() {
    uint16_t* __restrict cardinalities = m_task->m_ptr_storage->m_segment_sizes;
    Gate* __restrict locks = m_task->m_ptr_locks;
    PartitionIterator partitions { m_task->m_plan.m_apma_partitions };
    int64_t segment_id = m_task->get_window_start();
    const int64_t segments_per_lock = m_task->m_pma->get_segments_per_lock();
    for(int64_t lock_id = m_task->get_lock_start(), end = m_task->get_lock_end(); lock_id < end; lock_id++){
        uint32_t cardinality = 0;
        for(int64_t j = 0; j < segments_per_lock; j++){
            uint32_t apma_card = partitions.cardinality();
            cardinalities[segment_id] = (uint16_t) apma_card;
            cardinality += apma_card;

            partitions.move(1);
            segment_id++;
        }

        locks[lock_id].m_cardinality = cardinality;
    }

#if defined(DEBUG)
    if(m_task->m_plan.m_operation == RebalanceOperation::RESIZE_REBALANCE || m_task->m_plan.m_operation == RebalanceOperation::RESIZE){
        uint64_t global_cardinality = m_task->m_pma->m_cardinality;
        uint64_t locks_cardinality = 0;
        for(uint64_t i = 0; i < m_task->get_lock_length(); i++){
            locks_cardinality += m_task->m_ptr_locks[i].m_cardinality;
        }
        uint64_t segments_cardinality = 0;
        for(uint64_t i = 0; i < m_task->m_ptr_storage->m_number_segments; i++){
            segments_cardinality += m_task->m_ptr_storage->m_segment_sizes[i];
        }

        COUT_DEBUG("global_cardinality: " << global_cardinality << ", locks_cardinality: " << locks_cardinality << ", segments_cardinality: " << segments_cardinality);
        assert(global_cardinality == locks_cardinality && locks_cardinality == segments_cardinality);
    }
#endif
}

/*****************************************************************************
 *                                                                           *
 *   InputIterator                                                           *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[InputPositionIterator::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }

RebalancingWorker::InputPositionIterator::InputPositionIterator(const Storage& storage, int64_t position): m_storage(storage), m_position(position), m_move2nextchunk(false){
//    COUT_DEBUG("position: " << position);
}

void RebalancingWorker::InputPositionIterator::operator+=(int64_t shift){
    assert(shift >= 0);
    if(shift == 0) return;
//    int64_t initial_shift = shift; // debug only
    roundup();

    const int64_t segment_capacity = m_storage.m_segment_capacity;
    uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;
    int64_t segment_id = (m_position / (2* segment_capacity)) *2; // even segment
    if(segment_id >= m_storage.m_number_segments) return; // depleted
    int64_t segment_start = (segment_id +1) * segment_capacity - cardinalities[segment_id];
    int64_t leftover = cardinalities[segment_id] + cardinalities[segment_id +1] - (m_position - segment_start);
    while(shift > 0){
        if(leftover - shift >= 0){
            m_position += shift;
            m_move2nextchunk = (leftover == shift);
            shift = 0; // done
        } else {
            shift -= leftover;
            segment_id += 2;
            if(segment_id >= m_storage.m_number_segments){
                m_position = (m_storage.m_number_segments -1) * segment_capacity + cardinalities[m_storage.m_number_segments -1];
                m_move2nextchunk = false; // it doesn't matter. The iterator has been depleted and it's unsafe to use
                shift = 0; // done
            } else {
                m_position = (segment_id +1) * segment_capacity - cardinalities[segment_id];
                leftover = cardinalities[segment_id] + cardinalities[segment_id +1];
            }
        }
    }

//    COUT_DEBUG("shift: " << initial_shift << ", final position: " << m_position);
}

RebalancingWorker::InputPositionIterator::operator int64_t() const{
    return m_position;
}

void RebalancingWorker::InputPositionIterator::roundup() {
    if(!m_move2nextchunk) return;
//    COUT_DEBUG("old position: " << m_position);
    const int64_t segment_capacity = m_storage.m_segment_capacity;
    uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;
    int64_t segment_id = ((m_position -1) / (2* segment_capacity)) *2; // even segment
    segment_id += 2;
    m_position = (segment_id +1) * segment_capacity - cardinalities[segment_id];
//    COUT_DEBUG("new position: " << m_position);
    m_move2nextchunk = false;
}

/*****************************************************************************
 *                                                                           *
 *   PartitionIterator                                                       *
 *                                                                           *
 *****************************************************************************/

RebalancingWorker::PartitionIterator::PartitionIterator(const VectorOfPartitions& partitions) : PartitionIterator(partitions, 0, 0){ }

RebalancingWorker::PartitionIterator::PartitionIterator(const VectorOfPartitions& partitions, int64_t partition_id, int64_t partition_offset) :
    m_partitions(partitions), m_partition_id(partition_id), m_partition_offset(partition_offset){
}

size_t RebalancingWorker::PartitionIterator::cardinality() const {
    return cardinality_current();
}

size_t RebalancingWorker::PartitionIterator::cardinality_current() const {
    if(m_partition_id >= m_partitions.size()){
        return 0;
    } else {
        auto partition = m_partitions[m_partition_id];
        size_t card_per_segment = partition.m_cardinality / partition.m_segments;
        size_t odd_segments = partition.m_cardinality % partition.m_segments;
        return card_per_segment + (m_partition_offset < odd_segments);
    }
}

size_t RebalancingWorker::PartitionIterator::cardinality_next() const {
    size_t partition_id = m_partition_id;
    size_t partition_offset = m_partition_offset + 1;

    if(partition_offset >= m_partitions[partition_id].m_segments){
        partition_id++;
        partition_offset = 0;

        if(partition_id >= m_partitions.size()) return 0;
    }

    auto partition = m_partitions[partition_id];
    size_t card_per_segment = partition.m_cardinality / partition.m_segments;
    size_t odd_segments = partition.m_cardinality % partition.m_segments;
    return card_per_segment + (partition_offset < odd_segments);
}

void RebalancingWorker::PartitionIterator::move_fwd(size_t N){
    while(N > 0){
        if(m_partition_id >= m_partitions.size()) return; // overflow

        size_t max_step = m_partitions[m_partition_id].m_segments - m_partition_offset;
        size_t step = min(max_step, N);
        N -= step;

        if(step == max_step){ // next set of partitions
            m_partition_id++;
            m_partition_offset = 0;
        } else {
            m_partition_offset += step;
        }
    }
}

void RebalancingWorker::PartitionIterator::move_bwd(size_t N){
    while(N > 0){
        if(m_partition_id == 0 && m_partition_offset == 0) return; // underflow

        size_t step = min(N, m_partition_offset +1);
        N -= step;
        if(step > m_partition_offset){
            if(m_partition_id > 0){
                m_partition_id--;
                m_partition_offset = m_partitions[m_partition_id].m_segments -1;
            } else {
                m_partition_offset = 0;
            }
        } else {
            m_partition_offset -= step;
        }
    }
}

void RebalancingWorker::PartitionIterator::move(int64_t N){
    if(N >= 0){ // move forwards
        move_fwd(N);
    } else { // move backwards
        move_bwd(-N);
    }
}

bool RebalancingWorker::PartitionIterator::end() const {
    return m_partition_id >= m_partitions.size();
}

size_t RebalancingWorker::PartitionIterator::partition_id() const{
    return m_partition_id;
}

size_t RebalancingWorker::PartitionIterator::partition_offset() const{
    return m_partition_offset;
}

/*****************************************************************************
 *                                                                           *
 *   Debug only                                                              *
 *                                                                           *
 *****************************************************************************/
uint64_t RebalancingWorker::debug_make_subtasks_validate_apma_partitions(){
#if defined(NDEBUG)
    return 0;
#else
//    COUT_DEBUG("Segment cardinalities:");
    auto& storage = m_task->m_pma->m_storage;

    // check the cardinality in the segments
    const int64_t window_start = m_task->get_window_start(); // incl
    const int64_t window_end = m_task->m_plan.m_operation == RebalanceOperation::REBALANCE ? m_task->get_window_end() : storage.m_number_segments; // excl
    const auto segment_cardinalities = storage.m_segment_sizes;
    int64_t segments_card = 0;
    for(int64_t segment_id = window_start; segment_id < window_end; segment_id++){
        size_t start, stop;
        if(segment_id % 2 == 0){
            stop = (segment_id+1) * storage.m_segment_capacity;
            start = stop - segment_cardinalities[segment_id];
        } else {
            start = segment_id * storage.m_segment_capacity;
            stop = start + segment_cardinalities[segment_id];
        }
        segments_card += segment_cardinalities[segment_id];

//        COUT_DEBUG("Segment[" << segment_id << "] cardinality: " << segment_cardinalities[segment_id] << ", index start: " << start << " (incl.), stop: " << stop << " (excl.)") ;
    }

    // check the cardinalities of the APMA iterator
    PartitionIterator apma_partitions { m_task->m_plan.m_apma_partitions };
    int64_t partit_cardinality = 0;
    while(!apma_partitions.end()){
        partit_cardinality += apma_partitions.cardinality_current();
        apma_partitions.move(1);
    }

    if(partit_cardinality != m_task->m_plan.get_cardinality_after() || partit_cardinality != segments_card){
        // check failed, the next block serves only to print to stdout the cardinalities of the partitions & segments

        COUT_DEBUG_FORCE("Segment cardinalities: ");
        for(int64_t segment_id = window_start; segment_id < window_end; segment_id++){
            size_t start, stop;
            if(segment_id % 2 == 0){
                stop = (segment_id+1) * storage.m_segment_capacity;
                start = stop - segment_cardinalities[segment_id];
            } else {
                start = segment_id * storage.m_segment_capacity;
                stop = start + segment_cardinalities[segment_id];
            }

            COUT_DEBUG_FORCE("Segment[" << segment_id << "] cardinality: " << segment_cardinalities[segment_id] << ", index start: " << start << " (incl.), stop: " << stop << " (excl.)") ;
        }

        COUT_DEBUG_FORCE("--- CARDINALITY MISMATCH: the count returned from the partition iterator does not match the one from the segments. Cardinality segments: " << segments_card << ", cardinality plan: " << m_task->m_plan.get_cardinality_after() << ", cardinality partition iterator: " << partit_cardinality);
        assert(0 && "cardinality mismatch, error in the partition iterator");
    }

    // last position in the array valid for this subarray
    return (window_end -1) * storage.m_segment_capacity + storage.m_segment_sizes[window_end -1];
#endif
}

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

    // store the elements one by one in the vector _debug_items
    for(int64_t segment_id = window_start; segment_id < window_end; segment_id += 2){
        const int64_t position_start = (segment_id +1) * storage.m_segment_capacity - storage.m_segment_sizes[segment_id]; // incl.
        const int64_t position_end = position_start + storage.m_segment_sizes[segment_id] + storage.m_segment_sizes[segment_id +1]; // excl.

//        COUT_DEBUG("cardinality [" << segment_id << "]: " << storage.m_segment_sizes[segment_id] );
//        COUT_DEBUG("cardinality [" << segment_id +1 << "]: " << storage.m_segment_sizes[segment_id +1] );

        for(int64_t i = position_start; i < position_end; i++){
            _debug_items.emplace_back(keys[i], values[i]);
        }
    }
#endif
}

void RebalancingWorker::debug_content_after(){
#if defined(DEBUG_CONTENT)
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


    // Read one by one the elements in the vector _debug_items
    uint64_t j = 0;
    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id += 2){
        const int64_t position_start = (segment_id +1) * storage.m_segment_capacity - storage.m_segment_sizes[segment_id]; // incl.
        const int64_t position_end = position_start + storage.m_segment_sizes[segment_id] + storage.m_segment_sizes[segment_id +1]; // excl.

        for(int64_t i = position_start; i < position_end; i++){
            assert(j < _debug_items.size() && "Too many items the output storage");
            auto key_before = _debug_items[j].first;
            auto value_before = _debug_items[j].second;
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
                        COUT_DEBUG_FORCE("[" << k << "] input: <" << _debug_items[k].first << ", " << _debug_items[k].second << ">, "
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

} // baseline
