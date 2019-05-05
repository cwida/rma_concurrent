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

#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/circular_array.hpp"
#include "rebalancing_pool.hpp"
#include "rebalancing_task.hpp"
#include "wakelist.hpp"

namespace data_structures::rma::one_by_one {

class RebalancingMaster {
private:
    PackedMemoryArray* m_instance; // the pma instance

    ::common::CircularArray<RebalancingTask*> m_todo; // tasks postponed for execution
    std::vector<const RebalancingTask*> m_executing; // tasks currently in execution

    // Internal tasks
    struct InternalTask {
        enum class Type { Invalid, Rebalance, TaskDone, ClientExit, Stop };
        Type m_type;
        uint64_t m_payload;
        std::string to_string() const; // for debug purposes only
    };
    // Concurrent queue
    mutable std::mutex m_mutex;
    ::common::CircularArray<InternalTask> m_queue;
    std::condition_variable m_condvar;
    std::thread m_handle; // Handle to the controller thread
    bool m_resizing = false; // Whether the whole PMA is currently being resized
    RebalancingPool m_thread_pool; // Thread pool

    // Check if a rebalancing window is already on execution or in the todo list for the given gate id
    bool ignore_lock(uint64_t lock_id) const;

    const RebalancingTask* find_child_on_execution(uint64_t lock_start, uint64_t lock_length) const;

    // Lock a single gate && read its cardinality
    uint64_t acquire_lock(RebalancingTask* task, uint64_t lock_id);

    // Unlock a single gate && wake up the first thread in the queue
    void release_lock(uint64_t lock_id, WakeList& worker_list);

    // Release all threads in a single gate, and invalidate its fence keys
    void cleanup_lock(Gate& gate, WakeList& worker_list);

    // Process the list of tasks in the todo list
    void process_todo_list();

    // Find the task created to process the given lock id
    RebalancingTask* get_todo_task_for(size_t lock_id) const;

    // Remove the lock in the wait_to_complete list
    void wait_to_complete_remove(RebalancingTask* task, uint64_t lock_id);

    // Increase the size of the window
    int64_t next_window_length(int64_t current_window_length) const;

    // Validate the cardinalities of the locks and segments before launching a task
    void debug_validate_launch_task(RebalancingTask* task) const;

protected:
    void main_thread(); // Controller

    // Start rebalancing from a given gate
    RebalancingTask* rebal_init(uint64_t lock_id);

    // Find the window to rebalance for the given task
    void rebal_resume(RebalancingTask* task);

    // Execute the given task
    void launch_task(RebalancingWorker* worker, RebalancingTask* task);

public:
    RebalancingMaster(PackedMemoryArray* pma, uint64_t num_workers);

    ~RebalancingMaster();

    void start();

    /**
     * Terminates the Master node for the rebalancing
     * Before of shutting down this instance, all client threads must have been terminated, and no jobs must be in execution.
     */
    void stop();

    /**
     * Workers pool
     */
    RebalancingPool& thread_pool();

    /**
     * Request the rebalancing of the given gate
     */
    void rebalance(uint64_t gate_id);

    /**
     * Signal the exit of a client from a given gate
     */
    void exit(uint64_t gate_id);

    /**
     * Signal the end of a workers task
     */
    void task_done(RebalancingTask* task);
};

} // namespace
