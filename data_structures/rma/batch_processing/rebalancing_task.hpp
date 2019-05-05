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

#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <mutex>
#include <ostream>
#include <vector>

#include "rebalance_plan.hpp"
#include "rebalancing_statistics.hpp"

namespace data_structures::rma::common { class StaticIndex; } // forward decl.

namespace data_structures::rma::batch_processing {

// forward declarations
class ClientContextQueue;
class Gate;
class PackedMemoryArray;
class RebalancingMaster;
struct Storage;

class RebalancingTask {
public:
    PackedMemoryArray * const m_pma;
    RebalancingMaster* m_master;
    Gate* m_ptr_locks;
    common::StaticIndex* m_ptr_index;
    Storage* m_ptr_storage;

    RebalancePlan m_plan; // the window & the operation to perform
    uint32_t m_window_id; // the current window. Only used by the master to traverse the calibrator tree
    int64_t m_cardinality_min; // the minimum number of elements allowed in the current window
    int64_t m_cardinality_max; // as above, the max number of elements for the current window

    struct WaitToComplete { uint64_t m_lock_id; int64_t m_cardinality ;};
    std::vector<WaitToComplete> m_wait_to_complete; // extents waiting to complete
    int64_t m_blocked_on_lock = -1; // only used by the Master to keep track which extent need to be processed before this task can be executed
    size_t m_num_locks = 0; // keep track of the previous number of gates, before a resize
    bool m_forced_resize; // true if |cardinality| < capacity /2

    // Bulk Loading
    using insertion_t = std::pair<int64_t, int64_t>;
    std::vector<ClientContextQueue*> m_blkld_elts; // writer queues with the elements to insert, possibly unsorted

    // Only used by the workers
    struct SubTask {
        int64_t m_input_position_start, m_input_position_end; // position_start is inclusive, position_end is exclusive
        int64_t m_input_extent_start, m_input_extent_end;
        int64_t m_output_extent_start, m_output_extent_end;
        int64_t m_blkload_start, m_blkload_end;
        int64_t m_cardinality; // total cardinality (input + bulk loading)
    };
    std::vector<SubTask> m_subtasks;
    std::vector<int64_t> m_input_watermarks;
    std::mutex m_workers_mutex;
    std::condition_variable m_workers_condvar;
    std::atomic<int64_t> m_active_workers = 0;

#if defined(PROFILING)
    RebalancingStatistics m_statistics; // time spent to perform the task
#endif

    /**
     * Constructor
     */
    RebalancingTask(PackedMemoryArray* pma, RebalancingMaster* master, Gate* gate);

    /**
     * Can we execute this task?
     */
    bool ready_for_execution() const noexcept;

    /**
     * True if the window of this instance is a subset of the given task
     */
    bool is_subset_of(size_t lock_start, size_t lock_length) const;

    /**
     * Whether the current task overlaps with the given window
     */
    bool overlaps(size_t lock_start, size_t lock_length) const;

    /**
     * True if the window of this instance is a superset of the given window
     */
    bool is_superset_of(size_t lock_start, size_t lock_length) const;

    /**
     * The first segment of this window
     */
    int64_t get_window_start() const noexcept;

    /**
     * The number of segments in this window
     */
    int64_t get_window_length() const noexcept;

    /**
     * The last segment of this window (excl)
     */
    int64_t get_window_end() const noexcept;

    /**
     * The first lock of this window
     */
    int64_t get_lock_start() const noexcept;

    /**
     * The number of locks in this window
     */
    int64_t get_lock_length() const noexcept;

    /**
     * The last lock of this window
     */
    int64_t get_lock_end() const noexcept;

    /**
     * The first extent of this window
     * Note: extent != lock/gate, an extent is the minimum granularity for rewiring (e.g. 16 segments), a lock or gate is the length of
     *       segments protected by a single gate (e.g. 4 segments)
     */
    int64_t get_extent_start() const noexcept;

    /**
     * The number of extents in this window. It can be zero, if the window is less than the size of a single extent.
     */
    int64_t get_extent_length() const noexcept;

    /**
     * Set the rebalancing window
     */
    void set_lock_window(int64_t lock_start, int64_t lock_length) noexcept;

    /**
     * Check whether a suitable window to rebalance has been computed
     */
    bool is_rebalancing_window_computed() const noexcept;
};

// for debugging purposes
std::ostream& operator<<(std::ostream& out, const RebalancingTask* task);
std::ostream& operator<<(std::ostream& out, const RebalancingTask& task);
std::ostream& operator<<(std::ostream& out, const RebalancingTask::SubTask& subtask);

} // namespace
