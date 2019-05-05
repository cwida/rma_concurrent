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

#include <cstddef>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include "partition.hpp"
#include "rebalancing_task.hpp"

namespace data_structures::rma::one_by_one {

// Forward declarations
class RebalancingPool;
struct Storage;

class RebalancingWorker {
    RebalancingTask* m_task; // the task to perform
    int64_t m_worker_id; // coordinator worker ?
    std::mutex m_mutex; // controller mutex
    std::condition_variable m_condition_variable; // sync the controller on the current task
    std::thread m_handle; // current thread handle
    struct Extent2Rewire{ int64_t m_extent_id; int64_t* m_buffer_keys; int64_t* m_buffer_values; };
    std::deque<Extent2Rewire> m_extents_to_rewire; // a list of extents to be rewired

    class InputPositionIterator{
        const Storage& m_storage;
        int64_t m_position;
        bool m_move2nextchunk;
    public:
        InputPositionIterator(const Storage& storage, int64_t position);

        void operator+=(int64_t shift);

        operator int64_t() const;

        void roundup();
    };

    // Iterator over the partitions created by the APMA algorithm
    class PartitionIterator{
        const VectorOfPartitions& m_partitions;
        size_t m_partition_id; //=0, current partition
        size_t m_partition_offset; //=0, current offset in the partition

        void move_fwd(size_t N); // move ahead
        void move_bwd(size_t N); // move back
    public:
        PartitionIterator(const VectorOfPartitions& partitions);

        PartitionIterator(const VectorOfPartitions& partitions, int64_t partition_id, int64_t partition_offset);

        /**
         * Get the cardinality of the current partition
         */
        size_t cardinality_current() const;
        size_t cardinality() const; // alias for #cardinality_current

        /**
         * Get the cardinality of the next partition
         */
        size_t cardinality_next() const;

        /**
         * Move the current partition by N
         */
        void move(int64_t N); // move ahead if N > 0, else move back

        /**
         * Return true if we reached the last partition
         */
        bool end() const;

        // Current position
        size_t partition_id() const;
        size_t partition_offset() const;
    };

private:
    void main_thread();

    void do_execute();
    void do_execute_single(); // for windows < extent
    void do_execute_queue(); // for windows >= extent
    void do_execute_subtask(RebalancingTask::SubTask& subtask, int64_t input_extent_watermark);

    void make_subtasks();

    // Debug functions for make_subtasks
    uint64_t debug_make_subtasks_validate_apma_partitions();

    // Check the content of the window considered before & after the rebalancing
    void debug_content_before();
    void debug_content_after();

    void execute0(RebalancingTask* task, int64_t worker_id);

    void do_execute(const RebalancingTask::SubTask& subtask);

    void rebalance_rewire(Storage* storage, int64_t input_extent_watermark, int64_t input_position, int64_t extent_start, int64_t extent_length, RebalancingWorker::PartitionIterator& apma_partitions);

    void resize(const Storage* input, Storage* output, int64_t input_start, int64_t output_segment_id, int64_t output_num_segments, RebalancingWorker::PartitionIterator& apma_partitions);

    void spread_rewire(Storage* storage, int64_t& input_position, int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, RebalancingWorker::PartitionIterator& apma_partitions);

    void spread_local();

    void set_separator_key(uint64_t segment_id, int64_t key);

    void reclaim_past_extents(int64_t input_extent_watermark);

    void update_segment_cardinalities();

public:
    RebalancingWorker();

    ~RebalancingWorker();

    void start();

    void stop();

    void execute(RebalancingTask* task);
};

} // namespace
