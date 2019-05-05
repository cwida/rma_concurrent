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
#include <deque>
#include <mutex>
#include <thread>

#include "rebalancing_task.hpp"

namespace data_structures::rma::batch_processing {

// Forward declarations
class ClientContextQueue;
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

    class InputIterator {
        const Storage& m_storage;
        /* const */ int64_t m_start; // first position of the window (incl)
        /* const */ int64_t m_end; // last position of the window (excl)
        int64_t m_position; // current position
        int64_t m_overflow = 0; // number of positions that the iterator is above (or below) the current window
        enum class MarkerPosition { START /* start of a chunk */, MIDDLE, END /* one after the last pos of a chunk */ };
        MarkerPosition m_feature;

        // move the current position forwards by the given amount
        void move_forwards_by(int64_t shift);

        // move the current position backwards by the given amount
        void move_backwards_by(int64_t shift);
    public:
        InputIterator(const Storage& storage, int64_t window_start, int64_t window_end);

        // move the cursor either backwards (<0) or forwards (>0)
        void move(int64_t shift);

        // when m_feature == END, the cursor is one past the last position of the last current odd segment
        // invoking this method, it moves the cursor at the start of the next even segment
        void fetch_next_chunk();

        // return the key at the current position, or -inf/+inf if the cursor is out of the window
        int64_t get_key() const;

        // positions of the iterator
        int64_t get_absolute_position_start() const;
        int64_t get_absolute_position_current() const;
        int64_t get_absolute_position_end() const;

        // syntactic sugar
        void operator+=(int64_t shift) { move(shift); }
        void operator-=(int64_t shift) { move(-shift); }
        void operator++(int){ move(+1); }
        void operator--(int){ move(-1); }

        // debug only
        void dump(std::ostream& out) const;
    };
    friend std::ostream& operator<<(std::ostream& out, const RebalancingWorker::InputIterator& subtask);

    // Retrieve one by one the elements to bulk load
    class BulkLoadingIterator{
        using element_list_t = std::vector<ClientContextQueue*>;
        const element_list_t& m_vectors; // list of vectors of the elts to insert, from the task
        int64_t m_vect_start; // fist vector to examine (inclusive)
        int64_t m_vect_last; // last vector to examine (inclusive)
        int64_t m_vect_pos; // current vector being examined

        int64_t m_pos_start; // the first position in the first vector
        int64_t m_pos_end; // the last position in the last vector (m_vect_end -1)
        int64_t m_position; // next element to fetch in the current vector

        size_t m_cardinality; // the total amount of elements in the loader (whether visited or not)

        static void abs2vect(const element_list_t& vectors, int64_t absolute_position, int64_t* out_vect_id, int64_t* out_vect_offset);
        static int64_t vect2abs(const element_list_t& vectors, int64_t vect_id, int64_t vect_offset);

    public:
        BulkLoadingIterator(const RebalancingTask* task);
        BulkLoadingIterator(const RebalancingTask* task, size_t start /* incl */, size_t end /* excl */);

        void operator++(int);
        void operator--(int);

        // the next element from the queue, or <+inf, +inf> if the iterator has been depleted
        std::pair<int64_t, int64_t> get() const;

        int64_t get_absolute_position_start() const;
        int64_t get_absolute_position_current() const;
        int64_t get_absolute_position_end() const;

        size_t cardinality() const; // total amount of elements in the loader (including those already visited)
        bool empty() const;

        bool is_behind() const;
        bool is_ahead() const;
    };

private:
    void main_thread();

    void do_execute();
    void do_execute_single(); // for windows < extent
    void do_execute_queue(); // for windows >= extent
    void do_execute_subtask(RebalancingTask::SubTask& subtask, int64_t input_extent_watermark);

    void make_subtasks();

    // Check the content of the window considered before & after the rebalancing
    void debug_content_before();
    void debug_content_after();

    void execute0(RebalancingTask* task, int64_t worker_id);

    void do_execute(const RebalancingTask::SubTask& subtask);

    void rebalance_rewire(Storage* storage, int64_t input_extent_watermark, int64_t input_position_start, int64_t input_position_end, BulkLoadingIterator& loader, int64_t output_extent_start, int64_t output_extent_length, size_t cardinality);

    void resize(const Storage* input, Storage* output, int64_t input_pos_start, int64_t input_pos_end, BulkLoadingIterator& loader, int64_t output_window_start, int64_t output_window_length, size_t output_cardinality);

    void spread_rewire(Storage* storage, int64_t& input_position_start, const int64_t input_position_end, BulkLoadingIterator& loader, int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, size_t cardinality);

    void spread_local();

    void set_separator_key(uint64_t segment_id, int64_t key);

    void reclaim_past_extents(int64_t input_extent_watermark);

    void update_segment_cardinalities();

    // sort the vectors to load in the task
    void sort_blkload_elts();

    // remove all elts from the writers' queues (clean up)
    void clear_blkload_queues();

public:
    RebalancingWorker();

    ~RebalancingWorker();

    void start();

    void stop();

    void execute(RebalancingTask* task);
};

// for debugging purposes
std::ostream& operator<<(std::ostream& out, const RebalancingWorker::InputIterator& subtask);

} // namespace
