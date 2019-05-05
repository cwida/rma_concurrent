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

#include <cinttypes>
#include <chrono>
#include <future>

#include "common/circular_array.hpp"
#include "common/miscellaneous.hpp"
#include "common/spin_lock.hpp"

namespace data_structures::rma::batch_processing {

// forward declarations
class ClientContext;
class ClientContextQueue;
class RebalancingTask;
struct Storage;
class WakeList;

class Gate {
public:
    const uint32_t m_window_start; // the first segment of this gate
    const uint32_t m_window_length; // the number of segments controlled by this gate
    enum class State {
        FREE, // no threads are operating on this gate
        READ, // one or more readers are active on this gate
        WRITE, // one & only one writer is active on this gate
        TIMEOUT, // set by the timer manager on an occupied gate, the last reader/writer must ask to rebalance the gate
        REBAL, // this gate is closed and it's currently being rebalanced
    };
    State m_state = State::FREE; // whether reader/writer/rebalancing in progress?
    ::common::SpinLock m_spin_lock; // sync the access to the gate
#if !defined(NDEBUG) // for debugging purposes
    bool m_locked = false; // keep track whether the spin lock has been acquired, for debugging purposes
    int64_t m_owned_by = -1; // if the mutex is owned, report the thread id of the last thread that acquired the lock
#endif
    int32_t m_num_active_threads; // how many readers are accessing this gate?
    uint32_t m_cardinality; // the total number of elements in this gate
    int64_t m_fence_low_key; // the minimum key that can be stored in this gate (inclusive)
    int64_t m_fence_high_key; // the maximum key that can be stored in this gate (exclusive)
    ClientContextQueue* m_async_queue; // queue to add/remove elements asynchronously
    std::chrono::steady_clock::time_point m_time_last_rebal; // the last time this gate was rebalanced

    struct SleepingBeauty{
        State m_purpose; // either read or write
        std::promise<void>* m_promise; // the thread waiting
    };
    ::common::CircularArray<SleepingBeauty> m_queue; // a queue with the threads being on the wait
    int64_t* m_separator_keys; // the separator keys for the segments in this gate


public:
    // The result of check_fence_keys()
    enum class Direction{
        LEFT, // the given key is lower than m_fence_low_key, check the gate on the left
        RIGHT, // the given key is greater or equal than m_fence_high_key, check the gate on the right
        GO_AHEAD, // the given key is in the interval of the gate fence keys
        INVALID, //
    };
    Direction check_fence_keys(int64_t key) const;

private:
    // Constructor
    Gate(uint32_t window_start, uint32_t window_length);

public:

    // Acquire the spin lock protecting this gate
    void lock(){
        m_spin_lock.lock();
#if !defined(NDEBUG)
        ::common::barrier();
        assert(m_locked == false && "Spin lock already acquired");
        m_locked = true;
        m_owned_by = ::common::get_thread_id();
        ::common::barrier();
#endif
    }

    // Release the spin lock protecting this gate
    void unlock(){
#if !defined(NDEBUG)
        ::common::barrier();
        assert(m_locked == true && "Spin lock already released");
        m_locked = false;
        m_owned_by = -1;
        ::common::barrier();
#endif
        m_spin_lock.unlock();
    }

    /**
     * Retrieve the segment associated to the given key.
     * Precondition: the gate has been acquired by the thread
     */
    uint64_t find(int64_t key) const;

    /**
     * The first segment in this gate
     */
    int64_t window_start() const {
        return m_window_start;
    }

    /**
     * The number of segments in this gate
     */
    int64_t window_length() const {
        return m_window_length;
    }

    /**
     * The ID associated to this gate\lock
     */
    int64_t lock_id() const {
        // assuming all gates have the same length
        return m_window_start / m_window_length;
    }

    /**
     * Set the separator key at the given offset
     */
    void set_separator_key(size_t segment_id, int64_t key);

    /**
     * Retrieve the segment key for a given segment
     */
    int64_t get_separator_key(size_t segment_id) const;

    /**
     * Retrieve the next writer or set of readers from the queue to be wake up.
     * Precondition: the caller holds the lock for this gate
     */
    void wake_next(WakeList& wake_list);
    void wake_next(ClientContext* context); // shortcut

    /**
     * Retrieve the list of workers to wake up in this gate
     */
    void wake_all(WakeList& wake_list);

    /**
     * Allocate an array of locks
     */
    static Gate* allocate(uint64_t num_locks, uint64_t segments_per_lock);

    /**
     * Deallocate an array of locks
     */
    static void deallocate(Gate* array);
};


inline
uint64_t Gate::find(int64_t key) const {
    assert(m_fence_low_key <= key && key <= m_fence_high_key && "Fence keys check: the key does not belong to this gate");
    int i = 0, sz = m_window_length -1;
    int64_t* __restrict keys = m_separator_keys;
    while(i < sz && keys[i] <= key) i++;
    return m_window_start + i;
}

} // namespace
