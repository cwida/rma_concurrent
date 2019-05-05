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
#include <iostream>
#include <mutex>
#include <vector>

#include "common/circular_array.hpp"
#include "common/spin_lock.hpp"
#include "wakelist.hpp"

namespace data_structures::rma::one_by_one {

// Forward declaration
class PackedMemoryArray;
class ThreadContext;
class ThreadContextList;

class ThreadContext {
friend class ThreadContextList;
private:
    friend ::std::ostream& operator<<(::std::ostream& out, const ThreadContext& operation);

    uint64_t m_timestamp; // the current timestamp (or epoch) for the current thread. Only utilised for the purposes of the Garbage Collector
    bool m_hosted; // whether a thread owns this context

    static thread_local int m_thread_id; // the ID of the current thread

public:
    struct Update { // The operation [update] to perform
        bool m_is_insert; // true => insertion, false => deletion
        int64_t m_key; // the key to insert
        int64_t m_value; // if insertion, the value to insert, if deletion it's ignored
    };
    WakeList m_wakelist; // cached list
private:
    bool m_has_update; // if there is an update to perform
    Update m_current_update; // current update to perform
    ::common::CircularArray<Update> m_queue_next; // items to insert/delete (supposedly) in the same segment
    mutable ::common::SpinLock m_queue_mutex; // spin lock to protect the access to m_queue

public:
    /**
     * Empty context
     */
    ThreadContext();

    /**
     * Destructor
     */
    ~ThreadContext();

    void enter();

    void exit();

    bool busy() const;

    /**
     * Register the current thread
     */
    static void register_thread(int thread_id);

    /**
     * Unregister the given thread
     */
    static void unregister_thread();

    /**
     * Retrieve the current thread id
     */
    static int thread_id();

    /**
     * Retrieve the current epoch of this thread
     */
    uint64_t epoch() const noexcept;

    /**
     * Set the current timestamp for the current (worker) thread
     */
    void hello() noexcept;

    /**
     * Remove the timestamp for the current (worker) thread
     */
    void bye() noexcept;

    /**
     * Enqueue an item to insert/delete.
     * Return true on success, false if the item was not enqueue (the queue is full)
     */
    bool enqueue(const Update& update);

    /**
     * Check whether there is an operation scheduled for the current worker
     */
    bool has_update() const noexcept;

    /**
     * The update to perform
     */
    Update& get_update() noexcept;

    /**
     * Set the current operation for this worker
     */
    void set_update(bool is_insertion, int64_t key, int64_t value) noexcept;

    /**
     * Unset the current operation to perform
     */
//    void unset_update() noexcept;

    /**
     * Fetch the next operation from the queue
     */
    void fetch_local_queue() noexcept;
    void fetch_local_queue_unsafe() noexcept;

    /**
     * Wake up the workers in the wakelist
     */
    void process_wakelist() noexcept { m_wakelist(); }
};

// For debugging purposes
::std::ostream& operator<<(::std::ostream& out, const ThreadContext& context);
::std::ostream& operator<<(::std::ostream& out, const ThreadContext* context);
::std::ostream& operator<<(::std::ostream& out, const ThreadContext::Update& operation);

// Inline methods
inline bool ThreadContext::has_update() const noexcept { return m_has_update; }
inline ThreadContext::Update& ThreadContext::get_update() noexcept { return m_current_update; }
//inline void ThreadContext::unset_update() noexcept { m_has_update = false; }

/**
 * A safe enough list of thread contexts
 */
class ThreadContextList {
    constexpr static uint64_t m_capacity = 128;
    uint64_t m_size = 0;
    ThreadContext m_contexts[m_capacity];
    mutable std::mutex m_mutex;

public:
    ThreadContextList();
    ~ThreadContextList();

    void resize(uint64_t size);

    // Current size
    uint64_t size() const { return m_size; }

    ThreadContext* operator[](uint64_t index) const;

    uint64_t min_epoch() const;
};

/**
 * Automatically set & reset the state of this thread, based on the scope
 */
class ScopedState {
protected:
    ThreadContext* m_context;
public:
    ScopedState(const PackedMemoryArray* pma);
    ScopedState(ThreadContext* context);
    ~ScopedState();
};

} // namespace
