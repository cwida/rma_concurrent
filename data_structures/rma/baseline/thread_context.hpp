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
#include <condition_variable>
#include <mutex>
#include <vector>

namespace data_structures::rma::baseline {

// Forward declarations
class PackedMemoryArray;
class ThreadContext;
class ThreadContextList;

class ThreadContext {
friend class ThreadContextList;
private:
//    union {
//        struct {
            uint64_t m_timestamp; // the current timestamp (or epoch) for the current thread. Only utilised for the purposes of the Garbage Collector
            bool m_hosted; // whether a thread owns this context
            mutable std::mutex m_mutex; // auxiliary mutex for the condition variable. It's acquired when a thread is operating
            std::condition_variable_any m_condition_variable; // auxiliary cond. variable to block this thread when an extent is full
            bool m_spurious_wake; // avoid releasing the condition variable if not explicitly instructed so
//        };
//        uint8_t PADDING[8]; // Use a full cache block for this data structure
//    };

    static thread_local int m_thread_id; // the ID of the current thread

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
     * Block the current thread
     */
    void wait();

    /**
     * Wake up the associated thread
     */
    void notify();
};


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
