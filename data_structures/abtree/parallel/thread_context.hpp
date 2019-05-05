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

#include "common/spin_lock.hpp"

#include "third-party/art-olc/Epoche.h"
#include "third-party/art-olc/Tree.h"

namespace data_structures::abtree::parallel {

/**
 * A ThreadContext represents state information associated to a single thread operating on the
 * data structure. The information contained is :
 * - the current epoch of the thread (+ inf if the thread is inactive)
 */
class ThreadContext {
private:
    uint64_t m_epoch; // current epoch, +inf if the thread is inactive

public:
    /**
     * Create the a new thread context, associated to a given ART index
     */
    ThreadContext();

    /**
     * Destructor
     */
    ~ThreadContext();

    /**
     * Return the current epoch
     */
    uint64_t epoch() const { return m_epoch; }

    /**
     * (Re)-join in the current absolute epoch (that is the current hw timestamp)
     */
    void hello();

    /**
     * Leave the epoch
     */
    void bye();

    /**
     * Thread thread_id of the current thread, in [0, numThreads)
     */
    static thread_local int thread_id; // -1 on init
};

/**
 * A safe enough list of thread contexts
 */
class ThreadContextList {
    constexpr static uint64_t m_capacity = 128;
    uint64_t m_size = 0;
    ThreadContext m_contexts[m_capacity];
    mutable common::SpinLock m_spinlock;

public:
    ThreadContextList();
    ~ThreadContextList();

    void resize(uint64_t size);

    // Current size
    uint64_t size() const { return m_size; }

    ThreadContext* operator[](uint64_t index) const;

    uint64_t min_epoch() const;

    // Retrieve the context associated to the current thread
    ThreadContext& my_context() const;
};

/**
 * RAII class to enter & leave an epoch within the current epoch
 */
class ScopedContext {
private:
    ThreadContext* m_context; // the current context
public:
    /**
     * Join a new epoch in the current thread. It assumes that ThreadContext::thread_id has been set before
     */
    ScopedContext(const ThreadContextList& list);

    /**
     * Leave the current epoch
     */
    ~ScopedContext();

    /**
     * The actual context associated to this class
     */
    ThreadContext& context();

    /**
     * Rejoin a new absolute epoch
     */
    void tick();
};

} // namespace data_structures::abtree::parallel
