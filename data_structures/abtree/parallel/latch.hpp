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
#include <cassert>
#include <iostream>

namespace data_structures::abtree::parallel {

class Latch {
    // Convention:
    // -2: the latch is invalid, it fires an Abort exception. Once invalid, it cannot be
    //     reversed. This is used to detect deleted nodes in the tree.
    // -1: the latch has been acquired in write mode, only one thread is allowed
    // 0: the latch is free
    // +1 - +inf: the latch has been acquired in read mode, multiple readers can access it
    std::atomic<int64_t> m_latch {0};

public:

    /**
     * When accessing an invalid node, because its latch is set to invalid, then
     * an exception with type Abort is fired.
     */
    class Abort { };

    /**
     * Acquire the latch in read mode, fire an Abort exception if the latch is invalid (the associated node has been deleted)
     */
    void lock_read(){
        int64_t expected { 0 };
        while(!m_latch.compare_exchange_weak(/* by ref, out */ expected, /* by value */ expected+1,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed)){
            if(expected == -2)
                throw Abort {}; // this latch has been invalidated and the node deleted
            else if(expected == -1) // a writer is operating on this node, wait
                expected = 0;
            // else nop, try again with expected > 0, multiple readers
        }
    }

    /**
     * Releases the latch previously acquired in read mode. This method should never Abort if the protocol to
     * acquire/release the latch has been properly respected
     */
    void unlock_read(){
        assert(m_latch > 0 && "The latch should have been previously acquired in read mode");
        m_latch--;
    }

    /**
     * Acquire the latch in write mode, fire an Abort exception if the latch is invalid (the associated node has been deleted)
     */
    void lock_write(){
        int64_t current_value { 0 };
        while(!m_latch.compare_exchange_weak(/* by ref, out */ current_value, /* xclusive mode */ -1,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed)){
            if(current_value == -2) throw Abort {}; // this latch has been invalidated and the node deleted

            current_value = 0; // try again
        }
    }

    /**
     * Releases a latch previously acquired in write mode
     */
    void unlock_write(){
        assert(m_latch == -1 && "The latch should have been acquired previously in write mode");
        m_latch = 0;
    }

    /**
     * Invalidates the given latch
     */
    void invalidate(){
        m_latch = -2;
    }

    /**
     * Get the current value of the latch (for debugging purposes)
     */
    int64_t value() const {
        return m_latch;
    }
};

/**
 * Interface to acquire a latch in Read-Only manner.
 */
class ReadLatch {
    ReadLatch(const ReadLatch& latch) = delete;
    ReadLatch& operator=(const ReadLatch& latch) = delete;

    Latch* m_latch; // the latch owned. When nullptr => already released
public:
    /**
     * Init the instance and acquire the given latch in read mode
     */
    ReadLatch(Latch& latch) : m_latch(nullptr) {
        latch.lock_read();
        m_latch = &latch;
    }

    /**
     * Transfer the ownership of the associated latch
     */
    ReadLatch& operator=(ReadLatch&& old){
        release();
        m_latch = old.m_latch;
        old.m_latch = nullptr;
        return *this;
    }

    /**
     * Release the last acquired latch
     */
    ~ReadLatch(){ release(); }

    /**
     * Lock coupling: acquire the new latch in read mode, release the old latch
     */
    void traverse(Latch& latch){
        // acquire the new latch in read mode
        latch.lock_read();
        // release the old latch
        m_latch->unlock_read();

        // save the new latch
        m_latch = &latch;
    }

    /**
     * Release the current latch
     */
    void release(){
        if(m_latch != nullptr){ // the ctor may have fired an exception when acquired the latch
            m_latch->unlock_read();
            m_latch = nullptr;
        }
    }

};

/**
 * Interface to acquire a latch in Write mode
 */
class WriteLatch {
    Latch* m_latch; // the latch held, when nullptr => already released
    WriteLatch(const WriteLatch& latch) = delete;
    WriteLatch& operator=(const WriteLatch& latch) = delete;

public:
    /**
     * Init the instance and acquire the given latch in write mode
     */
    WriteLatch(Latch& latch) : m_latch(nullptr) {
        latch.lock_write();
        m_latch = &latch;
    }

    /**
     * Transfer the ownership of the latch
     */
    WriteLatch& operator=(WriteLatch&& old){
        release();
        m_latch = old.m_latch;
        old.m_latch = nullptr;
        return *this;
    }

    /**
     * Release the acquired latch
     */
    ~WriteLatch(){ release(); }

    /**
     * Release the acquired latch
     */
    void release(){
        if(m_latch == nullptr) return;
        m_latch->unlock_write();
        m_latch = nullptr;
    }

    /**
     * Invalidate the acquired latch
     */
    void invalidate(){
        if(m_latch == nullptr) throw std::runtime_error("Latch already released");
        m_latch->invalidate();
        m_latch = nullptr;
    }
};

} // data_structures::abtree::parallel
