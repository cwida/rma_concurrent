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

#include <bitset>
#include <cassert>
#include <cinttypes>
#include <iostream>
#include <mutex>
#include <utility> // std::swap
#include <vector>

#include "common/spin_lock.hpp"
#include "wakelist.hpp"

namespace data_structures::rma::common { class Bitset; } // forward decl.

namespace data_structures::rma::batch_processing {

// Forward declarations
class ClientContext;
class ClientContextQueue;
class PackedMemoryArray;
class RebalancingMaster;
class ThreadContext;
class ThreadContextList;

/**
 * Base Thread Context contain an epoch. It used by both the client threads that perform the single operations on the PMA
 * and the Timer Manager service to delay a rebalance
 */
class ThreadContext {
    uint64_t m_timestamp; // the current timestamp (or epoch) for the current thread. Only utilised for the purposes of the Garbage Collector

public:
    /**
     * Empty context
     */
    ThreadContext();

    /**
     * Destructor
     */
    ~ThreadContext();

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

};

// For debugging purposes
::std::ostream& operator<<(::std::ostream& out, const ThreadContext& context);
::std::ostream& operator<<(::std::ostream& out, const ThreadContext* context);

class ClientContext : public ThreadContext {
friend class ThreadContextList;
private:
//    friend class PackedMemoryArray;
//    friend class RebalancingMaster;
    friend ::std::ostream& operator<<(::std::ostream& out, const ClientContext& context);

    static thread_local int m_thread_id; // the ID of the current thread
    bool m_hosted; // whether a thread owns this context
    ClientContextQueue* m_local; // local queue, private to this thread
    ClientContextQueue* m_spare; // spare queue, can be shared into a gate from time to time

public:

    WakeList m_wakelist; // cached list of threads to wake up
    using bitset_t = common::Bitset;
    bitset_t* m_bitset = nullptr; // bitset to keep track of which segments to rebalance in the writer loop

public:
    ClientContext();

    ~ClientContext();

    void enter();

    void exit();

    bool busy() const;

    /**
     * Register the current thread
     */
    static void register_client_thread(int thread_id);

    /**
     * Unregister the given thread
     */
    static void unregister_client_thread();

    /**
     * Retrieve the current thread id
     */
    static int thread_id();

    /**
     * Retrieve the local queue
     */
    ClientContextQueue* queue_local() noexcept { return m_local; }
    const ClientContextQueue* queue_local() const noexcept { return m_local; }

    /**
     * Retrieve the spare/global queue
     */
    ClientContextQueue* queue_spare() noexcept{ return m_spare; }
    const ClientContextQueue* queue_spare() const noexcept { return m_spare; }

    /**
     * Create a new spare queue
     */
    void queue_new();

    /**
     * Swap the local and the spare queues
     */
    void queue_swap() noexcept { std::swap(m_local, m_spare); }

    /**
     * Wake up the workers in the wakelist
     */
    void process_wakelist() noexcept { m_wakelist(); }
};

// For debugging purposes
::std::ostream& operator<<(::std::ostream& out, const ThreadContext& context);
::std::ostream& operator<<(::std::ostream& out, const ThreadContext* context);


/**
 * A pair of queues for asynchronous insertions and deletions
 */
class ClientContextQueue {
public:
    using insertion_t = std::pair<int64_t, int64_t>; // key, value

private:
    std::vector<insertion_t> m_insertions;
    std::vector<int64_t> m_deletions;

public:
    /**
     * Constructor
     */
    ClientContextQueue() { }

    /**
     * Enqueue an insertion
     */
    void enqueue_insertion(int64_t key, int64_t value){ m_insertions.emplace_back(key, value); }

    /**
     * Enqueue a deletion
     */
    void enqueue_deletion(int64_t key){ m_deletions.push_back(key); }


    /**
     * The queue for the insertions
     */
    std::vector<insertion_t>& insertions(){ return m_insertions; }
    const std::vector<insertion_t>& insertions() const{ return m_insertions; }

    /**
     * The queue for the deletions
     */
    std::vector<int64_t>& deletions(){ return m_deletions; }
    const std::vector<int64_t>& deletions() const{ return m_deletions; }

    /**
     * Check whether both queues are empty
     */
    bool empty() const { return m_insertions.empty() && m_deletions.empty(); }

    /**
     * Load the insertions/deletions from another queue
     */
    void merge(ClientContextQueue* queue){
        assert(queue != nullptr);
        if(!queue->insertions().empty()){
            for(auto p : queue->insertions()){
                m_insertions.push_back(p);
            }
            queue->insertions().clear();
        }
        if(!queue->deletions().empty()){
            for(auto d : queue->deletions()){
                m_deletions.push_back(d);
            }
            queue->deletions().clear();
        }
    }
};

// For debugging purposes
::std::ostream& operator<<(::std::ostream& out, const ClientContextQueue& queue);
::std::ostream& operator<<(::std::ostream& out, const ClientContextQueue* queue);


/**
 * A safe enough list of thread contexts
 */
class ThreadContextList {
    constexpr static uint64_t m_context_clients_capacity = 128;
    uint64_t m_context_clients_size = 0;
    ClientContext m_context_clients[m_context_clients_capacity];
    ThreadContext m_context_timer_manager; // the context of the timer manager
    mutable std::mutex m_mutex;

public:
    ThreadContextList();
    ~ThreadContextList();

    void resize(uint64_t size);

    // Current size
    uint64_t size() const { return m_context_clients_size; }

    ClientContext* operator[](uint64_t index) const;

    // The context attached to the timer manager
    ThreadContext* timer_manager() const;

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


/**
 * Implementation details
 */
inline void ClientContext::queue_new() { m_spare = new ClientContextQueue(); }

} // namespace
