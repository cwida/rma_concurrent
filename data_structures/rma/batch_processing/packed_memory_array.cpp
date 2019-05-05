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

#include "packed_memory_array.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <mutex>
#include <thread>

#include "common/miscellaneous.hpp"
#include "rma/common/bitset.hpp"
#include "rma/common/buffered_rewired_memory.hpp"
#include "rma/common/static_index.hpp"
#include "garbage_collector.hpp"
#include "gate.hpp"
#include "iterator.hpp"
#include "rebalancing_master.hpp"
#include "thread_context.hpp"
#include "timer_manager.hpp"

using namespace common;
using namespace data_structures::rma::common;
using namespace std;

namespace data_structures::rma::batch_processing {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
mutex _debug_mutex;
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { lock_guard<mutex> lock(_debug_mutex); std::cout << "[PackedMemoryArray::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

PackedMemoryArray::PackedMemoryArray(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent, size_t num_worker_threads, size_t segments_per_lock, chrono::milliseconds delay_rebalance) :
        m_storage(pma_segment_size, pages_per_extent),
        m_index(new StaticIndex(btree_block_size)),
        m_locks(Gate::allocate(1, segments_per_lock)),
        m_density_bounds1(0, 0.75, 0.75, 1), /* there is rationale for these hardwired thresholds */
        m_rebalancer(new RebalancingMaster{ this, num_worker_threads } ),
        m_garbage_collector( new GarbageCollector(this) ),
        m_timer_manager( new TimerManager(this) ),
        m_segments_per_lock(segments_per_lock),
        m_delayed_rebalance(delay_rebalance){
    if(!is_power_of_2(segments_per_lock)) throw std::invalid_argument("[PackedMemoryArray::ctor] Invalid value for the `segments_per_lock', it is not a power of 2");
    if(segments_per_lock < 2) throw std::invalid_argument("[PackedMemoryArray::ctor] Invalid value for the `segments_per_lock', it must be >= 2");
    if(segments_per_lock > 256) throw std::invalid_argument("[PackedMemoryArray::ctor] This implementation does not support more than 256 segments per lock/gate, due to the implmentation limit of std::bitset<256> in ClientContext");
    if(m_storage.get_segments_per_extent() % segments_per_lock != 0) throw std::invalid_argument("[PackedMemoryArray::ctor] The parameter `segments_per_extent' must be a multiple of `segments_per_lock'");


    // set the init time for the gates
    m_locks.get_unsafe()->m_time_last_rebal = chrono::steady_clock::now();

    // start the garbage collector
    GC()->start();

    // start the rebalancer
    m_rebalancer->start();

    // start the timer manager
    m_timer_manager->start();

    // by default allow one thread to run
    m_thread_contexts.resize(1);

    m_index.get_unsafe()->set_separator_key(0, numeric_limits<int64_t>::min());
}


PackedMemoryArray::~PackedMemoryArray() {
    // stop the timer manager (impl. called by the dtor)
    delete m_timer_manager; m_timer_manager = nullptr;

    // stop the rebalancer
    delete m_rebalancer; m_rebalancer = nullptr;

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;

    // remove the index
    delete m_index.get_unsafe(); m_index.set(nullptr);

    // remove all non owned global queues
    for(size_t i = 0; i < get_number_locks(); i++){
        Gate& gate = m_locks.get_unsafe()[i];
        delete gate.m_async_queue; gate.m_async_queue = nullptr;
    }

    // remove the locks
    Gate::deallocate(m_locks.get_unsafe()); m_locks.set(nullptr);
}


/*****************************************************************************
 *                                                                           *
 *   ThreadContext                                                           *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::set_max_number_workers(size_t num_workers){
    m_thread_contexts.resize(num_workers);
}

void PackedMemoryArray::register_thread(uint32_t client_id){
    assert(client_id < m_thread_contexts.size());
    if(client_id >= m_thread_contexts.size()){
        throw std::invalid_argument("Invalid thread id: space not previously obtained in the list of threads");
    }

    ClientContext::register_client_thread(client_id);
    m_thread_contexts[client_id]->enter();
    if(m_thread_contexts[client_id]->m_bitset == nullptr){
        m_thread_contexts[client_id]->m_bitset = new Bitset( get_segments_per_lock() );
    }

#if !defined(NDEBUG)
    set_thread_name(string("PMA Worker #") + to_string(client_id));
#endif
}

void PackedMemoryArray::unregister_thread(){
    get_context()->exit();
    ClientContext::unregister_client_thread();
}

ClientContext* PackedMemoryArray::get_context() const {
    return m_thread_contexts[ ClientContext::thread_id() ];
}

GarbageCollector* PackedMemoryArray::GC() const noexcept{
    return m_garbage_collector;
}

/*****************************************************************************
 *                                                                           *
 *   Concurrency for writers                                                 *
 *                                                                           *
 *****************************************************************************/

void PackedMemoryArray::writer_loop(int64_t key){
    ClientContext* __restrict context = get_context();

    assert(context != nullptr);
    assert(!context->queue_local()->empty() && "There are no updates scheduled");
    assert(context->epoch() < numeric_limits<uint64_t>::max() && "Internal epoch not set");

    Gate* gate = writer_on_entry(key);
    if(gate == nullptr) return; // asynchronous update

    do {
        assert(context->m_bitset->none() && "The auxiliary bitset should be empty, that is there should be not segments to rebalance");

        bool do_global_rebalance = false;
        int64_t num_deletions = 0;
        int64_t num_insertions = 0;

        // 1) perform all deletions from the local queue
        std::vector<int64_t>& deletions = context->queue_local()->deletions();
        if(deletions.size() > 0){
            std::sort(begin(deletions), end(deletions));
            for(auto& key : deletions){
                int64_t value = -1;
                int64_t absseg2rebal = do_remove(gate, key, /* ignored */ &value);

                // keep track of which segments to rebalance at the end
                if(absseg2rebal != -1){
                    int64_t relseg2rebal = absseg2rebal - gate->window_start();
                    assert(relseg2rebal >= 0 && relseg2rebal < get_segments_per_lock());
                    context->m_bitset->set(relseg2rebal);
                }

                num_deletions += (value != -1);
            }
            deletions.clear();
        }


        // 2) perform all insertions from the local queue
        bool inserted = true;
        auto& insertions = context->queue_local()->insertions();
        ClientContext::bitset_t* segments2rebalance = (num_deletions > 0 && context->m_bitset->any()) ? context->m_bitset : nullptr;
        if(insertions.size() > 0){
            do {
                auto& pair = insertions.back();
                inserted = do_insert(gate, pair.first, pair.second, segments2rebalance);
                COUT_DEBUG("inserted = " << inserted);
                if(inserted) {
                    num_insertions++;
                    insertions.pop_back();
                }
            } while(inserted && !insertions.empty());
        }
        do_global_rebalance |= !inserted;

        // 3) are there still any standing rebalances due to deletions?
        if(segments2rebalance != nullptr){
            int64_t relative_segment_id = 0;
            int64_t num_segments = get_segments_per_lock();
            while(!do_global_rebalance && segments2rebalance->any() && relative_segment_id < num_segments){
                if(segments2rebalance->test(relative_segment_id)){
                    do_global_rebalance |= !rebalance_local(gate->window_start() + relative_segment_id, /* insertion */ nullptr, segments2rebalance);
                }
                relative_segment_id++;
            }
        }
        context->m_bitset->reset(); // clear the bitset

        writer_on_exit(gate, /* cardinality change */ num_insertions - num_deletions, /* rebalance ? */ do_global_rebalance);
    } while(!context->queue_local()->empty());
}


Gate* PackedMemoryArray::writer_on_entry(int64_t key){
    ClientContext* __restrict context = get_context();
    assert(context != nullptr);
    assert(context->queue_spare());
    assert(context->queue_spare()->empty() && "The global queue should be empty");
    assert(context->epoch() < numeric_limits<uint64_t>::max() && "Context not registered");

    Gate* result = nullptr;
    bool done = false;
    bool context_switch = false;

    do {
        try {
            StaticIndex* index = m_index.get(*context); // snapshot, current index
            auto gate_id = index->find(key);
            Gate* gates = m_locks.get(*context);

            do {
                // enter in the private section
                auto& gate = gates[gate_id];
                unique_lock<Gate> lock(gate);

                // is this the right gate ?
                if(check_fence_keys(gate, /* in/out */ gate_id, key)){

                    switch(gate.m_state){
                    case Gate::State::FREE:
                    case Gate::State::READ:
                    case Gate::State::WRITE:
                        if(gate.m_async_queue != nullptr){ // there is an asynchronous queue installed
                            assert(gate.m_async_queue != context->queue_local() && "Inserting in my own private queue!");
                            assert(gate.m_async_queue != context->queue_spare() && "Inserting in my own spare queue!");
                            gate.m_async_queue->merge(context->queue_local());
                            done = true;
                        } else {
                            gate.m_async_queue = context->queue_spare();
                            if(gate.m_state == Gate::State::FREE){ // man this gate
                                assert(gate.m_num_active_threads == 0 && "There should not be any thread active on a free gate");
                                gate.m_state = Gate::State::WRITE;
                                gate.m_num_active_threads = 1;
                                result = &gate;
                                done = true;
                            } else {
                                gate.m_async_queue->merge(context->queue_local()); // move the items from the local to the global queue
                                writer_wait(gate, lock);
                                context_switch = true;
                            }
                        } break;
                    case Gate::State::TIMEOUT:
                    case Gate::State::REBAL:
                        writer_wait(gate, lock);
                    }

                    // we installed the global queue, check whether it's still in
                    while(context_switch){
                        lock.lock(); // relock the gate
                        if(gate.m_async_queue != context->queue_spare()){
                            // kaboom, a rebalance occurred in the meanwhile & the items in the local queues have been loaded by the rebalancer
                            context->queue_new();
                            context_switch = false; // exit the context_switch loop
                            done = true; // exit the Abort loop
                        } else {
                            switch(gate.m_state){
                            case Gate::State::FREE: // finally, man this gate
                                assert(gate.m_num_active_threads == 0 && "There should not be any thread active on a free gate");
                                gate.m_state = Gate::State::WRITE;
                                gate.m_num_active_threads = 1;
                                result = &gate;

                                assert(!context->queue_spare()->empty() && "The global queue should at least contain the first synchronous update");
                                assert(context->queue_local()->empty() && "The private queue should be empty as the thread was in the wait list");
                                context->queue_swap();
                                gate.m_async_queue = context->queue_spare(); // the previous private queue is now the public queue

                                // we're done
                                context_switch = false;
                                done = true;
                                break;
                            default:
                                writer_wait(gate, lock);
                            }
                        }
                    }
                } // check_fence_keys
            } while(!done);
        } catch( Abort ){
            context->hello(); // update the internal timestamp
            // ... and try again
        }
    } while(!done);

    return result;
}

bool PackedMemoryArray::writer_on_exit(Gate* gate, int64_t cardinality_change, bool do_rebalance){
    assert(gate != nullptr);
    COUT_DEBUG("gate: " << gate->lock_id() << ", cardinality_change: " << cardinality_change << ", do_rebalance: " << do_rebalance);

    bool hold_this_gate = false;
    ClientContext* context = get_context();

    bool send_rebalance_request = false; // shall we invoke the rebalancer at the end?
    bool client_exit = false; // is this a rebalancing (false) or a client_exit (true) request ?

    gate->lock();
    assert(static_cast<int64_t>(gate->m_cardinality) + cardinality_change >= 0);
    gate->m_cardinality += cardinality_change;
    debug_validate_cardinality_gate(gate, cardinality_change);

    assert(gate->m_state == Gate::State::WRITE || gate->m_state == Gate::State::TIMEOUT || gate->m_state == Gate::State::REBAL);
    assert(gate->m_async_queue == context->queue_spare());
    assert(gate->m_num_active_threads == 1 && "The writer should have previously updated this member");

    switch(gate->m_state){
    case Gate::State::WRITE:
        if(do_rebalance) {
            gate->m_async_queue->merge(context->queue_local());
            gate->m_num_active_threads = 0;
            context->queue_new();

            auto now = chrono::steady_clock::now();
            if(now < gate->m_time_last_rebal + m_delayed_rebalance){ // delay this rebalance
                gate->m_state = Gate::State::FREE;
                auto delay_usecs = chrono::duration_cast<chrono::microseconds>(gate->m_time_last_rebal + m_delayed_rebalance - now);
                m_timer_manager->delay_rebalance(gate->lock_id(), gate->m_time_last_rebal, delay_usecs);
                gate->wake_next(context);
            } else { // rebalance immediately
                gate->m_state = Gate::State::REBAL;
                send_rebalance_request = true;
                writer_do_pending_deletions(gate);
            }
        } else if (gate->m_async_queue->empty()){ // we're done, there are no more items to asynchronously update
            gate->m_async_queue = nullptr;
            gate->m_num_active_threads = 0;
            gate->m_state = Gate::State::FREE;
            gate->wake_next(context);
        } else if (gate->m_queue.size() > 0){ // context switch, other clients are waiting to access this queue
            gate->m_num_active_threads = 0;
            gate->m_state = Gate::State::FREE;
            gate->wake_next(context);

            std::promise<void> producer;
            std::future<void> consumer = producer.get_future();
            gate->m_queue.append({ Gate::State::WRITE, &producer } );
            gate->unlock();
            context->process_wakelist();
            consumer.wait();

            bool context_switch = true;

            do {
                gate->lock();
                if(gate->m_async_queue != context->queue_spare()){ // the Rebalancer must have touched this gate, taking already care to merge the items in the global queue
                    context->queue_new();
                    context_switch = false; // done
                } else if (gate->m_state != Gate::State::FREE){ // someone else stole our spot!
                    writer_wait(*gate); // context switch
                } else {
                    assert(gate->m_state == Gate::State::FREE);
                    assert(gate->m_async_queue == context->queue_spare());
                    assert(!context->queue_spare()->empty() && "Who cleared my global queue?");

                    context->queue_swap();
                    gate->m_async_queue = context->queue_spare(); // the previous private queue is now the public queue

                    gate->m_num_active_threads = 1;
                    gate->m_state = Gate::State::WRITE;

                    hold_this_gate = true; // we still have to man this gate
                    context_switch = false; // done
                }
            } while(context_switch);
        } else { // there are no other clients waiting for && the global queue is not empty
            context->queue_swap();
            gate->m_async_queue = context->queue_spare(); // the previous private queue is now the public queue
            hold_this_gate = true;
        }
        break;
    case Gate::State::TIMEOUT:
    case Gate::State::REBAL:
        send_rebalance_request = true;
        client_exit = (gate->m_state == Gate::State::REBAL);

        gate->m_state = Gate::State::REBAL;
        gate->m_num_active_threads = 0;

        gate->m_async_queue->merge(context->queue_local());
        writer_do_pending_deletions(gate);
        context->queue_new();

        break;
    default:
        assert(0 && "Invalid state");
    }


    COUT_DEBUG("send_rebalance_request: " << send_rebalance_request << ", client_exit: " << client_exit << ",  async queue: " << gate->m_async_queue);

    gate->unlock();

    if(send_rebalance_request){
        rebalance_global(gate->lock_id(), client_exit);
    } else {
        context->process_wakelist();
    }

    assert(hold_this_gate == !context->queue_local()->empty() && "If we are holding this gate, then there are still updates to perform");
    return hold_this_gate;
}

void PackedMemoryArray::writer_do_pending_deletions(Gate* gate){
    assert(gate != nullptr);
    assert(gate->m_locked == true && "This method can be invoked only while helding the lock for the gate");
    if(gate->m_async_queue == nullptr) return; // there is no queue attached to this gate

    auto& queue = gate->m_async_queue->deletions();
    if(queue.empty()) return;

    int64_t num_deletions = 0;
    std::sort(begin(queue), end(queue));
    for(int64_t key : queue){
        int64_t value = -1;
        do_remove(gate, key, &value);
        num_deletions += (value != -1);
    }

    assert(gate->m_cardinality >= num_deletions);
    gate->m_cardinality -= num_deletions;
}

template<typename Lock>
void PackedMemoryArray::writer_wait(Gate& gate, Lock& lock){
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    gate.m_queue.append({ Gate::State::WRITE, &producer } );
    lock.unlock();
    consumer.wait();
}

/*****************************************************************************
 *                                                                           *
 *   Concurrency for readers                                                 *
 *                                                                           *
 *****************************************************************************/

Gate* PackedMemoryArray::reader_on_entry(int64_t key, int64_t start_gate_id) const {
    ClientContext* context = get_context();
    assert(context != nullptr);
    uint64_t gate_id { 0 };
    if(start_gate_id < 0){
        StaticIndex* index = m_index.get(*context); // snapshot, current index
        gate_id = index->find(key);
    } else {
        gate_id = static_cast<uint64_t>(start_gate_id);
    }

    Gate* result = nullptr;

    bool done = false;
    do {
        Gate* gates = m_locks.get(*context);
        // enter in the private section
        auto& gate = gates[gate_id];
        unique_lock<Gate> lock(gate);
        // is this the right gate ?
        if(check_fence_keys(gate, /* in/out */ gate_id, key)){
            switch(gate.m_state){
            case Gate::State::FREE:
                assert(gate.m_num_active_threads == 0 && "Precondition not satisfied");
                gate.m_state = Gate::State::READ;
                gate.m_num_active_threads = 1;
                lock.unlock();

                result = gates + gate_id;
                done = true; // done, go on
                break;
            case Gate::State::READ:
                if(gate.m_queue.empty()){ // as above
                    gate.m_num_active_threads++;
                    lock.unlock();
                    result = gates + gate_id;
                    done = true;
                } else {
                    std::promise<void> producer;
                    std::future<void> consumer = producer.get_future();

                    gate.m_queue.append({ Gate::State::READ, &producer } );
                    lock.unlock();

                    consumer.wait();
                }
                break;
            case Gate::State::WRITE:
            case Gate::State::TIMEOUT:
            case Gate::State::REBAL:
                { // add the thread in the queue
                    std::promise<void> producer;
                    std::future<void> consumer = producer.get_future();

                    gate.m_queue.append({ Gate::State::READ, &producer } );
                    lock.unlock();

                    consumer.wait();
                }
            }
        }
    } while(!done);

    return result;
}

void PackedMemoryArray::reader_on_exit(Gate* gate) const {
    bool send_message_to_rebalancer = false; // shall we invoke the rebalancer at the end?
    bool client_exit = false; // is this a global rebalancing or a client exit?
    ClientContext* context = get_context();

    assert(gate != nullptr);
    gate->lock();
    assert(gate->m_num_active_threads > 0 && "This reader should have been registered");
    gate->m_num_active_threads--;
    if(gate->m_num_active_threads == 0){
       switch(gate->m_state){
       case Gate::State::READ: { // as before
           gate->m_state = Gate::State::FREE;
           gate->wake_next(context);
       } break;
       case Gate::State::TIMEOUT:
       case Gate::State::REBAL: {
           send_message_to_rebalancer = true;
           client_exit = (gate->m_state == Gate::State::REBAL);
           gate->m_state = Gate::State::REBAL;

           // optimisation, avoid performing the deletions in the rebalancer
           const_cast<PackedMemoryArray*>(this)->writer_do_pending_deletions(gate);
       } break;
       default:
           assert(0 && "Invalid state");
       }
    }
    gate->unlock();

    if(send_message_to_rebalancer){
        rebalance_global(gate->lock_id(), client_exit);
    } else {
        context->process_wakelist();
    }
}


/*****************************************************************************
 *                                                                           *
 *   Timeout                                                                 *
 *   - Only invoked by the Timer Manager                                     *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::timeout(size_t gate_id, std::chrono::steady_clock::time_point time_last_rebal){
    auto context = m_thread_contexts.timer_manager();
    ScopedState scope{context};

    try {
        bool send_rebalance_request = false; // request a global rebalance ?

        if(gate_id >= get_number_locks()) return; // it may refer a gate_id that doesn't exist anymore due to a downsize

        Gate& gate = m_locks.get(context)[gate_id]; // it can throw an Abort exception

        unique_lock<Gate> lock(gate);

        if(gate.m_time_last_rebal == time_last_rebal){ // check that the gate hasn't been rebalanced in the meanwhile
            switch(gate.m_state){
            case Gate::State::FREE:
                assert(gate.m_num_active_threads == 0 && "Great, the gate is free but there are registered threads being active on it");
                send_rebalance_request = true;
                gate.m_state = Gate::State::REBAL;
                break;
            case Gate::State::READ:
            case Gate::State::WRITE:
                assert(gate.m_num_active_threads > 0 && "There should be some client thread still active on this gate");
                gate.m_state = Gate::State::TIMEOUT; // the last client thread that leaves this gate needs to invoke the global rebalancer
                break;
            case Gate::State::TIMEOUT:
                // we've already requested to rebalance this segment?
                assert(gate.m_num_active_threads > 0 && "There should be some client thread still active on this gate");
                /* nop */
                break;
            case Gate::State::REBAL:
                /* nop */
                break;
            }
        }

        lock.unlock();

        if(send_rebalance_request)
            rebalance_global(gate_id, false);

    } catch(Abort) {
        // if we abort, then it means that the PMA has resized in the meanwhile and the request
        // to rebalance became obsolete -> nop
    }
}


/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/

bool PackedMemoryArray::empty() const noexcept{
    assert(m_cardinality >= 0 && "Negative cardinality ?");
    return m_cardinality == 0;
}

size_t PackedMemoryArray::size() const noexcept {
    assert(m_cardinality >= 0 && "Negative cardinality ?");
    return m_cardinality;
}

const CachedDensityBounds& PackedMemoryArray::get_thresholds() const {
    if(m_primary_densities){
        return m_density_bounds1;
    } else {
        return m_density_bounds0;
    }
}

std::pair<double, double> PackedMemoryArray::get_thresholds(int height) const {
    assert(height >= 1 && height <= m_storage.hyperheight());
    return get_thresholds().thresholds(height);
}

void PackedMemoryArray::set_thresholds(int height_calibrator_tree){
    assert(height_calibrator_tree >= 1);
    if(m_primary_densities){
        m_density_bounds1.thresholds(height_calibrator_tree, height_calibrator_tree);
    } else {
        m_density_bounds0.thresholds(height_calibrator_tree, height_calibrator_tree);
    }
}

void PackedMemoryArray::set_thresholds(const RebalancePlan& action){
    if(action.m_operation == RebalanceOperation::RESIZE || action.m_operation == RebalanceOperation::RESIZE_REBALANCE){
        m_primary_densities = action.m_window_length > balanced_thresholds_cutoff();
        set_thresholds(ceil(log2(action.m_window_length)) +1);
    }
}

size_t PackedMemoryArray::balanced_thresholds_cutoff() const {
    return m_knobs.get_thresholds_switch() * m_storage.get_segments_per_extent();
}

CachedMemoryPool& PackedMemoryArray::memory_pool() {
    return m_memory_pool;
}

Knobs& PackedMemoryArray::knobs(){
    return m_knobs;
}

size_t PackedMemoryArray::get_segment_capacity() const noexcept {
    return m_storage.m_segment_capacity;
}

size_t PackedMemoryArray::get_segments_per_lock() const noexcept {
    return m_segments_per_lock;
}

size_t PackedMemoryArray::get_number_locks() const noexcept {
    return max<size_t>(1ull, m_storage.m_number_segments / get_segments_per_lock());
}

void PackedMemoryArray::build() {
    on_complete();
}

size_t PackedMemoryArray::memory_footprint() const {
    size_t space_index = m_index.get_unsafe()->memory_footprint();
    size_t space_locks = get_segments_per_lock() * (sizeof(Gate) + /* separator keys */ (m_index.get_unsafe()->node_size() -1) * sizeof(int64_t));
    size_t space_storage = m_storage.memory_footprint();

    return sizeof(decltype(*this)) + space_index + space_locks + space_storage;
}

void PackedMemoryArray::rebalance_global(uint64_t gate_id, bool client_exit) const{
    if(client_exit){
        m_rebalancer->exit(gate_id);
    } else {
        m_rebalancer->rebalance(gate_id);
    }
}

bool PackedMemoryArray::check_fence_keys(Gate& gate, uint64_t& gate_id, int64_t key) const {
    assert(gate.m_locked && "To invoke this method the gate's lock must be acquired first");

    auto d = gate.check_fence_keys(key);
    switch(d){
    case Gate::Direction::LEFT:
        assert(gate_id > 0 && "This is already the first gate");
        gate_id--;
        break;
    case Gate::Direction::RIGHT:
        gate_id++;
        break;
    case Gate::Direction::INVALID:
        throw Abort{}; // start from scratch
        break;
    case Gate::Direction::GO_AHEAD:
        return true;
    }

    return false;
}

/*****************************************************************************
 *                                                                           *
 *   Index                                                                   *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::set_separator_key(uint64_t segment_id, int64_t key) {
    auto lock_id = segment_id / get_segments_per_lock();
    auto lock_offset = segment_id % get_segments_per_lock();
    COUT_DEBUG("segment_id: " << segment_id << ", key: " << key << ", gate_id: " << lock_id << ", gate_offset: " << lock_offset);

    if(lock_offset == 0){ // ignore, it would need to update the fence keys
        /* nop */
    } else {
        // Assume the lock to this gate has already been acquired
        Gate* gate = m_locks.get_unsafe() + lock_id;
        gate->set_separator_key(segment_id, key);
    }
}

int64_t PackedMemoryArray::max_separator_key(size_t segment_id){
    auto lock_id = segment_id / get_segments_per_lock();
    auto lock_offset = segment_id % get_segments_per_lock();

    if(lock_offset == get_segments_per_lock() -1){
        return m_locks.get_unsafe()[lock_id].m_fence_high_key;
    } else {
        return m_locks.get_unsafe()[lock_id].get_separator_key(segment_id +1);
    }
}


/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::insert(int64_t key, int64_t value){
    ClientContext* context = get_context();
    ScopedState scope { context };

    // At the start all queues should be empty
    assert(context->queue_local()->empty());
    assert(context->queue_spare()->empty());

    // Add the element to process in the local queue
    context->queue_local()->insertions().emplace_back(key, value);

    // Process the insertion from the local queue
    writer_loop(key);

    // At the end all queues should be empty
    assert(context->queue_local()->empty());
    assert(context->queue_spare()->empty());
}

bool PackedMemoryArray::do_insert(Gate* gate, int64_t key, int64_t value, ClientContext::bitset_t* bitset){
    assert(gate != nullptr && "Null pointer");
    COUT_DEBUG("Gate: " << gate->lock_id() << ", key: " << key << ", value: " << value);

    if(UNLIKELY( empty() )){
        insert_empty(key, value);
        return true;
    } else {
        size_t segment = gate->find(key);
        return insert_common(segment, key, value, bitset);
    }
}

void PackedMemoryArray::insert_empty(int64_t key, int64_t value){
    assert(empty());
    COUT_DEBUG("key: " << key << ", value: " << value);

    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_cardinality = 1;
}

bool PackedMemoryArray::insert_common(size_t segment_id, int64_t key, int64_t value, ClientContext::bitset_t* bitset){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_number_segments && "Overflow: attempting to access an invalid segment in the PMA");
//    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        InsertionT elt_to_insert { key, value, (int64_t) segment_id };
        return rebalance_local(segment_id, &elt_to_insert, bitset);
    } else { // standard case: just insert the element in the segment
        bool minimum_updated = storage_insert_unsafe(segment_id, key, value);

        // have we just updated the minimum ?
        if (minimum_updated) set_separator_key(segment_id, key);

        return true; // report that the element has been inserted
    }
}

bool PackedMemoryArray::storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value){
    assert(m_storage.m_segment_sizes[segment_id] < m_storage.m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_storage.m_segment_sizes[segment_id];
    assert(sz < m_storage.m_segment_capacity && "Segment overfilled");

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), insert at the end of the segment
        size_t stop = m_storage.m_segment_capacity -1;
        size_t start = m_storage.m_segment_capacity - sz -1;
        size_t i = start;

        while(i < stop && keys[i+1] < key){
            keys[i] = keys[i+1];
            i++;
        }

//        COUT_DEBUG("(even) segment_id: " << segment_id << ", start: " << start << ", stop: " << stop << ", key: " << key << ", value: " << value << ", pos   ition: " << i);
        keys[i] = key;

        for(size_t j = start; j < i; j++){
            values[j] = values[j+1];
        }
        values[i] = value;

        minimum = (i == start);
    } else { // for odd segment ids (1, 3, ...), insert at the front of the segment
        size_t i = sz;
        while(i > 0 && keys[i-1] > key){
            keys[i] = keys[i-1];
            i--;
        }

//        COUT_DEBUG("(odd) segment_id: " << segment_id << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = sz; j > i; j--){
            values[j] = values[j-1];
        }
        values[i] = value;

        minimum = (i == 0);
    }

    // update the cardinality
    m_storage.m_segment_sizes[segment_id]++;
    m_cardinality++;

    return minimum;
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t PackedMemoryArray::remove(int64_t key){
    ClientContext* context = get_context();
    ScopedState scope { context };

    // At the start all queues should be empty
    assert(context->queue_local()->empty());
    assert(context->queue_spare()->empty());

    // Add the element to process in the local queue
    context->queue_local()->deletions().push_back(key);

    // Process the deletion from the local queue
    writer_loop(key);

    // At the end all queues should be empty
    assert(context->queue_local()->empty());
    assert(context->queue_spare()->empty());

    // in this implementation, removes are asynchronous
    return -1;
}

int64_t PackedMemoryArray::do_remove(Gate* gate, int64_t key, int64_t* out_value){
    COUT_DEBUG("key: " << key);

    assert(gate != nullptr && "Null pointer");

    *out_value = -1;
    if(empty()) return false;
//    bool request_global_rebalance = false;

    int64_t segment_id = gate->find(key);
//    COUT_DEBUG("Gate: " << gate->gate_id() << ", segment: " << segment_id << ", key: " << key);
    size_t sz = m_storage.m_segment_sizes[segment_id];
    if(sz == 0) return segment_id; // this segment is empty, and anyway should be rebalanced
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;

    int64_t value = -1;
    int64_t predecessor = numeric_limits<int64_t>::min(); // to forward to the detector/predictor
    int64_t successor = numeric_limits<int64_t>::max(); // to forward to the detector/predictor

    if (segment_id % 2 == 0) { // even
        size_t imin = m_storage.m_segment_capacity - sz, i;
        for(i = imin; i < m_storage.m_segment_capacity; i++){ if(keys[i] == key) break; }
        if(i < m_storage.m_segment_capacity){ // found ?
            // to update the predictor/detector
            if(i > imin) predecessor = keys[i-1];
            if(i < m_storage.m_segment_capacity -1) successor = keys[i+1];

            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j > imin; j--){
                keys[j] = keys[j -1];
                values[j] = values[j-1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_cardinality--;

            if(i == imin){ // update the pivot
                if(m_cardinality == 0){ // global minimum
                    set_separator_key(0, numeric_limits<int64_t>::min());
                } else {
                    set_separator_key(segment_id, keys[imin +1]);
                }
            }
        } // end if (found)
    } else { // odd
        // find the key in the segment
        size_t i = 0;
        for( ; i < sz; i++){ if(keys[i] == key) break; }
        if(i < sz){ // found?
            // to update the predictor/detector
            if(i > 0) predecessor = keys[i-1];
            if(i < sz -1) successor = keys[i+1];

            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j < sz - 1; j++){
                keys[j] = keys[j+1];
                values[j] = values[j+1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_cardinality--;

            // update the minimum
            if(i == 0 && sz > 0){ // sz > 0 => otherwise we are going to rebalance this segment anyway
                set_separator_key(segment_id, keys[0]);
            }
        } // end if (found)
    } // end if (odd segment)

    // shall we rebalance ?
    int64_t rebalance_segment = -1;
    if(value != -1){

//        if(m_storage.m_number_segments >= 2 * balanced_thresholds_cutoff() && static_cast<double>(m_cardinality) < 0.5 * m_storage.capacity()){
//            assert(m_storage.get_number_extents() > 1);
//            request_global_rebalance = true;
//        } else
        if(m_storage.m_number_segments > 1) {
            const size_t minimum_size = max<size_t>(get_thresholds(1).first * m_storage.m_segment_capacity, 1); // at least one element per segment
            if(sz < minimum_size){ rebalance_segment = segment_id; }
        }
    }

//#if defined(DEBUG)
//    dump();
//#endif

    *out_value = value;
    return rebalance_segment;
}

/*****************************************************************************
 *                                                                           *
 *   Local rebalance                                                         *
 *                                                                           *
 *****************************************************************************/

bool PackedMemoryArray::rebalance_local(size_t segment_id, InsertionT* insertion /* possibly nullptr */, ClientContext::bitset_t* bitset){
    const bool is_insert = (insertion != nullptr);
    COUT_DEBUG("segment_id: " << segment_id << ", is_insert: " << is_insert);

    int64_t window_start {0}, window_length {0}, cardinality_after {0};
    bool do_resize { false };
    bool is_local_rebalance = rebalance_find_window(segment_id, is_insert, &window_start, &window_length, &cardinality_after, &do_resize);
    if(!is_local_rebalance) return false;

    if(bitset != nullptr){ // signal the client context that we have locally rebalanced these segments
        bitset->reset(window_start % get_segments_per_lock(), window_length);
    }

    int64_t cardinality_before = cardinality_after + (is_insert ? -1 : 0);
    auto plan = rebalance_plan(window_start, window_length, cardinality_before, cardinality_after, do_resize);
    set_thresholds(plan); // update the thresholds in the calibrator tree

    do_rebalance_local(plan, insertion);

    return true;
}

bool PackedMemoryArray::rebalance_find_window(size_t segment_id, bool is_insertion, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const {
    assert(out_window_start != nullptr && out_window_length != nullptr && out_cardinality_after != nullptr && out_resize != nullptr);
    assert(segment_id < m_storage.m_number_segments && "Invalid segment");

    int64_t window_length = 1;
    int64_t window_id = segment_id;
    int64_t window_start = segment_id /* incl */, window_end = segment_id +1 /* excl */;
    int64_t cardinality_after = is_insertion ? m_storage.m_segment_capacity +1 : m_storage.m_segment_sizes[segment_id];
    int height = 1;
    int spe_height = log2(get_segments_per_lock()) +1;
    int cb_height = m_storage.height();
    int max_height_local_rebalance = std::min(spe_height, cb_height);

    // these inits are only valid for the edge case that the calibrator tree has height 1, i.e. the data structure contains only one segment
    double rho = 0.0, theta = 1.0, density = static_cast<double>(cardinality_after)/m_storage.m_segment_capacity;

    // determine the window to rebalance
    if(m_storage.height() > 1){
        int64_t index_left = segment_id -1;
        int64_t index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;

            // re-align the calibrator tree
            if(window_end > m_storage.m_number_segments){
                int offset = window_end - m_storage.m_number_segments;
                window_start -= offset;
                window_end -= offset;
            }

            auto density_bounds = get_thresholds(height);
            rho = density_bounds.first;
            theta = density_bounds.second;

            // find the number of elements in the interval
            while(index_left >= window_start){
                cardinality_after += m_storage.m_segment_sizes[index_left];
                index_left--;
            }
            while(index_right < window_end){
                cardinality_after += m_storage.m_segment_sizes[index_right];
                index_right++;
            }

            density = ((double) cardinality_after) / (window_length * m_storage.m_segment_capacity);

        } while( ((is_insertion && density > theta) || (!is_insertion && density < rho))
                && height < max_height_local_rebalance);
    }

    COUT_DEBUG("rho: " << rho << ", density: " << density << ", theta: " << theta << ", height: " << height << ", calibrator tree: " << m_storage.height() << ", segments_per_lock: " << get_segments_per_lock() << ", spe_height: " << spe_height << ", is_insert: " << is_insertion);
    if((is_insertion && density <= theta) || (!is_insertion && density >= rho)){ // rebalance
        *out_cardinality_after = cardinality_after;
        *out_window_start = window_start;
        *out_window_length = window_length;
        *out_resize = false;
        return true;
    } else if ((is_insertion && cb_height < spe_height) || (!is_insertion && cb_height <= spe_height)) { // resize
        *out_cardinality_after = m_cardinality + (is_insertion ? 1 : 0);
        *out_window_start = 0;
        *out_window_length = m_storage.m_number_segments;
        *out_resize = true;
        return true;
    } else { // global rebalance
        return false;
    }
}

RebalancePlan PackedMemoryArray::rebalance_plan(int64_t window_start, int64_t window_length, int64_t cardinality_before, int64_t cardinality_after, bool resize) const {
    COUT_DEBUG("window_start: " << window_start << ", window_length: " << window_length << ", cardinality_before: " << cardinality_before << ", cardinality_after: " << cardinality_after << ", resize: " << resize);

    RebalancePlan result { };
    result.m_cardinality_before = cardinality_before;
    result.m_cardinality_change = cardinality_after - cardinality_before;
    result.m_window_start = window_start;
    result.m_window_length = window_length;
    result.m_operation = resize ? RebalanceOperation::RESIZE : RebalanceOperation::REBALANCE;

    rebalance_plan(&result);

    return result;
}

void PackedMemoryArray::rebalance_plan(RebalancePlan* plan) const {
    assert(plan != nullptr && "Null pointer");
    if(plan->m_operation == RebalanceOperation::REBALANCE) return; /* nop */

    const size_t density_threshold = balanced_thresholds_cutoff();

    // this is a resize
    plan->m_window_start = 0;
    plan->m_window_length = m_storage.m_number_segments; // default

    double density = static_cast<double>(plan->get_cardinality_after()) / (static_cast<int64_t>(m_storage.m_number_segments) * m_storage.m_segment_capacity);

    if(density < get_thresholds().get_lower_threshold_root()){ // downsize
        plan->m_operation = RebalanceOperation::RESIZE; // we don't support RESIZE_REBALANCE in downsizes

        // to compute the ideal number of segments:
        // 1) we want that the elements occupy 75% of the whole array : N = 0.75 C => C = N/0.75
        // 2) the ideal number of segments is C / segment_size = N/0.75 * 1/segment_size
        const size_t ideal_number_of_segments = ceil( static_cast<double>(plan->get_cardinality_after()) / static_cast<double>(m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity));
        COUT_DEBUG("cardinality after: " << plan->get_cardinality_after() << ", ideal number of segments: " << ideal_number_of_segments << ", segment_capacity: " << m_storage.m_segment_capacity << ", current number of segments: " << m_storage.m_number_segments << ", density threshold: " << density_threshold);
        if(ideal_number_of_segments <= density_threshold){
            plan->m_window_length = std::max<int64_t>(hyperceil(ideal_number_of_segments), 1);
            while(plan->m_window_length > 1 && plan->get_cardinality_after() < m_density_bounds0.get_lower_threshold_root() * plan->m_window_length * m_storage.m_segment_capacity){
                plan->m_window_length /= 2;
            }
        } else { // at least one extent
            const size_t segments_per_extent = m_storage.get_segments_per_extent();
            size_t num_extents = floor( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
            assert(num_extents <= m_storage.get_number_extents() && "We are not reducing the array size");
            plan->m_window_length = m_storage.get_segments_per_extent() * num_extents;
        }
        assert(plan->m_window_length * m_storage.m_segment_capacity >= plan->get_cardinality_after() && "There is not enough space to store all the elements");
        assert(plan->m_window_length < m_storage.m_number_segments && "We are not downsizing the array...");
    } else if (density > get_thresholds().get_upper_threshold_root()){ // upsize
        size_t ideal_number_of_segments = max<size_t>(ceil( static_cast<double>(plan->get_cardinality_after()) / (m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity) ), m_storage.m_number_segments +1);
        if(ideal_number_of_segments < density_threshold){
            plan->m_window_length = m_storage.m_number_segments;
            do {
                plan->m_window_length *= 2;
                density /= 2;
            } while(density > m_density_bounds0.get_upper_threshold_root());
//            COUT_DEBUG("original size: " << m_storage.m_number_segments << ", final length: " << plan->m_window_length << ", density: " << density);
        } else {
            const size_t segments_per_extent = m_storage.get_segments_per_extent();
            size_t num_extents = ceil( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
            assert(num_extents >= m_storage.get_number_extents());
            if(num_extents == m_storage.get_number_extents()) num_extents++;
            plan->m_window_length = num_extents * segments_per_extent;
        }

        if(m_storage.m_number_segments >= m_storage.get_segments_per_extent()){ // use rewiring
            plan->m_operation = RebalanceOperation::RESIZE_REBALANCE;
        } else {
            plan->m_operation = RebalanceOperation::RESIZE;
        }
    } else { // this should be a normal rebalance
        plan->m_operation =  RebalanceOperation::REBALANCE;
    }
}

void PackedMemoryArray::do_rebalance_local(const RebalancePlan& action, InsertionT* insertion) {
    COUT_DEBUG("Plan: " << action);

    switch(action.m_operation){
    case RebalanceOperation::REBALANCE:
        spread_local(action, insertion); // local to the gate
        break;
    case RebalanceOperation::RESIZE_REBALANCE:
    case RebalanceOperation::RESIZE:
        resize_local(action, insertion);
        break;
    default:
        assert(0 && "Invalid operation");
    }
}

/*****************************************************************************
 *                                                                           *
 *   Local resize                                                            *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::resize_local(const RebalancePlan& action, InsertionT* insertion) {
    assert( action.is_insert() == (insertion != nullptr)  && "action.is_insert() <=> insertion != nullptr (ptr to the element to insert)");
    bool do_insert = action.is_insert();
    size_t num_segments = action.m_window_length; // new number of segments
    COUT_DEBUG("# segments, from: " << m_storage.m_number_segments << " -> " << num_segments);
    assert(m_storage.m_number_segments <= get_segments_per_lock() && "Otherwise the procedure should have been performed by the RebalancingMaster");
    assert(num_segments <= get_segments_per_lock() && "Otherwise the procedure should have been performed by the RebalancingMaster");

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    BufferedRewiredMemory* ixRewiredMemoryKeys;
    BufferedRewiredMemory* ixRewiredMemoryValues;
    RewiredMemory* ixRewiredMemoryCardinalities;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    swap(ixRewiredMemoryKeys, m_storage.m_memory_keys);
    swap(ixRewiredMemoryValues, m_storage.m_memory_values);
    swap(ixRewiredMemoryCardinalities, m_storage.m_memory_sizes);
    auto xDeleter = [&](void*){ Storage::dealloc_workspace(&ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities); };
    unique_ptr<PackedMemoryArray, decltype(xDeleter)> ixCleanup { this, xDeleter };
    int64_t* __restrict xKeys = m_storage.m_keys;
    int64_t* __restrict xValues = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict xSizes = m_storage.m_segment_sizes;

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    while(ixSizes[input_segment_id] + ixSizes[input_segment_id +1] == 0){
        input_segment_id += 2;
        assert(input_segment_id < m_storage.m_number_segments && "Are all segments empty?");
    }
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity * (input_segment_id +1) - ixSizes[input_segment_id];
    int64_t* input_values = ixValues + m_storage.m_segment_capacity * (input_segment_id +1) - ixSizes[input_segment_id];
    size_t input_size = ixSizes[input_segment_id] + ixSizes[input_segment_id +1];

    // cardinality of the output segments
    const size_t output_elts_per_segments = action.get_cardinality_after() / num_segments;
    const size_t output_num_odd_segments = action.get_cardinality_after() % num_segments;
    assert(output_elts_per_segments > 0 && "No segments should be empty");
    assert(output_elts_per_segments + 1 < m_storage.m_segment_capacity && "It breaks the logic for the insertions, a segment may be potential full before the elt is going to be inserted");

    for(size_t j = 0; j < num_segments; j+=2){
        // elements to copy
        xSizes[j] = output_elts_per_segments + (j < output_num_odd_segments);
        xSizes[j+1] = num_segments == 1 ? 0 : (output_elts_per_segments + (j+1 < output_num_odd_segments));
        size_t elements_to_copy = xSizes[j] + xSizes[j+1];

        // base pointers for the output
        const size_t output_offset = (j+1) * m_storage.m_segment_capacity - xSizes[j];
        int64_t* output_keys = xKeys + output_offset;
        int64_t* output_values = xValues + output_offset;

        do {
            size_t cpy1 = min(elements_to_copy, input_size);
            assert((do_insert || cpy1 > 0) && "Infinite loop, missing elements to insert");

            size_t input_copied, output_copied;

            if(do_insert && (input_size == 0 || insertion->m_key <= input_keys[cpy1 -1])){ // merge with the new element to insert
                // note, in the above guard, cpy1 is always evaluated when > 0, otherwise the predicate input_size == 0 is true
                input_copied = max<int64_t>(0, static_cast<int64_t>(cpy1) -1); // min = 0
                output_copied = input_copied +1;
                spread_insert_unsafe(input_keys, input_values, output_keys, output_values, input_copied, insertion->m_key, insertion->m_value);
                do_insert = false;
            } else {
                input_copied = output_copied = cpy1;
                memcpy(output_keys, input_keys, cpy1 * sizeof(m_storage.m_keys[0]));
                memcpy(output_values, input_values, cpy1 * sizeof(m_storage.m_values[0]));
            }

            assert(output_copied >= 1 && "Made no progress");
            output_keys += output_copied; input_keys += input_copied;
            output_values += output_copied; input_values += input_copied;
            input_size -= input_copied;

            while(input_size == 0){ // refill the input
                assert(input_segment_id % 2 == 0 && "Always expected to be even");
                input_segment_id += 2;
                if(input_segment_id >= m_storage.m_number_segments) break; // overflow
                input_size = ixSizes[input_segment_id] + ixSizes[input_segment_id +1];
                size_t input_offset = m_storage.m_segment_capacity * (input_segment_id +1) - ixSizes[input_segment_id];
                input_keys = ixKeys + input_offset;
                input_values = ixValues + input_offset;
            }

            elements_to_copy -= output_copied;
        } while(elements_to_copy > 0);

        // update the separator keys, there should be no empty segments
        set_separator_key(j, xKeys[output_offset]);
        if((j+1) < num_segments) set_separator_key(j+1, xKeys[output_offset + xSizes[j]]);
    }

    assert(do_insert == false && "The new element should have been inserted");

    // Reset the separator keys in the gate in case of downsizing
    if(/* new number of segments */ num_segments < /* old number of segments */ m_storage.m_number_segments){ // only when decreasing the size of the PMA
        assert(m_storage.m_number_segments <= get_segments_per_lock() && "Otherwise this task should have been performed by the global rebalancer");
        for(size_t i = num_segments; i < m_storage.m_number_segments; i++){
            set_separator_key(i, numeric_limits<int64_t>::max());
        }
    }

    // update the PMA properties
    m_storage.m_number_segments = num_segments;
}

/*****************************************************************************
 *                                                                           *
 *   Local spread                                                            *
 *                                                                           *
 *****************************************************************************/

void PackedMemoryArray::spread_local(const RebalancePlan& action, InsertionT* insertion){
    COUT_DEBUG("start: " << action.m_window_start << ", length: " << action.m_window_length);
    assert(action.m_window_length <= get_segments_per_lock() && "This operation should have been performed by the RebalancingMaster");

    // workspace
    auto fn_deallocate = [this](void* ptr){ m_memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> input_keys_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    unique_ptr<int64_t, decltype(fn_deallocate)> input_values_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_values = input_values_ptr.get();
    uint16_t* __restrict segment_sizes_shifted = m_storage.m_segment_sizes + action.m_window_start;

    // 1) first copy all elements in input keys
    int64_t insert_position = -1;
    spread_load(action, input_keys, input_values, insertion, &insert_position);

//    // debug only
//#if defined(DEBUG)
//    for(size_t i =0; i < action.get_cardinality_after(); i++){
//        cout << "Input [" << i << "] <" << input_keys[i] << ", " << input_values[i] << ">" << endl;
//    }
//#endif

    // 2) adjust the cardinality of the single segments
    const size_t elements_per_segments = action.get_cardinality_after() / action.m_window_length;
    const size_t num_odd_segments = action.get_cardinality_after() % action.m_window_length;
    assert((elements_per_segments + (num_odd_segments >0)) <= m_storage.m_segment_capacity && "Segment overfilled");
    for(size_t i = 0; i < num_odd_segments; i++){ segment_sizes_shifted[i] = elements_per_segments +1; }
    for(size_t i = num_odd_segments, end = action.m_window_length; i < end; i++){  segment_sizes_shifted[i] = elements_per_segments; }

    // 3) copy the elements from input_keys to the final segments
    size_t segment_id = action.m_window_start;
    assert(action.m_window_length % 2 == 0 && "Expected an even number of segments");
    for(size_t i = 0, sz = action.m_window_length; i < sz; i+=2){
        const size_t card_left = elements_per_segments + (i < num_odd_segments);
        const size_t card_right = elements_per_segments + ((i+1) < num_odd_segments);
        const size_t card_total = card_left + card_right;

        if(card_total > 0){
            int64_t output_displ = (segment_id +1) * m_storage.m_segment_capacity - card_left;
            int64_t* __restrict output_keys = m_storage.m_keys + output_displ;
            int64_t* __restrict output_values = m_storage.m_values + output_displ;
            memcpy(output_keys, input_keys, card_total * sizeof(output_keys[0]));
            memcpy(output_values, input_values, card_total * sizeof(output_keys[0]));

            set_separator_key(segment_id,  card_left > 0 ? output_keys[0] : max_separator_key(segment_id));
            set_separator_key(segment_id + 1, card_right > 0 ? output_keys[card_left] : max_separator_key(segment_id +1));
        }

        input_keys += card_total;
        input_values += card_total;
        segment_id += 2;
    }

    COUT_DEBUG("Done");
}


size_t PackedMemoryArray::spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value){
    size_t i = 0;
    while(i < num_elements && keys_from[i] < new_key){
        keys_to[i] = keys_from[i];
        values_to[i] = values_from[i];
        i++;
    }
    keys_to[i] = new_key;
    values_to[i] = new_value;

    memcpy(keys_to + i + 1, keys_from + i, (num_elements -i) * sizeof(keys_to[0]));
    memcpy(values_to + i + 1, values_from + i, (num_elements -i) * sizeof(values_to[0]));

    m_cardinality++;

    return i;
}

void PackedMemoryArray::spread_load(const RebalancePlan& action, int64_t* __restrict keys_to, int64_t* __restrict values_to, InsertionT* insertion, int64_t* output_position_key_inserted){
    // insert position
    assert((!action.is_insert() || insertion != nullptr) && "If the plan covers an insertion, then the actual element to insert must be specified");
    int64_t position_key_inserted = 0;

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    int64_t* __restrict workspace_keys = m_storage.m_keys + action.m_window_start * m_storage.m_segment_capacity;
    int64_t* __restrict workspace_values = m_storage.m_values + action.m_window_start * m_storage.m_segment_capacity;
    segment_size_t* __restrict workspace_sizes = m_storage.m_segment_sizes + action.m_window_start;

    bool do_insert = action.is_insert();
    int64_t new_key_segment = do_insert ? (insertion->m_segment_id - action.m_window_start) : -1;

    for(int64_t i = 1; i < action.m_window_length; i+=2){
        size_t length = workspace_sizes[i -1] + workspace_sizes[i];
        size_t offset = (m_storage.m_segment_capacity * i) - workspace_sizes[i-1];
        int64_t* __restrict keys_from = workspace_keys + offset;
        int64_t* __restrict values_from = workspace_values + offset;

        // destination
        if (new_key_segment == i || new_key_segment == i -1){
            position_key_inserted += spread_insert_unsafe(keys_from, values_from, keys_to, values_to, length, insertion->m_key, insertion->m_value);
            if(output_position_key_inserted) *output_position_key_inserted = position_key_inserted;
            do_insert = false;
            keys_to++; values_to++;
        } else {
            memcpy(keys_to, keys_from, sizeof(keys_to[0]) * length);
            memcpy(values_to, values_from, sizeof(values_from[0]) * length);
            if(do_insert) { position_key_inserted += length; } // the inserted key has not been yet inserted
        }

        keys_to += length; values_to += length;
    }

    if(do_insert){
        position_key_inserted += spread_insert_unsafe(nullptr, nullptr, keys_to, values_to, 0, insertion->m_key, insertion->m_value);
        if(output_position_key_inserted) *output_position_key_inserted = position_key_inserted;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/

int64_t PackedMemoryArray::find(int64_t key) const {
//    COUT_DEBUG("key: " << key);
    if(empty()) return -1;

    int64_t value = -1;
    bool done = false;
    do{
        try {
            ScopedState scope{ this };
            Gate* gate = find_on_entry(key);
            value = do_find(gate, key);
            find_on_exit(gate);
            done = true;
        } catch (Abort) { /* retry */ }
    } while (!done);

    return value;
}

int64_t PackedMemoryArray::do_find(Gate* gate, int64_t key) const{
    auto segment_id = gate->find(key);
    COUT_DEBUG("gate: " << gate->lock_id() << ", key: " << key << ", segment_id: " << segment_id);

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];

    size_t start, stop;
    if(segment_id % 2 == 0){ // even
        stop = m_storage.m_segment_capacity;
        start = stop - sz;
    } else { // odd
        start = 0;
        stop = sz;
    }

    for(size_t i = start; i < stop; i++){
        if(keys[i] == key){
            return *(m_storage.m_values + segment_id * m_storage.m_segment_capacity + i);
        }
    }

    return -1;
}

Gate* PackedMemoryArray::find_on_entry(int64_t key) const {
    return reader_on_entry(key);
}

void PackedMemoryArray::find_on_exit(Gate* gate) const {
    reader_on_exit(gate);
}

int PackedMemoryArray::find_position(size_t segment_id, int64_t key) const noexcept {
    if(key == std::numeric_limits<int64_t>::min()) return 0;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    if(key == std::numeric_limits<int64_t>::max()) return static_cast<int>(sz);
    // in ::rebalance, we may temporary alter the size of a segment to segment_capacity +1
    sz = min<size_t>(sz, m_storage.m_segment_capacity); // avoid overflow

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int start, stop;

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), the keys are at the end
        start = m_storage.m_segment_capacity - sz;
        stop = m_storage.m_segment_capacity;
    } else { // odd segment ids (1, 3, ...), the keys are at the start of the segment
        start = 0;
        stop = sz;
    }

    for(int i = start; i < stop; i++){
        if(keys[i] == key){
            return i -start;
        }
    }

    return -1; // not found
}

/*****************************************************************************
 *                                                                           *
 *   Range queries                                                           *
 *                                                                           *
 *****************************************************************************/

unique_ptr<::data_structures::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const {
    return make_unique<Iterator>( this, min, max );
}

unique_ptr<::data_structures::Iterator> PackedMemoryArray::iterator() const {
    return find(numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/
::data_structures::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    using SumResult = ::data_structures::Interface::SumResult;
    if(/* empty ? */m_cardinality == 0 ||
       /* invalid min, max */ max < min ||
       /* scans disabled */ !::data_structures::global_parallel_scan_enabled){ return SumResult{}; }

    bool done = false;
    SumResult result;
    result.m_first_key = numeric_limits<int64_t>::max();

    do {
        try {
            ScopedState scope { this };
            auto gate_id = m_index.get(get_context())->find(min);
            do_sum(gate_id, /* in/out */ min, /* in */ max, /* in/out */ &result);
            done = true;
        } catch (Abort){ /* retry */ }
    } while (!done);


    if(result.m_num_elements == 0)
        result.m_first_key = 0;

    return result;
}

void PackedMemoryArray::do_sum(uint64_t gate_id, int64_t& next_min, int64_t max, ::data_structures::Interface::SumResult* __restrict sum) const {
    assert(sum != nullptr && "Null pointer");
//    COUT_DEBUG("gate_id: " << gate_id << ", min: " << next_min << ", max: " << max << ", partial sum: " << *sum);
    bool sum_done = false;

#if !defined(NDEBUG) // DEBUG ONLY
    int64_t key_previous = numeric_limits<int64_t>::min();
#endif

    do {
        bool read_all { false };
        Gate* gate = sum_on_entry(gate_id, next_min, max, &read_all);
//        COUT_DEBUG("READER ENTRY gate_id: " << gate->gate_id() << ", readall: " << read_all << ", min: " << next_min << ", max: " << max);

        if(read_all){ // read the whole content protected by this gate
            int64_t* __restrict keys = m_storage.m_keys + gate->m_window_start * m_storage.m_segment_capacity;
            int64_t* __restrict values = m_storage.m_values + gate->m_window_start * m_storage.m_segment_capacity;
            uint16_t* __restrict cardinalities = m_storage.m_segment_sizes + gate->m_window_start;

            sum->m_first_key = std::min(sum->m_first_key, keys[m_storage.m_segment_capacity - cardinalities[0]]);
            for(int64_t segment_id = 0, last_segment_id = gate->m_window_length; segment_id < last_segment_id; segment_id+= 2){
                int64_t start = (segment_id+1) * m_storage.m_segment_capacity - cardinalities[segment_id];
                int64_t end = start + cardinalities[segment_id] + cardinalities[segment_id +1];

                for(int64_t i = start; i < end; i++){
                    sum->m_sum_keys += keys[i];
                    sum->m_sum_values += values[i];
#if !defined(NDEBUG)
                    assert(keys[i] >= key_previous && "Sorted order not respected");
                    key_previous = keys[i];
#endif
                }
                sum->m_num_elements += (end - start);
            }
            sum->m_last_key = keys[m_storage.m_segment_capacity * (gate->m_window_length -1) + cardinalities[gate->m_window_length -1] -1];
        } else { // read only partially this chunk of the array
            int64_t* __restrict keys = m_storage.m_keys;
            uint16_t* __restrict cardinalities = m_storage.m_segment_sizes;

            bool min_notfound = true;
            int64_t segment_begin = gate->find(next_min), start = 0;
            if(segment_begin % 2 == 0){
                start = ( segment_begin +1 )* m_storage.m_segment_capacity - cardinalities[segment_begin];
            } else {
                start = segment_begin * m_storage.m_segment_capacity;
            }
            segment_begin = (segment_begin / 2) * 2; // make it even: 0 => 0, 1 => 0, 2 => 2, 3 => 2, ...
            int64_t stop = ( segment_begin +1 )* m_storage.m_segment_capacity + cardinalities[segment_begin +1];
            int64_t window_end = ( gate->lock_id() == 0 && m_storage.m_number_segments < gate->m_window_length ) ?
                    std::max<int64_t>(2, m_storage.m_number_segments) : // the storage always guarantee that sizes[1] exists, in case set to 0
                    gate->m_window_start + gate->m_window_length;

            // find the starting offset
            while(min_notfound && segment_begin < window_end){
                while(start < stop && keys[start] < next_min){ start++; }

                min_notfound = (start == stop);
                if(min_notfound){
                    segment_begin+=2;
                    if(segment_begin < window_end){
                        start = (segment_begin +1) * m_storage.m_segment_capacity - cardinalities[segment_begin];
                        stop = start + cardinalities[segment_begin] + cardinalities[segment_begin +1];
                    }
                }
            }

//            COUT_DEBUG("segment_begin: " << segment_begin << ", start: " << start << ", stop: " << stop << ", min_notfound: " << min_notfound);

            // find the ending offset
            int64_t segment_end = -1, end = -1;
            bool max_notfound = true;
            if(max > gate->m_fence_high_key){
                // read the rest of the segment
                segment_end = window_end -1; // -1 => inclusive
                end = segment_end * m_storage.m_segment_capacity + cardinalities[segment_end];
                max_notfound = false;
            } else {
                segment_end = gate->find(max);
                if(segment_end >= window_end -1) segment_end = window_end -1; // inclusive
                // make it odd: 0 => 1, 1 => 1, 2 => 3, 3 => 3, ...
                segment_end = (segment_end / 2) * 2 +1;
                end = segment_end * m_storage.m_segment_capacity + cardinalities[segment_end];
                {
                    int64_t stop = segment_end * m_storage.m_segment_capacity - cardinalities[segment_end -1];
                    int64_t index = end -1;

                    while(max_notfound && segment_end >= segment_begin){
                        while(index >= stop && keys[index] > max) index--;
                        max_notfound = (index < stop);
                        if(max_notfound){
                            segment_end -= 2;
                            if(segment_end >= segment_begin){
                                index = segment_end * m_storage.m_segment_capacity + cardinalities[segment_end] -1;
                                stop = segment_end * m_storage.m_segment_capacity - cardinalities[segment_end -1];
                            }
                        }
                    }

                    end = index +1;
                }
            }
//            COUT_DEBUG("segment_end: " << segment_end << ", end " << end << ", max_notfound: " << max_notfound);

            // read between start and end
            if(!min_notfound && !max_notfound){
                int64_t offset = start;
                int64_t segment_id = segment_begin;
                assert(segment_id % 2 == 0 && "Expected even, always");
                stop = std::min(stop, end);

                int64_t* __restrict values = m_storage.m_values;
                sum->m_first_key = std::min(sum->m_first_key, keys[offset]);

                while(offset < stop){
                    sum->m_num_elements += (stop - offset);
                    while(offset < stop){
                        sum->m_sum_keys += keys[offset];
                        sum->m_sum_values += values[offset];
#if !defined(NDEBUG)
                        assert(keys[offset] >= key_previous && "Sorted order not respected");
                        key_previous = keys[offset];
#endif
                        offset++;
                    }

                    segment_id += 2; // next even segment
                    if(segment_id < window_end){
                        int64_t size_lhs = m_storage.m_segment_sizes[segment_id];
                        assert(size_lhs >= 0 && size_lhs <= m_storage.m_segment_capacity);
                        int64_t size_rhs = m_storage.m_segment_sizes[segment_id +1];
                        assert(size_rhs >= 0 && size_rhs <= m_storage.m_segment_capacity);
                        offset = (segment_id +1) * m_storage.m_segment_capacity - size_lhs;
                        stop = std::min(end, offset + size_lhs + size_rhs);
                    }
                }
                sum->m_last_key = keys[end -1];
                sum_done = end < (window_end -1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[window_end -1];
            }

        } // end if (read partially this chunk)

        next_min = gate->m_fence_high_key;
        if(!sum_done && (next_min == numeric_limits<int64_t>::max() || (next_min +1) > max || !(::data_structures::global_parallel_scan_enabled))){
            sum_done = true;
        } else {
            next_min++;
            gate_id = gate->lock_id() +1; // next gate to access
        }

//        COUT_DEBUG("READER EXIT gate_id: " << gate->gate_id());
        sum_on_exit(gate);

    } while (!sum_done);
}


Gate* PackedMemoryArray::sum_on_entry(uint64_t gate_id, int64_t min, int64_t max, bool* out_readall) const{
    Gate* gate = reader_on_entry(min, gate_id);
    if(out_readall != nullptr){
        *out_readall = min <= gate->m_fence_low_key && gate->m_fence_high_key <= max && m_storage.m_number_segments >= gate->m_window_length;
    }
    return gate;
}

void PackedMemoryArray::sum_on_exit(Gate* gate) const {
    reader_on_exit(gate);
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void PackedMemoryArray::dump() const { dump(std::cout);  }

void PackedMemoryArray::dump(std::ostream& out) const {
    scoped_lock<mutex> lock(_debug_mutex);

    bool integrity_check = true;

    m_index.get_unsafe()->dump(out, &integrity_check);
    out << "\n";

    dump_locks(out, &integrity_check);
    out << "\n";

    dump_storage(out, &integrity_check);
    out << "\n";

    m_garbage_collector->dump(out);
    out << "\n";

    out << "Density bounds (user): " << m_density_bounds0.densities().rho_0 << ", " <<
            m_density_bounds0.densities().rho_h << ", " <<
            m_density_bounds0.densities().theta_h << ", " <<
            m_density_bounds0.densities().theta_0 << "\n";
    out << "Density bounds (primary): " << m_density_bounds1.densities().rho_0 << ", " <<
            m_density_bounds1.densities().rho_h << ", " <<
            m_density_bounds1.densities().theta_h << ", " <<
            m_density_bounds1.densities().theta_0 << "\n";

    assert(integrity_check && "Integrity check failed!");
}

void PackedMemoryArray::dump_locks(std::ostream& out, bool* integrity_check) const {
    size_t num_locks = get_number_locks();
    out << "[Locks] Number of locks: " << num_locks << "\n";
    int64_t fence_key_next = numeric_limits<int64_t>::min();
    for(int64_t i = 0; i < num_locks; i++){
        Gate& gate = m_locks.get_unsafe()[i];
        out << "[" << i << "] interval: [" << gate.m_window_start << ", " << gate.m_window_start + gate.m_window_length << ")";
        out << ", state: ";
        switch(gate.m_state){
        case Gate::State::FREE: out << "free"; break;
        case Gate::State::READ: out << "read"; break;
        case Gate::State::WRITE: out << "write"; break;
        case Gate::State::TIMEOUT: out << "timeout"; break;
        case Gate::State::REBAL: out << "rebal"; break;
        default: out << "?"; break;
        }
        out << ", active threads: " << gate.m_num_active_threads;
        out << ", queue: " << gate.m_queue.size();
        out << ", cardinality: " << gate.m_cardinality;
        out << ", fence keys: " << gate.m_fence_low_key << ", " << gate.m_fence_high_key;

        if(i * get_segments_per_lock() != gate.m_window_start){
            out << " (ERROR: invalid window, expected: " << (i * get_segments_per_lock()) << ", got: " << gate.m_window_start << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(i != gate.lock_id()){
            out << " (ERROR: invalid gate id: " << gate.lock_id() << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(gate.m_fence_low_key != fence_key_next){
            out << " (ERROR: invalid lower fence key: " << gate.m_fence_low_key << ", expected: " << fence_key_next << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(gate.m_fence_low_key >= gate.m_fence_high_key){
            out << " (ERROR: the lower fence key is greater than the higher fence key)";
            if(integrity_check) *integrity_check = false;
        }

        fence_key_next = gate.m_fence_high_key +1;
        out << "\n    Separator keys: ";
        for(int64_t j =0; j < static_cast<int64_t>(gate.m_window_length) -1; j++){
            if(j > 0) out << ", ";
            out << gate.m_separator_keys[j];
        }
        out << endl;
    }
}

void PackedMemoryArray::dump_storage(std::ostream& out, bool* integrity_check) const {
    out << "[PMA] cardinality: " << m_cardinality << ", capacity: " << m_storage.capacity() << ", " <<
            "height: "<< m_storage.hyperheight() << ", #segments: " << m_storage.m_number_segments <<
            ", segment size: " << m_storage.m_segment_capacity << ", pages per extent: " << m_storage.m_pages_per_extent <<
            ", segments per extent: " << m_storage.get_segments_per_extent() <<
            ", segments per lock: " << get_segments_per_lock() <<
            ", # segments for balanced thresholds: " << balanced_thresholds_cutoff() << endl;

    if(empty()){ // edge case
        out << "-- empty --" << endl;
        return;
    }

    int64_t previous_key = numeric_limits<int64_t>::min();

    int64_t* keys = m_storage.m_keys;
    int64_t* values = m_storage.m_values;
    auto sizes = m_storage.m_segment_sizes;
    size_t tot_count = 0;

    for(size_t i = 0; i < m_storage.m_number_segments; i++){
        out << "[" << i << "] ";

        tot_count += sizes[i];
        bool even = i % 2 == 0;
        size_t start = even ? m_storage.m_segment_capacity - sizes[i] : 0;
        size_t end = even ? m_storage.m_segment_capacity : sizes[i];

        for(size_t j = start, sz = end; j < sz; j++){
            if(j > start) out << ", ";
            out << "<" << keys[j] << ", " << values[j] << ">";

//            // only for the unit tests
//            if(keys[j] <= 0){
//                out << " (ERROR: key less than 1)";
//                if(integrity_check) *integrity_check = false;
//            }

            if(keys[j] < previous_key){
                out << " (ERROR: order mismatch: " << previous_key << " > " << keys[j] << ")";
                if(integrity_check) *integrity_check = false;
            }
            previous_key = keys[j];
        }
        out << endl;

        // check the content of the index is correct
        int64_t gate_id = i / get_segments_per_lock();
        if(i % get_segments_per_lock() == 0){
            int64_t indexed_key = m_index.get_unsafe()->get_separator_key(gate_id);
            if(keys[start] < indexed_key){
                out << " (ERROR: invalid key in the index, minimum: " << keys[start] << ", indexed key: " << indexed_key << ", gate: " << gate_id  << ")" << end;
                if(integrity_check) *integrity_check = false;
            }
        } else { // check the content in the extent
            int64_t offset = i % get_segments_per_lock() -1;
            int64_t  indexed_key = m_locks.get_unsafe()[gate_id].m_separator_keys[offset];
            if(keys[start] != indexed_key){
                out << " (ERROR: invalid key in the extent, minimum: " << keys[start] << ", indexed key: " << indexed_key << ", gate: " << gate_id  << ")" << end;
                if(integrity_check) *integrity_check = false;
            }
        }


        // next segment
        keys += m_storage.m_segment_capacity;
        values += m_storage.m_segment_capacity;
    }

    if(m_cardinality != tot_count){
        out << " (ERROR: size mismatch, pma registered cardinality: " << m_cardinality << ", computed cardinality: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Debug only                                                              *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::debug_validate_cardinality_gate(Gate* gate, int64_t cardinality_change){
#if !defined(NDEBUG)
    assert(gate != nullptr && "null pointer");

    int64_t segments_cardinality = 0;
    int64_t window_end = std::min<int64_t>(gate->m_window_start + gate->m_window_length, m_storage.m_number_segments);
    for(int64_t segment_id = gate->m_window_start; segment_id < window_end; segment_id ++){
        segments_cardinality += m_storage.m_segment_sizes[segment_id];
    }
    if(segments_cardinality != gate->m_cardinality){
        for(int64_t segment_id = gate->m_window_start; segment_id < window_end; segment_id ++){
            COUT_DEBUG_FORCE("segment[" << segment_id << "]: " <<  m_storage.m_segment_sizes[segment_id]);
        }
        COUT_DEBUG_FORCE("cardinality mismatch, gate " << gate->lock_id() << " cardinality: " << gate->m_cardinality << ", cardinality_change: " << cardinality_change << ", segments cardinality: " << segments_cardinality);
        assert(0 && "cardinality mismatch");
    }
#endif
}


/*****************************************************************************
 *                                                                           *
 *   Parallel Callbacks                                                      *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::on_init_main(int num_threads) {
    set_max_number_workers(num_threads);
}
void PackedMemoryArray::on_init_worker(int worker_id) {
    register_thread(worker_id);
}
void PackedMemoryArray::on_destroy_worker(int worker_id) {
    unregister_thread();
}

void PackedMemoryArray::on_complete(){
    m_timer_manager->flush();
    m_rebalancer->complete();
}

void PackedMemoryArray::on_destroy_main() {
    set_max_number_workers(0);
}

} // namespace
