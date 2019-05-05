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
#include "rma/common/abort.hpp"
#include "rma/common/buffered_rewired_memory.hpp"
#include "rma/common/move_detector_info.hpp"
#include "adaptive_rebalancing.hpp"
#include "garbage_collector.hpp"
#include "gate.hpp"
#include "iterator.hpp"
#include "rebalancing_master.hpp"
#include "thread_context.hpp"
#include "weights.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::one_by_one {

using Abort = common::Abort;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
mutex _debug_mutex;
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { lock_guard<mutex> lock(_debug_mutex); std::cout << "[PackedMemoryArray::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
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

PackedMemoryArray::PackedMemoryArray(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent, size_t num_worker_threads, size_t segments_per_lock) :
        m_storage(pma_segment_size, pages_per_extent),
        m_index(new StaticIndex(btree_block_size)),
        m_locks(Gate::allocate(1, segments_per_lock)),
        m_detector(m_knobs, 1, 8),
        m_density_bounds1(0, 0.75, 0.75, 1), /* there is rationale for these hardwired thresholds */
        m_rebalancer(new RebalancingMaster{ this, num_worker_threads } ),
        m_garbage_collector( new GarbageCollector(this) ),
        m_segments_per_lock(segments_per_lock){
    if(!is_power_of_2(segments_per_lock)) throw std::invalid_argument("[PackedMemoryArray::ctor] Invalid value for the `segments_per_lock', it is not a power of 2");
    if(segments_per_lock < 2) throw std::invalid_argument("[PackedMemoryArray::ctor] Invalid value for the `segments_per_lock', it must be >= 2");
    if(m_storage.get_segments_per_extent() % segments_per_lock != 0) throw std::invalid_argument("[PackedMemoryArray::ctor] The parameter `segments_per_extent' must be a multiple of `segments_per_lock'");

    // start the garbage collector
    GC()->start();

    // start the rebalancer
    m_rebalancer->start();

    // by default allow one thread to run
    m_thread_contexts.resize(1);

    m_index.get_unsafe()->set_separator_key(0, numeric_limits<int64_t>::min());
}


PackedMemoryArray::~PackedMemoryArray() {
    // stop the rebalancer
    delete m_rebalancer; m_rebalancer = nullptr;

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;

    // remove the index
    delete m_index.get_unsafe(); m_index.set(nullptr);

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

    ThreadContext::register_thread(client_id);
    m_thread_contexts[client_id]->enter();
}

void PackedMemoryArray::unregister_thread(){
    get_context()->exit();
    ThreadContext::unregister_thread();
}

ThreadContext* PackedMemoryArray::get_context() const {
    return m_thread_contexts[ ThreadContext::thread_id() ];
}

GarbageCollector* PackedMemoryArray::GC() const noexcept{
    return m_garbage_collector;
}

/*****************************************************************************
 *                                                                           *
 *   Concurrency                                                             *
 *                                                                           *
 *****************************************************************************/

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

void PackedMemoryArray::writer_main(){
    ThreadContext* __restrict context = get_context();
    assert(context->has_update() && "No operation set to perform");
    do {
        try {
            COUT_DEBUG("entry: " << context->get_update());

            ScopedState scope { this }; // enter a new epoch
            Gate* gate = writer_on_entry();

            int64_t num_insertions {0}, num_deletions {0};
            while (gate != nullptr){ // nullptr => the previous item has been forwarded to another worker, restart
                // to avoid starvation with a writer continuously owning the same gate, every N consecutive updates check
                // whether there are readers waiting to take control of the same gate, and in case temporarily
                // release the ownership of this gate
                if(num_insertions + num_deletions >= 32 /* magic number */) {
                    gate = writer_check_gate(gate, num_insertions - num_deletions);
                    if(gate == nullptr) break; // we don't own this gate anymore => restart
                    num_insertions = num_deletions = 0; // reset the counters
                }

                auto& update = context->get_update();

                COUT_DEBUG("key to update (insert/delete): " << update);

                if(update.m_is_insert){ // insertion
                    bool inserted = do_insert(gate, update.m_key, update.m_value);

                    if(!inserted){ // this is going to take a while
                        rebalance_global(gate, /* cardinality change */ num_insertions - num_deletions);
                        gate = nullptr; // restart
                    } else {
                        num_insertions++;
//                        COUT_DEBUG_FORCE("gate: " << gate->lock_id() << ", key inserted: " << update.m_key << ", num_insertions: " << num_insertions);
                        context->fetch_local_queue();

                        // perform another update from the local queue?
                        if(!context->has_update() || gate->check_fence_keys(context->get_update().m_key) != Gate::Direction::GO_AHEAD){
                            writer_on_exit(gate, /* cardinality change */ num_insertions - num_deletions, /* rebalance ? */ false);
                            gate = nullptr; // restart
                        }
                    }
                } else { // handle a deletion
                    bool need_global_rebalance = do_remove(gate, update.m_key, /* ignored */ &update.m_value);

                    if( update.m_value != -1 ) num_deletions++; // did it actually remove a value ?
                    context->fetch_local_queue(); // next item to handle

                    if(need_global_rebalance || !context->has_update() || gate->check_fence_keys(context->get_update().m_key) != Gate::Direction::GO_AHEAD){
                        writer_on_exit(gate, /* cardinality change */ num_insertions - num_deletions, /* rebalance ? */ need_global_rebalance);
                        gate = nullptr; // restart
                    }
                }
            }
        } catch(Abort) { } // retry
    } while(context->has_update());
}

Gate* PackedMemoryArray::writer_on_entry() {
    ThreadContext* __restrict context = get_context();
    assert(context != nullptr);
    assert(context->has_update() && "No operation set to perform?");
    StaticIndex* index = m_index.get(*context); // snapshot, current index
    int64_t key = context->get_update().m_key; // the key to insert / delete
    auto gate_id = index->find(key);
    Gate* result = nullptr; // output

    bool done = false;
    do { // enter in the protected area
        Gate* gates = m_locks.get(*context);
        // enter in the private section
        auto& gate = gates[gate_id];
        unique_lock<Gate> lock(gate);
        // is this the right gate ?
        if(check_fence_keys(gate, /* in/out */ gate_id, key)){
            if (gate.m_writer != nullptr && gate.m_writer != context){ // this gate is likely busy, but a writer is already operating here
                // forward the update to the existing worker && return
                gate.m_writer->enqueue(context->get_update());
                if(gate.m_state == Gate::State::FREE) gate.wake_next(context); // edge case, we detected multiple writers on this gate
                lock.unlock();

                context->process_wakelist();
                context->fetch_local_queue(); // fetch the next item to insert/delete (at this point, most likely empty)
                result = nullptr; // we asked another worker to insert/remove the key instead of us
                done = true; // quit the loop
            } else if (gate.m_state == Gate::State::FREE) { // no one here
                assert(gate.m_num_active_threads == 0 && "Precondition not satisfied");
                gate.m_state = Gate::State::WRITE;
                gate.m_num_active_threads = 1;
                gate.m_writer = context;
                lock.unlock();

                result = gates + gate_id;
                done = true; // done, go on with the update
            } else {
                // add the thread in the queue
                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();

                gate.m_queue.append({ Gate::State::WRITE, &producer } );
                if(gate.m_state != Gate::State::REBAL) gate.m_writer = context;
                lock.unlock();
                consumer.wait();

                // done = false
            }
        }
    } while(!done);

    return result;
}

Gate* PackedMemoryArray::writer_check_gate(Gate* gate, int64_t cardinality_change){
    assert(gate != nullptr);
    bool yield_ownership { false };
    bool unlock_master { false };
    ThreadContext* context = get_context();
    assert(context->has_update() && "No update operation set");

    unique_lock<Gate> lock(*gate);

    // Multiple writers may still operate on the same gate
//    assert(gate->m_writer == context && "The writer accessing this method should have registered itself for this gate");

    assert(static_cast<int64_t>(gate->m_cardinality) + cardinality_change >= 0);
    gate->m_cardinality += cardinality_change;

    debug_validate_cardinality_gate(gate, cardinality_change);

    switch(gate->m_state){
    case Gate::State::WRITE: // same state as before
        if(gate->m_queue.size() > 0){ // there are other workers waiting to take control
            gate->m_state = Gate::State::FREE;
            gate->m_num_active_threads = 0;
            gate->wake_next(context);
            yield_ownership = true;
        }

        // gain control of the lock's writer queue
        if(gate->m_writer == nullptr) gate->m_writer = context;

        break;
    case Gate::State::REBAL:
        // the Rebalancer wants to process this gate
        unlock_master = true;
        gate->m_writer = nullptr;
        gate->m_num_active_threads = 0;
        yield_ownership = true;
        break;
    default:
        assert(0 && "Invalid state");
    }

    if(yield_ownership){
        std::promise<void> producer;
        std::future<void> consumer = producer.get_future();
        gate->m_queue.append({ Gate::State::WRITE, &producer } );
        lock.unlock();

        if(unlock_master){
            m_rebalancer->exit(gate->lock_id());
        } else {
            context->process_wakelist();
        }

        // ... ZzZ ...
        consumer.wait();

        if(unlock_master) return nullptr; // killed by the rebalancer

        lock.lock(); // regain control of this gate
        if(gate->m_state == Gate::State::FREE && gate->m_writer == context){
            gate->m_state = Gate::State::WRITE;
            gate->m_num_active_threads = 1;
        } else {
            // If gate->m_writer != content => the rebalancer could have changed the content of this gate
            // while this worker was waiting in the queue.
            // Note, it may also occur that a rebalancer did not operate on this gate, but gate->m_Writer != context;
            // however for simplicity reasons, it just better to restart from scratch
            gate = nullptr;
        }
        lock.unlock();
        context->process_wakelist();
    }

    return gate;
}

void PackedMemoryArray::writer_on_exit(Gate* gate, int64_t cardinality_change, bool rebalance){
    assert(gate != nullptr);
    bool unlock_master { false };
    ThreadContext* context = get_context();

    gate->lock();

    // if we shared the queue, then remove it
    if(gate->m_writer == context) gate->m_writer = nullptr;

    assert(static_cast<int64_t>(gate->m_cardinality) + cardinality_change >= 0);
    gate->m_cardinality += cardinality_change;
    debug_validate_cardinality_gate(gate, cardinality_change);

    gate->m_num_active_threads = 0;

    switch(gate->m_state){
    case Gate::State::WRITE:
        // same state as before

        if(rebalance){
            gate->m_state = Gate::State::REBAL;
        } else {
            gate->m_state = Gate::State::FREE;
            gate->wake_next(context);
        }

        break;
    case Gate::State::REBAL:
        // the Rebalancer wants to process this gate
        unlock_master = true;
        break;
    default:
        assert(0 && "Invalid state");
    }

    gate->unlock();

    const size_t gate_id = gate->lock_id();
    if(unlock_master){
        m_rebalancer->exit(gate_id);
    } else if (rebalance){
        m_rebalancer->rebalance(gate_id);
    } else { // wait the next worker in the wait list
        context->process_wakelist();
    }

    // Protect from a potential race condition. We first fetch an item from the queue, in writer_main(),
    // without a holding the lock for the gate. Another worker may have put some item the queue in the
    // meanwhile, while holding the lock to the gate.
    if(!context->has_update()) context->fetch_local_queue_unsafe();
}

Gate* PackedMemoryArray::reader_on_entry(int64_t key, int64_t start_gate_id) const {
    ThreadContext* context = get_context();
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
    do { // enter in the protected area
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
    bool send_message_to_rebalancer = false;
    ThreadContext* context = get_context();

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
       case Gate::State::REBAL: {
           gate->m_writer = nullptr; // invalidate the queue of a writer
           send_message_to_rebalancer = true;
       } break;
       default:
           assert(0 && "Invalid state");
       }
    }
    gate->unlock();

    if(send_message_to_rebalancer){
        m_rebalancer->exit(gate->lock_id());
    } else {
        context->process_wakelist();
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

const common::CachedDensityBounds& PackedMemoryArray::get_thresholds() const {
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

common::CachedMemoryPool& PackedMemoryArray::memory_pool() {
    return m_memory_pool;
}

common::Detector& PackedMemoryArray::detector(){
    return m_detector;
}

common::Knobs& PackedMemoryArray::knobs(){
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

size_t PackedMemoryArray::memory_footprint() const {
    size_t space_index = m_index.get_unsafe()->memory_footprint();
    size_t space_locks = get_segments_per_lock() * (sizeof(Gate) + /* separator keys */ (m_index.get_unsafe()->node_size() -1) * sizeof(int64_t));
    size_t space_storage = m_storage.memory_footprint();
    size_t space_detector = m_detector.capacity() * m_detector.sizeof_entry() * sizeof(uint64_t);

    return sizeof(decltype(*this)) + space_index + space_locks + space_storage + space_detector;
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

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::insert(int64_t key, int64_t value){
    get_context()->set_update(/* insert ? */ true, key, value);
    writer_main(); // update loop
}

//Gate* PackedMemoryArray::insert_on_entry(int64_t key, int64_t value){
//    return writer_on_entry(/* is_insert ? */ true, key, value);
//}
//
//void PackedMemoryArray::insert_on_exit(Gate* lock) {
//    writer_on_exit(lock, /* insertion ? */ true, /* deletion ? */ false, /* rebalance ? */ false);
//}

bool PackedMemoryArray::do_insert(Gate* gate, int64_t key, int64_t value){
    assert(gate != nullptr && "Null pointer");
    COUT_DEBUG("Gate: " << gate->lock_id() << ", key: " << key << ", value: " << value);

    if(UNLIKELY( empty() )){
        insert_empty(key, value);
        return true;
    } else {
        size_t segment = gate->find(key);
        return insert_common(segment, key, value);
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

bool PackedMemoryArray::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_number_segments && "Overflow: attempting to access an invalid segment in the PMA");
//    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        return rebalance_local(segment_id, &key, &value);
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
    int64_t predecessor, successor; // to update the detector

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), insert at the end of the segment
        size_t stop = m_storage.m_segment_capacity -1;
        size_t start = m_storage.m_segment_capacity - sz -1;
        size_t i = start;

        while(i < stop && keys[i+1] < key){
            keys[i] = keys[i+1];
            i++;
        }

//        COUT_DEBUG("(even) segment_id: " << segment_id << ", start: " << start << ", stop: " << stop << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = start; j < i; j++){
            values[j] = values[j+1];
        }
        values[i] = value;

        minimum = (i == start);
        bool maximum = (i == stop);

        // update the detector
        predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1];
        successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1];

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
        bool maximum = (i == sz);

        // update the detector
        predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1];
        successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1];
    }

    m_detector.insert(segment_id, predecessor, successor);

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
    get_context()->set_update(/* insert ? */ false, key, /* ignored */ -1);
    writer_main(); // update loop

    // in this implementation we don't report the value removed, as the operation can be asynchronously processed by a different worker
    return -1;
}

bool PackedMemoryArray::do_remove(Gate* gate, int64_t key, int64_t* out_value){
    assert(gate != nullptr && "Null pointer");
    assert(out_value != nullptr && "Null pointer");

    *out_value = -1;
    if(empty()) return false;
    bool request_global_rebalance = false;

    auto segment_id = gate->find(key);
//    COUT_DEBUG("Gate: " << gate->gate_id() << ", segment: " << segment_id << ", key: " << key);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    assert(sz > 0 && "Empty segment!");

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
    if(value != -1){
        m_detector.remove(segment_id, predecessor, successor);

        if(m_storage.m_number_segments >= 2 * balanced_thresholds_cutoff() && static_cast<double>(m_cardinality) < 0.5 * m_storage.capacity()){
            assert(m_storage.get_number_extents() > 1);
            request_global_rebalance = true;
        } else if(m_storage.m_number_segments > 1) {
            const size_t minimum_size = max<size_t>(get_thresholds(1).first * m_storage.m_segment_capacity, 1); // at least one element per segment
            if(sz < minimum_size){ request_global_rebalance = ! rebalance_local(segment_id, nullptr, nullptr); }
        }
    }

//#if defined(DEBUG)
//    dump();
//#endif

    *out_value = value;
    return request_global_rebalance;
}

/*****************************************************************************
 *                                                                           *
 *   Global rebalance                                                        *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::rebalance_global(Gate* gate, int64_t cardinality_change) {
    assert(gate != nullptr && "Null pointer");
    bool send_rebalance_request = true; // whether to send a rebalance request OR an unlock request to the Rebalancer

    gate->lock();
    assert(gate->m_num_active_threads == 1 && "There should be only a writer (the current thread) using this gate");
    assert(static_cast<int64_t>(gate->m_cardinality) + cardinality_change >= 0 && "Negative cardinality");
    gate->m_cardinality += cardinality_change;

    switch (gate->m_state){
    case Gate::State::WRITE:
        // the gate is in the same state of when it was last accessed
        gate->m_state = Gate::State::REBAL;
        break;
    case Gate::State::REBAL:
        // this gate has already been marked by the Rebalancer
        send_rebalance_request = false;
        break;
    default:
        assert(0 && "Invalid state");
    }

    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();

    gate->m_num_active_threads = 0;
    gate->m_writer = nullptr; // this worker is not active anymore on this gate
    gate->m_queue.prepend({ Gate::State::WRITE, &producer });

    gate->unlock();

    const size_t gate_id = gate->lock_id();
    if(send_rebalance_request){
        m_rebalancer->rebalance(gate_id);
    } else {
        m_rebalancer->exit(gate_id);
    }

    // ZzZ...
    consumer.wait();
}

/*****************************************************************************
 *                                                                           *
 *   Local rebalance                                                         *
 *                                                                           *
 *****************************************************************************/

bool PackedMemoryArray::rebalance_local(size_t segment_id, int64_t* key, int64_t* value){
    assert(((key && value) || (!key && !value)) && "Either both key & value are specified (insert) or none of them is (delete)");
    const bool is_insert = key != nullptr;

    int64_t window_start {0}, window_length {0}, cardinality {0};
    bool do_resize { false };
    bool is_local_rebalance = rebalance_find_window(segment_id, is_insert, &window_start, &window_length, &cardinality, &do_resize);
    if(!is_local_rebalance) return false;

    auto metadata = rebalance_plan(is_insert, window_start, window_length, cardinality, do_resize);

    if(is_insert){
        metadata.m_insert_key = *key;
        metadata.m_insert_value = *value;
        metadata.m_insert_segment = segment_id;
    }

    rebalance_run_apma(metadata, /* fill the segments ? */ true);
    do_rebalance_local(metadata);

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

    COUT_DEBUG("rho: " << rho << ", density: " << density << ", theta: " << theta << ", height: " << height << ", calibrator tree: " << m_storage.height() << ", is_insert: " << is_insertion);
    if((is_insertion && density <= theta) || (!is_insertion && density >= rho)){ // rebalance
        *out_cardinality_after = cardinality_after;
        *out_window_start = window_start;
        *out_window_length = window_length;
        *out_resize = false;
        return true;
    } else if ((is_insertion && cb_height < spe_height) || (!is_insertion && cb_height <= spe_height)) { // resize
        *out_cardinality_after = m_cardinality + (is_insertion ? 1 : 0);
        *out_window_start = *out_window_length = 0;
        *out_resize = true;
        return true;
    } else { // global rebalance
        return false;
    }
}

RebalancePlan PackedMemoryArray::rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const {
    COUT_DEBUG("is_insert: " << is_insert << ", window_start: " << window_start << ", window_length: " << window_length << ", cardinality_after: " << cardinality_after << ", resize: " << resize);

    RebalancePlan result { const_cast<common::CachedMemoryPool&>( m_memory_pool ) };
    result.m_is_insert = is_insert;
    result.m_cardinality_after = cardinality_after;

    const size_t density_threshold = balanced_thresholds_cutoff();

    if(!resize){
        result.m_operation = RebalanceOperation::REBALANCE;
        result.m_window_start = window_start;
        result.m_window_length = window_length;
    } else if (is_insert){ // resize on insertion
        result.m_window_start = 0;
        size_t ideal_number_of_segments = max<size_t>(ceil( static_cast<double>(cardinality_after) / (m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity) ), m_storage.m_number_segments +1);
        if(ideal_number_of_segments < density_threshold){
            result.m_window_length = m_storage.m_number_segments * 2;
            if(result.m_window_length > m_storage.get_segments_per_extent()){ // use rewiring
                result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            } else {
                result.m_operation = RebalanceOperation::RESIZE;
            }
        } else {
            result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            const size_t segments_per_extent = m_storage.get_segments_per_extent();
            size_t num_extents = ceil( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
            assert(num_extents >= m_storage.get_number_extents());
            if(num_extents == m_storage.get_number_extents()) num_extents++;
            result.m_window_length = num_extents * segments_per_extent;
        }
    } else { // resize on deletion
        // it doesn't support RESIZE_REBALANCE on deletions
        result.m_operation = RebalanceOperation::RESIZE;

        result.m_window_start = 0;
        const size_t ideal_number_of_segments = floor( static_cast<double>(cardinality_after) / static_cast<double>(m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity));
        const size_t segments_per_extent = m_storage.get_segments_per_extent();
        size_t num_extents = floor( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
        assert(num_extents <= m_storage.get_number_extents());
        if(num_extents == m_storage.get_number_extents()) num_extents--;

        if(num_extents * segments_per_extent <= density_threshold){
            if(m_storage.m_number_segments > density_threshold){
                result.m_window_length = density_threshold;
            } else {
                result.m_window_length = m_storage.m_number_segments /2;
            }
        } else {
            result.m_window_length = num_extents * segments_per_extent;
        }
    }

    return result;
}

void PackedMemoryArray::rebalance_run_apma(RebalancePlan& action, bool can_fill_segments, int64_t storage_window_length) {
    // hack: pretend the new element has already been inserted
    if(action.is_insert()){ m_storage.m_segment_sizes[action.m_insert_segment]++; }

    // detect the hammered segments/intervals
    size_t window_start = action.m_window_start;
    size_t window_length = action.m_operation == RebalanceOperation::REBALANCE ? action.m_window_length :
            (storage_window_length > 0 ? storage_window_length : m_storage.m_number_segments);
    Weights weights_builder{ *this, window_start, window_length };
    auto weights = weights_builder.release(); // hammered segments/intervals
    int wbalance = weights_builder.balance(); // = amount of hammer insertions minus amount of hammer deletions

    // hack: readjust the cardinalities
    if(action.is_insert()){ m_storage.m_segment_sizes[action.m_insert_segment]--; }

    common::MoveDetectorInfo mdi { *this, static_cast<size_t>( action.m_window_start ) }, *ptr_mdi = nullptr;
    if(action.m_operation == RebalanceOperation::REBALANCE){
        mdi.resize(2 * weights.size()); // it can move up to 2 *|weights| info
        ptr_mdi = &mdi;
    }

    set_thresholds(action); // update the thresholds of the calibrator tree

    AdaptiveRebalancing ar{ *this, move(weights), wbalance, (size_t) action.m_window_length, (size_t) action.get_cardinality_after(), ptr_mdi, can_fill_segments };
    action.m_apma_partitions = ar.release();
}

void PackedMemoryArray::do_rebalance_local(const RebalancePlan& action) {
    COUT_DEBUG("Plan: " << action);

    switch(action.m_operation){
    case RebalanceOperation::REBALANCE:
        spread_local(action); // local to the gate
        break;
    case RebalanceOperation::RESIZE_REBALANCE:
    case RebalanceOperation::RESIZE:
        resize_local(action);
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
void PackedMemoryArray::resize_local(const RebalancePlan& action) {
    bool do_insert = action.m_is_insert;
    size_t num_segments = action.m_window_length; // new number of segments
    COUT_DEBUG("# segments, from: " << m_storage.m_number_segments << " -> " << num_segments);
    assert(m_storage.m_number_segments <= get_segments_per_lock() && "Otherwise the procedure should have been performed by the RebalancingMaster");
    assert(num_segments <= get_segments_per_lock() && "Otherwise the procedure should have been performed by the RebalancingMaster");

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    common::BufferedRewiredMemory* ixRewiredMemoryKeys;
    common::BufferedRewiredMemory* ixRewiredMemoryValues;
    common::RewiredMemory* ixRewiredMemoryCardinalities;
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
    m_detector.resize(num_segments);

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    size_t input_size = ixSizes[0];
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity;
    int64_t* input_values = ixValues + m_storage.m_segment_capacity;
    bool input_segment_odd = false; // consider '0' as even
    if(input_size == 0){ // corner case, the first segment is empty!
        assert(!do_insert && "Otherwise we shouldn't see empty segments");
        input_segment_id = 1;
        input_segment_odd = true; // segment '1' is odd
        input_size = ixSizes[1];
    } else { // stick to the first segment, even!
        input_keys -= input_size;
        input_values -= input_size;
    }

    // start copying the elements
    bool output_segment_odd = false; // consider '0' as even
    struct {
        int index = 0; // current position in the vector partitions
        int segment = 0; // current segment considered
        int card_per_segment = 0; // cardinality per segment
        int odd_segments = 0; // number of segments with an additional element than `card_per_segment'
    } partition_state;
    const auto& partitions = action.m_apma_partitions;
    partition_state.card_per_segment = partitions[0].m_cardinality / partitions[0].m_segments;
    partition_state.odd_segments = partitions[0].m_cardinality % partitions[0].m_segments;

    for(size_t j = 0; j < num_segments; j++){
        // elements to copy
        size_t elements_to_copy = partition_state.card_per_segment + (partition_state.segment < partition_state.odd_segments);

//        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : m_storage.m_segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * m_storage.m_segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;
        if(input_size > 0) // protect from the edge case: the first segment will contain only one element, that is the new element to be inserted
            set_separator_key(j, input_keys[0]);

        do {
            assert(elements_to_copy <= m_storage.m_segment_capacity && "Overflow");
            assert(((input_size > 0) || (elements_to_copy == 1 && j == num_segments -1)) && "Empty input segment");
            size_t cpy1 = min(elements_to_copy, input_size);
            size_t input_copied, output_copied;
            if(do_insert && (input_size == 0 || action.m_insert_key <= input_keys[cpy1 -1])){
                // merge
                input_copied = max<int64_t>(0, static_cast<int64_t>(cpy1) -1); // min = 0
                output_copied = input_copied +1;
                size_t position = spread_insert_unsafe(input_keys, input_values, output_keys, output_values, input_copied, action.m_insert_key, action.m_insert_value);
                if(position == 0 && output_keys == xKeys + output_canonical_index + output_offset)
                    set_separator_key(j, action.m_insert_key);
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

//            COUT_DEBUG("cpy1: " << cpy1 << ", elements_to_copy: " << elements_to_copy - cpy1 << ", input_size: " << input_size);

            if(input_size == 0){ // move to the next input segment
                input_segment_id++;
                input_segment_odd = !input_segment_odd;

                if(input_segment_id < m_storage.m_number_segments){ // avoid overflows
                    input_size = ixSizes[input_segment_id];

                    // in case of ::remove(), we might find an empty segment, skip it!
                    if(input_size == 0){
                        assert(!do_insert && "Otherwise we shouldn't see empty segments");
                        input_segment_id++;
                        input_segment_odd = !input_segment_odd; // flip again
                        if(input_segment_id < m_storage.m_number_segments){
                            input_size = ixSizes[input_segment_id];
                            assert(input_size > 0 && "Only a single empty segment should exist...");
                        }
                    }

                    size_t offset = input_segment_odd ? 0 : m_storage.m_segment_capacity - input_size;
                    size_t input_canonical_index = input_segment_id * m_storage.m_segment_capacity;
                    input_keys = ixKeys + input_canonical_index + offset;
                    input_values = ixValues + input_canonical_index + offset;
                }
                assert(input_segment_id <= (m_storage.m_number_segments +1) && "Infinite loop");
            }

            elements_to_copy -= output_copied;
        } while(elements_to_copy > 0);

        // should we insert a new element in this bucket
        if(do_insert && action.m_insert_key < output_keys[-1]){
            auto min = storage_insert_unsafe(j, action.m_insert_key, action.m_insert_value);
            if(min) set_separator_key(j, action.m_insert_key); // update the minimum in the B+ tree
            do_insert = false;
        }

        output_segment_odd = !output_segment_odd; // flip

        // move to the next segment
        partition_state.segment++;
        if(partition_state.segment >= partitions[partition_state.index].m_segments){
            partition_state.index++;
            partition_state.segment = 0;
            if(partition_state.index < partitions.size()){
                size_t cardinality = partitions[partition_state.index].m_cardinality;
                size_t num_segments = partitions[partition_state.index].m_segments;
                partition_state.card_per_segment = cardinality / num_segments;
                partition_state.odd_segments = cardinality % num_segments;
            }
        }
    }

    // if the element hasn't been inserted yet, it means it has to be placed in the last segment
    if(do_insert){
        auto min = storage_insert_unsafe(num_segments -1, action.m_insert_key, action.m_insert_value);
        if(min) set_separator_key(num_segments -1, action.m_insert_key); // update the minimum in the B+ tree
        do_insert = false;
    }

    // Reset the separator keys
    if(/* new number of segments */ num_segments < /* old number of segments */ m_storage.m_number_segments){ // only when decreasing the size of the PMA
        auto& gate = m_locks.get_unsafe()[0];
        assert(m_storage.m_number_segments <= get_segments_per_lock() && "Otherwise this task should have been performed by the global rebalancer");
        for(size_t i = num_segments; i < m_storage.m_number_segments; i++){
            gate.set_separator_key(i, numeric_limits<int64_t>::max());
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

void PackedMemoryArray::spread_local(const RebalancePlan& action){
    COUT_DEBUG("start: " << action.m_window_start << ", length: " << action.m_window_length);
    assert(action.m_window_length <= get_segments_per_lock() && "This operation should have been performed by the RebalancingMaster");

    // workspace
    auto fn_deallocate = [this](void* ptr){ m_memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> input_keys_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    unique_ptr<int64_t, decltype(fn_deallocate)> input_values_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_values = input_values_ptr.get();

    // 1) first copy all elements in input keys
    int64_t insert_position = -1;
    spread_load(action, input_keys, input_values, &insert_position);

//    // debug only
//#if defined(DEBUG)
//    for(size_t i =0; i < action.get_cardinality_after(); i++){
//        cout << "Input [" << i << "] <" << input_keys[i] << ", " << input_values[i] << ">" << endl;
//    }
//#endif

    // 2) detector record
    spread_detector_record detector_record, *ptr_detector_record = nullptr;
    if(action.is_insert()){
        detector_record = spread_create_detector_record(input_keys, action.get_cardinality_after(), insert_position);
        ptr_detector_record = &detector_record;
    }

    // 3) copy the elements from input_keys to the final segments
    const auto& partitions = action.m_apma_partitions;
    size_t segment_id = 0;
    for(size_t i = 0, sz = partitions.size(); i < sz; i++){
        assert(partitions[i].m_segments > 0);
        size_t length = partitions[i].m_cardinality;
        if(partitions[i].m_segments == 1){ // copy a single segment
            spread_save(action.m_window_start + segment_id, input_keys, input_values, length, ptr_detector_record);
        } else {
            spread_save(action.m_window_start + segment_id, partitions[i].m_segments, input_keys, input_values, length, ptr_detector_record);
        }

        input_keys += length;
        input_values += length;
        segment_id += partitions[i].m_segments;

        // adjust the starting offset of the inserted key
        if (ptr_detector_record){
           detector_record.m_position -= length;
           if(detector_record.m_position < 0) ptr_detector_record = nullptr;
        }
    }
    assert(segment_id == action.m_window_length && "Not all segments visited");

    if(action.m_window_length == m_storage.m_number_segments)
        m_detector.clear();
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

void PackedMemoryArray::spread_load(const RebalancePlan& action, int64_t* __restrict keys_to, int64_t* __restrict values_to, int64_t* output_position_key_inserted){
    // insert position
    int64_t position_key_inserted = 0;

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    int64_t* __restrict workspace_keys = m_storage.m_keys + action.m_window_start * m_storage.m_segment_capacity;
    int64_t* __restrict workspace_values = m_storage.m_values + action.m_window_start * m_storage.m_segment_capacity;
    segment_size_t* __restrict workspace_sizes = m_storage.m_segment_sizes + action.m_window_start;

    bool do_insert = action.is_insert();
    int64_t new_key_segment = do_insert ? (action.m_insert_segment - action.m_window_start) : -1;

    for(int64_t i = 1; i < action.m_window_length; i+=2){
        size_t length = workspace_sizes[i -1] + workspace_sizes[i];
        size_t offset = (m_storage.m_segment_capacity * i) - workspace_sizes[i-1];
        int64_t* __restrict keys_from = workspace_keys + offset;
        int64_t* __restrict values_from = workspace_values + offset;

        // destination
        if (new_key_segment == i || new_key_segment == i -1){
            position_key_inserted += spread_insert_unsafe(keys_from, values_from, keys_to, values_to, length, action.m_insert_key, action.m_insert_value);
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
        position_key_inserted += spread_insert_unsafe(nullptr, nullptr, keys_to, values_to, 0, action.m_insert_key, action.m_insert_value);
        if(output_position_key_inserted) *output_position_key_inserted = position_key_inserted;
    }
}

void PackedMemoryArray::spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record){
    assert(cardinality > 0 && "Empty segment");

    int64_t* keys_to = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* values_to = m_storage.m_values + segment_id * m_storage.m_segment_capacity;

    if(segment_id %2  == 0){ // even segment, adjust the base addresses
        keys_to += m_storage.m_segment_capacity - cardinality;
        values_to += m_storage.m_segment_capacity - cardinality;
    }

    memcpy(keys_to, keys_from, sizeof(keys_to[0]) * cardinality);
    memcpy(values_to, values_from, sizeof(values_to[0]) * cardinality);

    set_separator_key(segment_id, keys_from[0]);
    m_storage.m_segment_sizes[segment_id] = cardinality;

    if(detector_record && detector_record->m_position >= 0 && detector_record->m_position < cardinality)
        m_detector.insert(segment_id, detector_record->m_predecessor, detector_record->m_successor);
}

void PackedMemoryArray::spread_save(size_t window_start, size_t window_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record){
    int64_t* __restrict keys_to = m_storage.m_keys + window_start * m_storage.m_segment_capacity;
    int64_t* __restrict values_to = m_storage.m_values + window_start * m_storage.m_segment_capacity;
    uint16_t* __restrict segment_sizes = m_storage.m_segment_sizes + window_start;

    auto card_per_segment = cardinality / window_length;
    auto odd_segments = cardinality % window_length;

    // 1) handle the detector record
    if(detector_record && detector_record->m_position >= 0 && detector_record->m_position < cardinality){
        size_t detector_position = detector_record->m_position;
        size_t odd_segments_threshold = odd_segments * (card_per_segment +1);
        size_t segment_id;
        if(detector_position < odd_segments_threshold){
            segment_id = detector_position / (card_per_segment +1);
        } else {
            detector_position -= odd_segments_threshold;
            segment_id = odd_segments + detector_position / card_per_segment;
        }
        assert(segment_id < window_length && "Incorrect calculus");
        m_detector.insert(window_start + segment_id, detector_record->m_predecessor, detector_record->m_successor);
    }

    // 2) set the segment sizes
    assert((card_per_segment + (odd_segments >0)) <= m_storage.m_segment_capacity && "Segment overfilled");
    for(size_t i = 0; i < odd_segments; i++){
        segment_sizes[i] = card_per_segment +1;
    }
    for(size_t i = odd_segments; i < window_length; i++){
        segment_sizes[i] = card_per_segment;
    }

    // 3) copy the first segment if it's at an odd position
    if(window_start %2 == 1){
        size_t this_card = card_per_segment + odd_segments;
        memcpy(keys_to, keys_from, sizeof(keys_to[0]) * this_card);
        memcpy(values_to, values_from, sizeof(values_to[0]) * this_card);
        set_separator_key(window_start, keys_to[0]);

        window_start++;
        keys_to += m_storage.m_segment_capacity;
        values_to += m_storage.m_segment_capacity;
        keys_from += this_card;
        values_from += this_card;
        window_length--;
        if(odd_segments > 0) odd_segments--;
    }

    // 4) copy the bulk segments
    assert(window_length % 2 == 0 && "Expected an even position");
    for(size_t i = 1; i < window_length; i+=2){
        size_t card_left = card_per_segment + ((i -1) < odd_segments);
        size_t card_right = card_per_segment + ((i) < odd_segments);
        COUT_DEBUG("[bulk] i: " << i << ", card_left: " << card_left << ", card_right: " << card_right);

        size_t length = card_left + card_right;
        size_t offset = i * m_storage.m_segment_capacity - card_left;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        set_separator_key(window_start + i-1,  keys_from[0]);
        set_separator_key(window_start + i,   (keys_from + card_left)[0]);

        memcpy(keys_to_start, keys_from, length * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, length * sizeof(values_from[0]));

        keys_from += length;
        values_from += length;
    }

    // 5) copy the last segment, if it's at an even position
    if(window_length % 2 == 1){
        size_t offset = window_length * m_storage.m_segment_capacity - card_per_segment;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        set_separator_key(window_start + window_length -1, keys_from[0]);

        memcpy(keys_to_start, keys_from, card_per_segment * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, card_per_segment * sizeof(values_from[0]));
    }
}

PackedMemoryArray::spread_detector_record
PackedMemoryArray::spread_create_detector_record(int64_t* keys, int64_t size, int64_t position){
    if(position < 0)
        return {-1, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max() };

    int64_t predecessor = position > 0 ? keys[position -1] : numeric_limits<int64_t>::min();
    int64_t successor = position < (size -1) ? keys[position +1] : numeric_limits<int64_t>::max();

    return {position, predecessor, successor};
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/

int64_t PackedMemoryArray::find(int64_t key) const {
    COUT_DEBUG("key: " << key);
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

    m_detector.dump(out);
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
void PackedMemoryArray::on_destroy_main() {
    set_max_number_workers(0);
}

} // namespace

