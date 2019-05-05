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

#include "iterator.hpp"

#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>

#include "rma/common/abort.hpp"
#include "gate.hpp"
#include "packed_memory_array.hpp"
#include "parallel.hpp"
#include "rebalancing_master.hpp"
#include "thread_context.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::baseline {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[Iterator::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
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

Iterator::Iterator(const PackedMemoryArray* pma, int64_t min, int64_t max) : m_pma(pma), m_min(min), m_max(max){
    restart();
    set_offset();
}

Iterator::~Iterator(){
    if(m_gate != nullptr)
        release_lock();

    m_pma->get_context()->bye();
}

/*****************************************************************************
 *                                                                           *
 *   Concurrency                                                             *
 *                                                                           *
 *****************************************************************************/

void Iterator::restart(){
    assert(m_gate == nullptr && "Need to release the acquired lock first");
    bool done = false;
    do {
        try {
            auto context = m_pma->get_context();
            context->hello();
            auto gate_id = m_pma->m_locks.get(context)->find(m_min);
            acquire_lock(gate_id);
            done = true;
        } catch (data_structures::rma::common::Abort) { /* retry  */ };
    } while (!done);
}

void Iterator::acquire_lock(uint64_t gate_id){
    assert(m_gate == nullptr && "Need to release the acquired lock first");
    ThreadContext* context = m_pma->get_context();
    assert(context != nullptr && "No thread context available");

    bool done = false;
    do { // enter in the protected area
        Gate* gates = m_pma->m_locks.get(context);
        // enter in the private section
        auto& gate = gates[gate_id];
        unique_lock<Gate> lock(gate);
        // is this the right gate ?
        if(m_pma->check_fence_keys(gate, /* in/out */ gate_id, m_min)){
            switch(gate.m_state){
            case Gate::State::FREE:
                assert(gate.m_num_active_threads == 0 && "Precondition not satisfied");
                gate.m_state = Gate::State::READ;
                gate.m_num_active_threads = 1;
                lock.unlock();

                m_gate = gates + gate_id;
                done = true; // done, proceed with the insertion
                break;
            case Gate::State::READ:
                if(gate.m_queue.empty()){ // as above
                    gate.m_num_active_threads++;
                    lock.unlock();
                    m_gate = gates + gate_id;
                    done = true;
                } else {
                    gate.m_queue.append({ Gate::State::READ, context } );
                    lock.unlock();
                    context->wait();
                }
                break;
            case Gate::State::WRITE:
            case Gate::State::REBAL:
                // add the thread in the queue
                gate.m_queue.append({ Gate::State::READ, context } );
                lock.unlock();
                context->wait();
            }
        }
    } while(!done);
}

void Iterator::release_lock(){
    bool send_message_to_rebalancer = false;

    assert(m_gate != nullptr && "Gate already released");
    m_gate->lock();
    assert(m_gate->m_num_active_threads > 0 && "This reader should have been registered");
    m_gate->m_num_active_threads--;
    if(m_gate->m_num_active_threads == 0){
       switch(m_gate->m_state){
       case Gate::State::READ: { // as before
           m_gate->m_state = Gate::State::FREE;
           m_gate->wake_next();
       } break;
       case Gate::State::REBAL: {
           send_message_to_rebalancer = true;
       } break;
       default:
           assert(0 && "Invalid state");
       }
    }
    m_min = m_gate->m_fence_high_key +1; // next restarting point
    m_gate->unlock();
    m_gate = nullptr;

    if(send_message_to_rebalancer){
        m_pma->m_rebalancer->exit(m_gate->lock_id());
    }
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
void Iterator::set_offset(){
    assert(m_gate != nullptr && "Gate not acquired");
    m_last = m_max <= m_gate->m_fence_high_key;
    auto segment_id = m_gate->find(m_min);
    set_offset(segment_id);
}

void Iterator::set_offset(uint64_t segment_id){
    if(segment_id % 2 == 0){ // even
        m_offset = (segment_id +1) * m_pma->m_storage.m_segment_capacity - m_pma->m_storage.m_segment_sizes[segment_id];
    } else { // odd
        m_offset = (segment_id) * m_pma->m_storage.m_segment_capacity;
    }
    auto stop_segment_id = (segment_id /2) *2 +1; // odd segment
    m_stop = stop_segment_id * m_pma->m_storage.m_segment_capacity + m_pma->m_storage.m_segment_sizes[stop_segment_id] -1; // inclusive

    int64_t* __restrict keys = m_pma->m_storage.m_keys;
    while(m_offset <= m_stop && keys[m_offset] < m_min){
        m_offset++;
    }
    if(m_last){
        while(m_offset <= m_stop && keys[m_stop] > m_max){
            m_stop--;
        }
    }
}

void Iterator::fetch_next_chunk(){
    assert(m_offset > m_stop && "Invalid position");

    auto next_segment_id = (m_stop / m_pma->m_storage.m_segment_capacity) +1;
    if(next_segment_id % 2 == 1) return; // it means the stop offset has been moved from its fixed position due to reaching the maximum of the interval
    if(next_segment_id > m_pma->m_storage.m_number_segments) return; // depleted
    if(next_segment_id % m_pma->get_segments_per_lock() == 0){
        // move to the next lock
        release_lock();

        auto gate_id = next_segment_id / m_pma->get_segments_per_lock();
        try { acquire_lock(gate_id); } catch (data_structures::rma::common::Abort) { }
        if(m_gate == nullptr) { restart(); }

        set_offset();
    } else {
        set_offset(next_segment_id);
    }
}

bool Iterator::hasNext() const {
    return ::data_structures::global_parallel_scan_enabled && m_offset <= m_stop;
}

pair<int64_t, int64_t> Iterator::next(){
    int64_t* keys = m_pma->m_storage.m_keys;
    int64_t* values = m_pma->m_storage.m_values;

    pair<int64_t, int64_t> result { keys[m_offset], values[m_offset] };

    m_offset++;
    if(m_offset > m_stop) fetch_next_chunk();

    return result;
}

} // baseline
