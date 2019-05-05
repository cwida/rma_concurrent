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

#include "gate.hpp"

#include <cstdlib>
#include <iostream> // debug only
#include <limits>
#include <mutex> // debug only
#include <new>
#include <thread> // debug only

#include "thread_context.hpp"

using namespace std;

namespace data_structures::rma::one_by_one {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[Gate::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
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
Gate::Gate(uint32_t window_start, uint32_t window_length) : m_window_start(window_start), m_window_length(window_length), m_queue(/* initial capacity */ 2) {
    m_num_active_threads = 0;
    m_cardinality = 0;
    m_fence_low_key = m_fence_high_key = numeric_limits<int64_t>::min();
    m_separator_keys = nullptr; // needs to be set eventually
    m_writer = nullptr;
}

Gate* Gate::allocate(uint64_t num_locks, uint64_t segments_per_lock){
    assert(num_locks > 0 && segments_per_lock > 0);
    if(num_locks == 0 || segments_per_lock == 0) return nullptr;

    size_t space_per_gate = sizeof(Gate) + (segments_per_lock -1) * sizeof(int64_t);
    Gate* array_gates = (Gate*) malloc(space_per_gate * num_locks);
    if(array_gates == nullptr) throw std::bad_alloc();
    int64_t* __restrict array_separator_keys = reinterpret_cast<int64_t*>(array_gates + num_locks);

    int64_t* separator_keys = array_separator_keys;
    for(uint64_t i = 0; i < num_locks; i++){
        new( array_gates + i ) Gate{ static_cast<uint32_t>(i * segments_per_lock), static_cast<uint32_t>(segments_per_lock) };
        array_gates[i].m_separator_keys = separator_keys;
        separator_keys += (segments_per_lock -1);
    }

    // only for the separator keys in the first extent
    for(int64_t i = 0, end = segments_per_lock -1; i < end; i++){
        array_separator_keys[i] = std::numeric_limits<int64_t>::max();
    }

    // update the fence key for the last extent
    array_gates[num_locks -1].m_fence_high_key = numeric_limits<int64_t>::max();

    return array_gates;
}

void Gate::deallocate(Gate* gates){
    free(gates);
}

/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/

void Gate::set_separator_key(size_t segment_id, int64_t key){
    assert(segment_id >= m_window_start && segment_id < m_window_start + m_window_length);
    if(segment_id > m_window_start){
        m_separator_keys[segment_id - m_window_start -1] = key;
    }
}

Gate::Direction Gate::check_fence_keys(int64_t key) const {
    if(m_fence_high_key == std::numeric_limits<int64_t>::min())  // this array is not valid anymore, restart the operation
        return Direction::INVALID;
    else if(key < m_fence_low_key)
        return Direction::LEFT;
    else if(key > m_fence_high_key)
        return Direction::RIGHT;
    else
        return Direction::GO_AHEAD;
}

void Gate::wake_next(WakeList& wake_list) {
    COUT_DEBUG("gate id: " << gate_id());
    assert(m_locked && "To invoke this method the internal lock must be acquired first");

    if(m_queue.empty()) {
        return;
    } else if(m_queue[0].m_purpose == State::WRITE){
        wake_list.m_list_workers.push_back(m_queue[0].m_promise);
        m_queue.pop();
    } else {
        assert(m_queue[0].m_purpose == State::READ);
        do {
            std::promise<void>* producer = m_queue[0].m_promise;
            producer->set_value(); // notify
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::READ);
    }
}

void Gate::wake_next(ThreadContext* context) {
    assert(context != nullptr && "Null ptr");
    wake_next(context->m_wakelist);
}

void Gate::wake_all(WakeList& wake_list){
    COUT_DEBUG("gate id: " << gate_id());
    assert((m_locked || m_state == State::REBAL) && "To invoke this method the internal lock must be acquired first");

    while(!m_queue.empty()){
        m_queue[0].m_promise->set_value(); // notify
        m_queue.pop();
    }
}

} // namespace
