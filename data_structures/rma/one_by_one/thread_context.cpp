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

#include "thread_context.hpp"

#include <algorithm>
#include <iostream>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "common/spin_lock.hpp"
#include "packed_memory_array.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::one_by_one {

thread_local int ThreadContext::m_thread_id;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[ThreadContext::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   ThreadContextList                                                       *
 *                                                                           *
 *****************************************************************************/
ThreadContextList::ThreadContextList() {

}

ThreadContextList::~ThreadContextList() {
    resize(0);
}

void ThreadContextList::resize(uint64_t size){
    scoped_lock<mutex> lock(m_mutex); // this is going to be used only by the garbage collector, it's okay to lock here
    if(size > m_capacity){ RAISE_EXCEPTION(Exception, "Given size (" << size << ") greater than the maximum capacity: " << m_capacity); }
    if(size < m_size){ // assuming this is invoked on termination
        for(size_t i = size; i < m_size; i++){
            if(m_contexts[i].busy()){
                COUT_DEBUG("Context " << i << " still locked. Retrying in one second...");
                this_thread::sleep_for(1s);

                if(m_contexts[i].busy()){
                    RAISE_EXCEPTION(Exception, "Cannot remove the context " << i << ", still busy");
                }
            }
        }
    }
    m_size = size;
}

ThreadContext* ThreadContextList::operator[](uint64_t index) const {
    if(index >= m_size){ RAISE_EXCEPTION(Exception, "Invalid index: " << index << ", size: " << m_size); }
    return const_cast<ThreadContext*>(m_contexts + index);
}

uint64_t ThreadContextList::min_epoch() const {
    uint64_t min_epoch = numeric_limits<uint64_t>::max();
    scoped_lock<mutex> lock(m_mutex); // this is going to be used only by the garbage collector, it's okay to lock here
    for(uint64_t i =0; i < m_size; i++){
        min_epoch = std::min(min_epoch, m_contexts[i].epoch());
    }
    return min_epoch;
}

/*****************************************************************************
 *                                                                           *
 *   ThreadContext                                                           *
 *                                                                           *
 *****************************************************************************/

ThreadContext::ThreadContext() : m_timestamp(numeric_limits<uint64_t>::max()), m_hosted(false), m_has_update(false), m_queue_next(16) {

}

ThreadContext::~ThreadContext() {
    assert(!busy());
}

void ThreadContext::enter(){
    COUT_DEBUG("Entry");
    if(m_hosted == true) RAISE_EXCEPTION(Exception, "Context already hosted");
    m_hosted = true;
//    m_mutex.lock();
}

void ThreadContext::exit(){
    COUT_DEBUG("Exit [thread_id: " << m_thread_id << "]");
    if(!m_hosted) RAISE_EXCEPTION(Exception, "Already unregistered");
    m_hosted = false;
//    m_mutex.unlock();
}

bool ThreadContext::busy() const {
    return m_hosted;
}

void ThreadContext::hello() noexcept {
    m_timestamp = rdtscp(); // cpu clock
}

void ThreadContext::bye() noexcept {
    m_timestamp = numeric_limits<uint64_t>::max();
}

uint64_t ThreadContext::epoch() const noexcept {
    return m_timestamp;
}

void ThreadContext::register_thread(int thread_id){
    m_thread_id = thread_id;
}

void ThreadContext::unregister_thread(){
    m_thread_id = -1;
}

int ThreadContext::thread_id() {
    return m_thread_id;
}

bool ThreadContext::enqueue(const ThreadContext::Update& update){
    scoped_lock<SpinLock> lock(m_queue_mutex);
//    if(m_queue_next.size() < m_queue_next.capacity()){
        m_queue_next.append(update);
        return true;
//    } else {
//        return false;
//    }
}

void ThreadContext::fetch_local_queue() noexcept {
    scoped_lock<SpinLock> lock(m_queue_mutex);
    fetch_local_queue_unsafe();
}

void ThreadContext::fetch_local_queue_unsafe() noexcept {
    if(m_queue_next.empty()){
        m_has_update = false;
    } else {
        m_has_update = true;
        m_current_update = m_queue_next[0];
        m_queue_next.pop();
    }
}

void ThreadContext::set_update(bool is_insertion, int64_t key, int64_t value) noexcept{
    assert(m_has_update == false && "An operation is already set to be performed");
    assert(m_queue_next.empty() && "There are still operations in queue");
    m_has_update = true;
    m_current_update = Update{is_insertion, key, value};
}

::std::ostream& operator<<(::std::ostream& out, const ThreadContext& context){
    out << "[ThreadContext thread_id: " << ThreadContext::m_thread_id << ", timestamp: " << context.m_timestamp << ", "
            "hosted: " << (context.m_hosted ? "yes" : "no");
    if(context.m_has_update){
        out << ", current operation: " << context.m_has_update;
    }

    { // critical section
        scoped_lock<SpinLock> lock(context.m_queue_mutex);
        if(context.m_queue_next.empty()){
            out << ", queue empty";
        } else {
            out << ", queue next:\n";
        }
        for(size_t i = 0; i < context.m_queue_next.size(); i++){
            out << "[" << i << "] " << context.m_queue_next[i] << "\n";
        }
    }

    out << "}";
    return out;
}

::std::ostream& operator<<(::std::ostream& out, const ThreadContext* context){
    if(context != nullptr){
        out << *context;
    } else {
        out << "[ThreadContext nullptr]";
    }
    return out;
}

::std::ostream& operator<<(::std::ostream& out, const ThreadContext::Update& operation){
    if(operation.m_is_insert){
        out << "<(I) key: " << operation.m_key << ", value: " << operation.m_value << ">";
    } else {
        out << "<(D) key: " << operation.m_key << ">";
    }
    return out;
}


/*****************************************************************************
 *                                                                           *
 *   ScopedState                                                             *
 *                                                                           *
 *****************************************************************************/

ScopedState::ScopedState(const PackedMemoryArray* pma) : ScopedState(pma->get_context()){
    assert(pma != nullptr);
}

ScopedState::ScopedState(ThreadContext* context) : m_context(context){
    assert(m_context != nullptr);
    context->hello();
}

ScopedState::~ScopedState(){
    assert(m_context != nullptr);
    m_context->bye();
};

} // namespace
