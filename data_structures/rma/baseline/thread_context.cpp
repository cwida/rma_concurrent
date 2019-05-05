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

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "packed_memory_array.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::baseline {

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
    if(size < m_size){
        for(size_t i = size; i < m_size; i++){
            bool locked = m_contexts[i].m_mutex.try_lock();
            if(!locked){
                COUT_DEBUG("Context " << i << " still locked. Retrying in one second...");
                this_thread::sleep_for(1s);
                locked = m_contexts[i].m_mutex.try_lock();
                assert(locked && "Cannot acquire the lock for the related context => Context still busy");
                if(!locked){ RAISE_EXCEPTION(Exception, "The thread context " << i << " is still busy"); }
            }
            bool is_busy = m_contexts[i].busy();
            m_contexts[i].m_mutex.unlock();

            assert(!is_busy && "Context busy");
            if(is_busy) { RAISE_EXCEPTION(Exception, "Cannot remove the context " << i << ", still busy"); }
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

ThreadContext::ThreadContext() : m_timestamp(numeric_limits<uint64_t>::max()), m_hosted(false), m_spurious_wake(false) { }

ThreadContext::~ThreadContext() {
    assert(!busy());
}

void ThreadContext::enter(){
    COUT_DEBUG("Entry");
    if(m_hosted == true) RAISE_EXCEPTION(Exception, "Context already hosted");
    m_hosted = true;
    m_mutex.lock();
}

void ThreadContext::exit(){
    COUT_DEBUG("Exit [thread_id: " << m_thread_id << "]");
    if(!m_hosted) RAISE_EXCEPTION(Exception, "Already unregistered");
    m_hosted = false;
    m_mutex.unlock();
}

bool ThreadContext::busy() const {
    return m_hosted || m_spurious_wake;
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

void ThreadContext::wait() {
    m_spurious_wake = true;
    m_condition_variable.wait(m_mutex, [this]{ return !m_spurious_wake; });
}

void ThreadContext::notify(){
    m_mutex.lock();
    m_spurious_wake = false;
    m_condition_variable.notify_one();
    m_mutex.unlock();
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
