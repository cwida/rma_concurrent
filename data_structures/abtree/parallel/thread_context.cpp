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

#include <cassert>
#include <mutex>

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"

using namespace std;
using namespace common;

namespace data_structures::abtree::parallel {

/*****************************************************************************
 *                                                                           *
 *   ThreadContext                                                           *
 *                                                                           *
 *****************************************************************************/
ThreadContext::ThreadContext() : m_epoch(numeric_limits<uint64_t>::max()) { }
ThreadContext::~ThreadContext(){ bye(); }
void ThreadContext::hello(){ m_epoch = rdtscp(); }
void ThreadContext::bye(){ m_epoch = numeric_limits<uint64_t>::max(); }
thread_local int ThreadContext::thread_id { -1 };

/*****************************************************************************
 *                                                                           *
 *   ThreadContextList                                                       *
 *                                                                           *
 *****************************************************************************/
ThreadContextList::ThreadContextList() { }
ThreadContextList::~ThreadContextList() { resize(0); }

void ThreadContextList::resize(uint64_t size){
    scoped_lock<SpinLock> lock(m_spinlock); // this is going to be used only by the garbage collector, it's okay to lock here
    if(size > m_capacity){ RAISE_EXCEPTION(Exception, "Given size (" << size << ") greater than the maximum capacity: " << m_capacity); }
    m_size = size;
}

ThreadContext* ThreadContextList::operator[](uint64_t index) const {
    if(index >= m_size){ RAISE_EXCEPTION(Exception, "Invalid index: " << index << ", size: " << m_size); }
    return const_cast<ThreadContext*>(m_contexts + index);
}

uint64_t ThreadContextList::min_epoch() const {
    uint64_t min_epoch = numeric_limits<uint64_t>::max();
    scoped_lock<SpinLock> lock(m_spinlock); // this is going to be used only by the garbage collector, it's okay to lock here
    for(uint64_t i =0; i < m_size; i++){
        min_epoch = std::min(min_epoch, m_contexts[i].epoch());
    }
    return min_epoch;
}

ThreadContext& ThreadContextList::my_context() const {
    if(ThreadContext::thread_id < 0) { RAISE_EXCEPTION(Exception, "ThreadContext::thread_id not set: " << ThreadContext::thread_id); }
    return const_cast<ThreadContext&>(m_contexts[ThreadContext::thread_id]);
}

/*****************************************************************************
 *                                                                           *
 *   ScopedContext                                                           *
 *                                                                           *
 *****************************************************************************/
ScopedContext::ScopedContext(const ThreadContextList& list) : m_context(nullptr) {
    if(ThreadContext::thread_id < 0) { RAISE_EXCEPTION(Exception, "ThreadContext::thread_id not set: " << ThreadContext::thread_id); }
    m_context = list[ThreadContext::thread_id];
    tick();
}
ScopedContext::~ScopedContext(){ context().bye(); }
ThreadContext& ScopedContext::context(){ return *m_context; }
void ScopedContext::tick(){ context().hello(); }

} // namespace data_structures::abtree::parallel
