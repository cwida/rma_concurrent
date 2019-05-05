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
#include "rma/common/bitset.hpp"
#include "packed_memory_array.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::batch_processing {


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
    if(size > m_context_clients_capacity){ RAISE_EXCEPTION(Exception, "Given size (" << size << ") greater than the maximum capacity: " << m_context_clients_capacity); }
    if(size < m_context_clients_size){ // assuming this is invoked on termination
        for(size_t i = size; i < m_context_clients_size; i++){
            if(m_context_clients[i].busy()){
                COUT_DEBUG("Context " << i << " still locked. Retrying in one second...");
                this_thread::sleep_for(1s);

                if(m_context_clients[i].busy()){
                    RAISE_EXCEPTION(Exception, "Cannot remove the context " << i << ", still busy");
                }
            }
        }
    }
    m_context_clients_size = size;
}

ClientContext* ThreadContextList::operator[](uint64_t index) const {
    if(index >= m_context_clients_size){ RAISE_EXCEPTION(Exception, "Invalid index: " << index << ", size: " << m_context_clients_size); }
    return const_cast<ClientContext*>(m_context_clients + index);
}

ThreadContext* ThreadContextList::timer_manager() const {
    return const_cast<ThreadContext*>(&m_context_timer_manager);
}

uint64_t ThreadContextList::min_epoch() const {
    uint64_t min_epoch = numeric_limits<uint64_t>::max();
    scoped_lock<mutex> lock(m_mutex); // this is going to be used only by the garbage collector, it's okay to lock here
    for(uint64_t i =0; i < m_context_clients_size; i++){
        min_epoch = std::min(min_epoch, m_context_clients[i].epoch());
    }
    min_epoch = std::min(min_epoch, m_context_timer_manager.epoch());
    return min_epoch;
}

/*****************************************************************************
 *                                                                           *
 *   ThreadContext                                                           *
 *                                                                           *
 *****************************************************************************/
ThreadContext::ThreadContext() : m_timestamp(numeric_limits<uint64_t>::max()) {

}

ThreadContext::~ThreadContext() {

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

::std::ostream& operator<<(::std::ostream& out, const ThreadContext& context){
    out << "epoch: " << context.epoch();
    return out;
}

::std::ostream& operator<<(::std::ostream& out, const ThreadContext* context){
    if(context == nullptr){
        out << "nullptr";
    } else {
        out << *context;
    }
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   ClientContext                                                           *
 *                                                                           *
 *****************************************************************************/

thread_local int ClientContext::m_thread_id = -1;

ClientContext::ClientContext() : m_hosted(false), m_local(nullptr), m_spare(nullptr) {

}

ClientContext::~ClientContext() {
    assert(!busy());

    delete m_bitset; m_bitset = nullptr;
}

void ClientContext::enter(){
    COUT_DEBUG("Entry");
    if(m_hosted == true) RAISE_EXCEPTION(Exception, "Context already hosted");
    m_hosted = true;
    m_local = new ClientContextQueue();
    m_spare = new ClientContextQueue();
}

void ClientContext::exit(){
    COUT_DEBUG("Exit [thread_id: " << m_thread_id << "]");
    if(!m_hosted) RAISE_EXCEPTION(Exception, "Already unregistered");
    m_hosted = false;
    delete m_local; m_local = nullptr;
    delete m_spare; m_spare = nullptr;
}

bool ClientContext::busy() const {
    return m_hosted;
}


void ClientContext::register_client_thread(int thread_id){
    m_thread_id = thread_id;
}

void ClientContext::unregister_client_thread(){
    m_thread_id = -1;
}

int ClientContext::thread_id() {
    return m_thread_id;
}

::std::ostream& operator<<(::std::ostream& out, const ClientContext& context){
    out << "[ClientContext thread_id: " << ClientContext::m_thread_id << ", timestamp: " << context.epoch() << ", "
            "hosted: " << (context.m_hosted ? "yes" : "no") << ", local queue: " << context.queue_local() << ", spare/global queue: " << context.queue_spare() << "]";
    return out;
}

::std::ostream& operator<<(::std::ostream& out, const ClientContext* context){
    if(context != nullptr){
        out << *context;
    } else {
        out << "[ClientContext nullptr]";
    }
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   ClientContextQueue                                                      *
 *                                                                           *
 *****************************************************************************/

::std::ostream& operator<<(::std::ostream& out, const ClientContextQueue& queue){
    if(queue.empty()){
        out << "empty";
    } else {
        out << "{ ";
        if(queue.insertions().empty()){
            out << "insertions empty";
        } else {
            out << "insertions (" << queue.insertions().size() << "): ";
            bool first = true;
            for(auto p : queue.insertions()){
                if(!first) out << ", "; else first = false;
                out << "<" << p.first << ", " << p.second << ">";
            }
        }
        out << "; ";
        if(queue.deletions().empty()){
            out << "deletions empty";
        } else {
            out << "deletions (" << queue.deletions().size() << "): ";
            bool first = true;
            for(auto d : queue.deletions()){
                if(!first) out << ", "; else first = false;
                out << d;
            }
        }
        out << " }";
    }
    return out;
}

::std::ostream& operator<<(::std::ostream& out, const ClientContextQueue* queue){
    if(queue == nullptr){
        out << "nullptr";
    } else {
        out << *queue;
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
