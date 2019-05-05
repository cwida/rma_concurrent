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

#include "timer_manager.hpp"

#include <event2/event.h>
#include <mutex>

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"
#include "packed_memory_array.hpp"

using namespace common;
using namespace std;

namespace data_structures::rma::batch_processing {

#define ERROR(msg) RAISE_EXCEPTION(Exception, msg)

// Callback to retrieve all pending events stored in the libevent's queue
static int collect_events(const struct event_base*, const struct event* event,  void*  /* std::vector<struct event*>* */vector_events); // forward decl

// synchronisation to start/stop the background thread
static std::mutex g_mutex;
static std::condition_variable g_condvar;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[TimerManager::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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

TimerManager::TimerManager(PackedMemoryArray* instance) : m_instance(instance), m_queue(nullptr), m_eventloop_exec(false) {
    libevent_init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");

}

TimerManager::~TimerManager(){
    stop();
    event_base_free(m_queue); m_queue = nullptr;
    libevent_shutdown();
}

void TimerManager::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(g_mutex);
    if(m_background_thread.joinable()) ERROR("Invalid state. The background thread is already running");

    auto timer = duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &TimerManager::callback_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");

    m_background_thread = thread(&TimerManager::main_thread, this);

    g_condvar.wait(lock, [this](){ return m_eventloop_exec; });
    COUT_DEBUG("Started");
}

void TimerManager::callback_start(int fd, short flags, void* event_argument){
    COUT_DEBUG("Event loop started");

    TimerManager* instance = reinterpret_cast<TimerManager*>(event_argument);
    instance->m_eventloop_exec = true;
    g_condvar.notify_all();
}

void TimerManager::stop(){
    COUT_DEBUG("Stopping...");
    scoped_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) return;
    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_background_thread.join();

    // remove all enqueued events still in the queue
    std::vector<struct event*> pending_events;
    rc = event_base_foreach_event(m_queue, collect_events, &pending_events);
    if(rc != 0) ERROR("event_base_foreach_event");
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        free(event_get_callback_arg(e));
        event_free(e);
    }

    COUT_DEBUG("Stopped");
}

/*****************************************************************************
 *                                                                           *
 *   Execution thread                                                        *
 *                                                                           *
 *****************************************************************************/
void TimerManager::main_thread(){
    COUT_DEBUG("Service thread started");
    set_thread_name("Timer Manager");

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ cerr << "[TimerManager::main_thread] event_base_loop rc: " << rc << endl; }

    COUT_DEBUG("Service thread stopped");
}

/*****************************************************************************
 *                                                                           *
 *   Worker API                                                              *
 *                                                                           *
 *****************************************************************************/
void TimerManager::delay_rebalance(uint64_t gate_id, chrono::steady_clock::time_point time_last_rebal, std::chrono::microseconds when_to_rebalance){
    COUT_DEBUG("gate: " << gate_id << ", delay the rebalance of " << when_to_rebalance.count() << " microseconds");

    assert(m_background_thread.joinable() && "The service is not running");
    TimeoutEvent* event_payload = (TimeoutEvent*) malloc(sizeof(TimeoutEvent));
    assert(event_payload != nullptr && "cannot allocate the timer event");
    if(event_payload == nullptr) throw std::bad_alloc{};

    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_invoke, event_payload);
    if(event == nullptr) throw std::bad_alloc{};

    // the payload to associate to the event
    event_payload->m_event = event;
    event_payload->m_instance = m_instance;
    event_payload->m_gate_id = gate_id;
    event_payload->m_time_last_rebal = time_last_rebal;

    // time when the event should be invoked
    struct timeval timer = duration2timeval(when_to_rebalance);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: delay_rebalance, event_add failed");
        std::abort(); // not sure what we can do here
    }
}

// static method, trampoline to PMA::timeout(gate_id, time_last_rebal)
void TimerManager::callback_invoke(evutil_socket_t /* fd == -1 */, short /* flags */, void* argument){
    assert(argument != nullptr && "Invalid pointer");

    TimeoutEvent* event = reinterpret_cast<TimeoutEvent*>(argument);
    PackedMemoryArray* instance = event->m_instance;
    uint64_t gate_id = event->m_gate_id;
    auto time_last_rebal = event->m_time_last_rebal;

    // Release the memory associated to the event
    event_free(event->m_event); event->m_event = nullptr;
    free(event); event = nullptr;

    // Jump to the implementation of the timeout
    COUT_DEBUG("timeout, gate_id: " << gate_id);
    instance->timeout(gate_id, time_last_rebal);
}

/*****************************************************************************
 *                                                                           *
 *   Flush                                                                   *
 *                                                                           *
 *****************************************************************************/

static int collect_events(const struct event_base*, const struct event* event,  void*  /* std::vector<struct event*>* */ argument){
    // event_get_events: bad naming, it retrieves the flags associated to an event.
    // The guard should protect against
    if(event_get_events(event) == EV_TIMEOUT){
        auto vector_elements = reinterpret_cast<std::vector<struct event*>*>(argument);
        vector_elements->push_back(const_cast<struct event*>(event));
    }
    return 0;
}

void TimerManager::callback_flush(int fd, short flags, void* /* FlushEvent */ argument_untyped){
    FlushEvent* flush_event = reinterpret_cast<FlushEvent*>(argument_untyped);
    vector<struct event*> pending_events;

    int rc = event_base_foreach_event(flush_event->m_queue, collect_events, &pending_events);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: delay_rebalance, event_add failed");
        std::abort(); // not sure what we can do here
    }

    COUT_DEBUG("Pending events: " << pending_events.size());

    for(auto e : pending_events){
        event_del(e); // make the event non pending, it will be freed by the callback
        auto cb = event_get_callback(e);
        auto arg = event_get_callback_arg(e);
        cb(-1, EV_TIMEOUT, arg);
    }

    // notify the invoking thread
   flush_event->m_producer->set_value();

   // release the memory associated to the event
   event_free(flush_event->m_event);
   free(flush_event);
   flush_event = nullptr;
}

void TimerManager::flush(){
    COUT_DEBUG("Flushing the queue of events");
    assert(m_background_thread.joinable() && "The service is not running");

    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();

    FlushEvent* event_payload = (FlushEvent*) malloc(sizeof(FlushEvent));
    if(event_payload == nullptr) throw std::bad_alloc{};

    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, TimerManager::callback_flush, event_payload);
    if(event == nullptr) throw std::bad_alloc{};
    event_payload->m_event = event;
    event_payload->m_queue = m_queue;
    event_payload->m_producer = &producer;

    // activate it immediately
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;

    int rc = event_add(event, &timeout);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: flush, event_add failed");
        std::abort(); // not sure what we can do here
    }

    consumer.wait(); // Zzz

    COUT_DEBUG("Done");
}

} // namespace
