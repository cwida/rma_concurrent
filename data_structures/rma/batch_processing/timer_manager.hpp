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

#include <chrono>
#include <future>
#include <thread>

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace data_structures::rma::batch_processing {

class PackedMemoryArray; // forward declaration

/**
 * A service thread to create and handle multiple timers. When a timer expires, the service invokes pma->timeout(gate_id, time_last_rebal) to
 * issue a global rebalance of the specified gate_id.
 */
class TimerManager {
    PackedMemoryArray* m_instance; // pma instance associated to this TimerManager
    struct event_base* m_queue; // libevent's queue
    std::thread m_background_thread; // handle to the background thread
    bool m_eventloop_exec; // true when the service thread is running the event loop

    // The events enqueued
    struct BaseEvent {  struct event* m_event; }; // base class, every event enqueued must contain a pointer to the libevent's event
    struct TimeoutEvent : public BaseEvent { // timeout event
        PackedMemoryArray* m_instance; // the instance owning this queue
        uint64_t m_gate_id; // the id of the gate to rebalance
        std::chrono::steady_clock::time_point m_time_last_rebal; // the time the gate was last rebalanced
    };
    struct FlushEvent : public BaseEvent { // flush event
        struct event_base* m_queue; // the libevent's queue handle
        std::promise<void>* m_producer; // the thread to notify once the flush has been completed
    };

protected:
    // Method executed by the background thread, it runs the event loop
    void main_thread();

    // Notify the thread that started the service thread that the event loop is running
    static void callback_start(int fd, short flags, void* /* TimerManager instance */ event_argument);

    // The callback invoked by the libevent event loop, trampoline to `handle_callback'
    static void callback_invoke(int fd, short flags, void* /* TimerEvent */  event_argument);

    // Execute all pending events in the libevent queue
    static void callback_flush(int fd, short flags, void* /* FlushEvent */ event_argument);

public:
    // Constructor
    // @param instance the pma instance associated to this service
    TimerManager(PackedMemoryArray* instance);

    // Destructor
    ~TimerManager();

    // Start the service / background thread
    void start();

    // Stop the service / background thread
    void stop();

    // Schedule a new delayed rebalance
    void delay_rebalance(uint64_t gate_id, std::chrono::steady_clock::time_point time_last_rebal, std::chrono::microseconds when_to_rebalance);

    // Synchronously flush all events to complete
    void flush();
};


} // namespace
