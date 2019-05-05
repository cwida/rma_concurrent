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

#include "garbage_collector.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"

using namespace std;
using namespace common;

namespace data_structures::abtree::parallel {

GarbageCollector::GarbageCollector(const ThreadContextList&  list) : GarbageCollector(list, chrono::duration_cast<chrono::milliseconds>(chrono::seconds(1))) { }

GarbageCollector::GarbageCollector(const ThreadContextList&  list, chrono::milliseconds timer_interval) :
        m_thread_contexts(list), m_timer_interval(timer_interval) { }

GarbageCollector::~GarbageCollector() {
    stop();

    // clean up
    for(size_t i = 0, sz = m_items_to_delete.size(); i < sz; i++){
        auto descr = m_items_to_delete[i];
        descr->m_deleter->free(descr->m_pointer);
        delete descr; descr = nullptr;
    }
}

GarbageCollector::DeleteInterface::~DeleteInterface() { }

void GarbageCollector::start(){
    unique_lock<SpinLock> lock(m_mutex);
    if(m_thread_can_execute) RAISE_EXCEPTION(Exception, "Invalid state. The background thread is already running");

    m_thread_can_execute = true;
    barrier();

    m_background_thread = thread(&GarbageCollector::run, this);

    m_condvar.wait(lock, [this](){ return m_thread_is_running; });
}

void GarbageCollector::stop(){
    m_thread_can_execute = false;
    barrier();
    if(m_background_thread.joinable())
        m_background_thread.join(); // wait for the thread to finish
}

// Background thread
void GarbageCollector::run(){
    set_thread_name("GC");

    { // ensure that #notify is invoked only after m_thread_is_running == true
        scoped_lock<SpinLock> lock(m_mutex);
        m_thread_is_running = true;
    }
    m_condvar.notify_one();

    while(m_thread_can_execute){
        std::this_thread::sleep_for(m_timer_interval);
        perform_gc_pass();
    }
    m_thread_is_running = false;
}

void GarbageCollector::perform_gc_pass(){

    // current epoch
    auto epoch = m_thread_contexts.min_epoch();
    vector<Item*> items;
    items.reserve(/* magic number */ 64);
    {  // restrict the scope
        lock_guard<SpinLock> lock(m_mutex);
        for(uint64_t i = 0, sz = m_items_to_delete.size(); i < sz; i++){
            if(m_items_to_delete[0]->m_timestamp > epoch) break; // done
            items.push_back(m_items_to_delete[0]);
            m_items_to_delete.pop();
        }
    }

    // remove the objects identified for deletion
    for(auto& item : items){
        item->m_deleter->free(item->m_pointer);
        delete item; item = nullptr;
    }

}

void GarbageCollector::dump(std::ostream& out) const {
    auto current_epoch = m_thread_contexts.min_epoch();

    scoped_lock<SpinLock> lock(m_mutex);
    out << "[GarbageCollector] min epoch: " << current_epoch << ", # items: " << m_items_to_delete.size();
    if(m_items_to_delete.empty()){
        out << " -- empty";
    } else {
        out << ": ";
        for(size_t i = 0; i < m_items_to_delete.size(); i++){
            if(i > 0) out << ", ";
            Item* item = m_items_to_delete[i];
            out << "{epoch: " << item->m_timestamp << ", pointer: " << item->m_pointer << "}";
        }
    }

    out << "\n";
}

void GarbageCollector::dump() const{
    dump(cout);
}

} // namespace abtree:parallel
