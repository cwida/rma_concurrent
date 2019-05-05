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

#include "rebalancing_pool.hpp"

#include <cassert>
#include "rebalancing_worker.hpp"

using namespace std;

namespace data_structures::rma::one_by_one {

extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingPool::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


RebalancingPool::RebalancingPool(uint64_t num_workers) : m_num_workers_active(0){
    // create the pool
    m_workers_idle.reserve(num_workers);
    for(size_t i = 0; i < num_workers; i++){
        m_workers_idle.push_back(new RebalancingWorker());
    }
}

RebalancingPool::~RebalancingPool() {
    stop();

    for(size_t i = 0; i < m_workers_idle.size(); i++){
        delete m_workers_idle[i]; m_workers_idle[i] = nullptr;
    }
}

void RebalancingPool::start(){
    COUT_DEBUG("Starting...");
    scoped_lock<mutex> lock(m_mutex);
    for(size_t i = 0; i < m_workers_idle.size(); i++){
        m_workers_idle[i]->start();
    }
}

void RebalancingPool::stop(){
    COUT_DEBUG("Stopping...");
    scoped_lock<mutex> lock(m_mutex);
    assert(m_num_workers_active == 0 && "Cannot invoke this method yet, as there are still workers that need to completed");
    for(size_t i = 0; i < m_workers_idle.size(); i++){
        m_workers_idle[i]->stop();
    }
}

RebalancingWorker* RebalancingPool::acquire(){
    scoped_lock<mutex> lock(m_mutex);
    if(m_workers_idle.size() > 0){
        RebalancingWorker* last = m_workers_idle.back();
        m_workers_idle.pop_back();
        m_num_workers_active++;
        return last;
    } else {
        return nullptr;
    }
}

vector<RebalancingWorker*> RebalancingPool::acquire(size_t num_workers){
    scoped_lock<mutex> lock(m_mutex);
    vector<RebalancingWorker*> result;
    for(size_t i = 0, sz = std::min(num_workers, m_workers_idle.size()); i < sz; i++){
        result.push_back(m_workers_idle.back());
        m_workers_idle.pop_back();
        m_num_workers_active++;
    }
    return result;
}

void RebalancingPool::release(RebalancingWorker* worker){
    scoped_lock<mutex> lock(m_mutex);
    m_workers_idle.push_back(worker);
    assert(m_num_workers_active > 0 && "Underflow");
    m_num_workers_active--;
}

bool RebalancingPool::active() const {
    return m_num_workers_active > 0;
}

} // namespace
