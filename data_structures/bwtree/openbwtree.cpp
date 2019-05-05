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

/*
 * This is a wrapper to the Open BwTree implementation by Ziqi Wang
 * https://github.com/wangziqi2016/index-microbench
 */

#include "openbwtree.hpp"

#include <cassert>
#include <iostream>
#include <mutex>

#include "third-party/openbwtree/bwtree.h"

using BwTree = wangziqi2013::bwtree::BwTree<int64_t, int64_t>;
using ForwardIterator = BwTree::ForwardIterator;
using namespace std;

namespace data_structures::bwtree {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    static mutex _local_mutex;
    #define COUT_DEBUG(msg) { lock_guard<mutex> _lock(_local_mutex); \
        std::cout << "[OpenBwTree::" << __FUNCTION__ << "] [thread: " << pthread_self() << "] " << msg << std::endl; }
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

// Helper macro
#define BWTREE reinterpret_cast<BwTree*>(m_pImpl)
#define WORKER_ID wangziqi2013::bwtree::BwTreeBase::gc_id
#define BWTREE_RESULT(variable) auto& variable = m_values[WORKER_ID]; variable.clear();

OpenBwTree::OpenBwTree() : m_pImpl(nullptr), m_cardinality(0), m_sequential(true) {
    m_pImpl = new wangziqi2013::bwtree::BwTree<int64_t, int64_t>{true};

    BWTREE->UpdateThreadLocal(1); // number of threads to use, 1 => sequential
    BWTREE->AssignGCID(0);
    m_values.resize(1);
}

OpenBwTree::~OpenBwTree() {
    delete BWTREE; m_pImpl = nullptr;
}

void OpenBwTree::on_init_main(int num_threads){
    BWTREE->UpdateThreadLocal(num_threads); // prepare the tls for `num_threads'
    BWTREE->AssignGCID(-1); // no more operations from this thread (main)
    m_sequential = false; // switch to concurrent execution
    m_values.resize(num_threads);
}

void OpenBwTree::on_init_worker(int worker_id){
    assert(worker_id < BWTREE->GetThreadNum() && "TLS not properly initialised to host at least `worker_id +1' worker threads");
    BWTREE->AssignGCID(worker_id);
}

void OpenBwTree::on_destroy_worker(int worker_id){
    assert(worker_id < BWTREE->GetThreadNum() && "TLS not properly initialised to host at least `worker_id +1' worker threads");
    BWTREE->UnregisterThread(worker_id);
}

void OpenBwTree::on_destroy_main() {
    BWTREE->AssignGCID(0);
    BWTREE->UpdateThreadLocal(1); // 1 => sequential
    m_sequential = true; // back to sequential mode
    m_values.resize(1);
}

/*****************************************************************************
 *                                                                           *
 *   Insert/Delete/Point lookup                                              *
 *                                                                           *
 *****************************************************************************/

void OpenBwTree::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);
    bool result = BWTREE->Insert(key, value); // easy
    assert(result == true && "Duplicate key/value: Insert returns false iff the pair key/value already exists");
    if(result) m_cardinality++;
}

int64_t OpenBwTree::find(int64_t key) const {
    assert(WORKER_ID >= 0 && "Thread not registered");
    assert(WORKER_ID < m_values.size() && "Overflow: bogus value for the worker id");

    int64_t result = -1; // not found;

    BWTREE_RESULT(bwtree_result);
    BWTREE->GetValue(key, bwtree_result);

    if(bwtree_result.size() > 0){
        result = bwtree_result[0];
    }

    return result;
}

int64_t OpenBwTree::remove(int64_t key) {
    int64_t value = -1; // not found;

    BWTREE_RESULT(bwtree_result);
    BWTREE->GetValue(key, bwtree_result);

    if(bwtree_result.size() > 0){
        value = bwtree_result[0];
        BWTREE->Delete(key, value);
        m_cardinality--;
    }

    return value;
}

size_t OpenBwTree::size() const {
    return m_cardinality;
}

bool OpenBwTree::empty() const {
    return m_cardinality == 0;
}

/*****************************************************************************
 *                                                                           *
 *   Scans                                                                   *
 *                                                                           *
 *****************************************************************************/

::data_structures::Interface::SumResult OpenBwTree::sum(int64_t min, int64_t max) const {
    SumResult result;

    ForwardIterator iterator = BWTREE->Begin(min);

    // fetch the first key
    if(::data_structures::global_parallel_scan_enabled && !iterator.IsEnd()){
        auto pair = *iterator;
        if(pair.first > max) return result; // empty
        result.m_first_key = pair.first;
    }

    // fetch all the remaining pairs in the range
    while(::data_structures::global_parallel_scan_enabled && !iterator.IsEnd()){
        auto pair = *iterator;
        if(pair.first > max) break; // done
        result.m_num_elements++;
        result.m_sum_keys += pair.first;
        result.m_sum_values += pair.second;
        result.m_last_key = pair.first;

        ++iterator; // next
    }

    return result;
}

namespace {

struct OpenBwTreeIterator : public ::data_structures::Iterator{
    ForwardIterator m_iterator;

    OpenBwTreeIterator(ForwardIterator iterator) : m_iterator(iterator) { }
    bool hasNext() const{ return !m_iterator.IsEnd(); }
    std::pair<int64_t, int64_t> next() { return *(m_iterator++); }
};

} // anonymous namespace

std::unique_ptr<Iterator> OpenBwTree::iterator() const {
    return make_unique<OpenBwTreeIterator>(BWTREE->Begin());
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void OpenBwTree::dump() const {
    cout << "OpenBWTree cardinality: " << m_cardinality << ", num_threads: " << m_values.size() << ", sequential: " << m_sequential << "\n";
    int64_t i = 0;
    auto iterator = BWTREE->Begin();
    while(!iterator.IsEnd()){
        auto pair = *iterator;
        cout << "[" << i++ << "] key: " << pair.first << ", value: " << pair.second << "\n";
        ++iterator;
    }
    cout << endl;
}

} // namespace
