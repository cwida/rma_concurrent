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
 * Wrapper for the Masstree data structure by:
 * -- Eddie Kohler, Yandong Mao, Robert Morris
 * -- Copyright (c) 2012-2014 President and Fellows of Harvard College
 * -- Copyright (c) 2012-2014 Massachusetts Institute of Technology
 */

#include "parallel.hpp"

#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstring> // memset, strerror
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <utility>
#include <vector>

#include "third-party/masstree/kvthread.hh"
#include "third-party/masstree/masstree.hh"
#include "third-party/masstree/masstree_struct.hh"
#include "third-party/masstree/masstree_insert.hh"
#include "third-party/masstree/masstree_remove.hh"
#include "third-party/masstree/masstree_print.hh"
#include "third-party/masstree/masstree_tcursor.hh"
#include "third-party/masstree/masstree_scan.hh"

#include "globals.hpp"
#include "common/errorhandling.hpp"
#include "data_structures/iterator.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    static mutex _local_mutex;
    #define COUT_DEBUG(msg) { lock_guard<mutex> _lock(_local_mutex); \
        std::cout << "[MasstreeParallel::" << __FUNCTION__ << "] [thread: " << pthread_self() << "] " << msg << std::endl; }
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Thread info                                                             *
 *                                                                           *
 *****************************************************************************/
static thread_local threadinfo* tls_threadinfo = nullptr;

/*****************************************************************************
 *                                                                           *
 *   Masstree parameters                                                     *
 *                                                                           *
 *****************************************************************************/
namespace {
struct Parameters : public Masstree::nodeparams<15,15> /* defaults */ {
    static constexpr bool concurrent = true;
    typedef int64_t value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef threadinfo threadinfo_type;
};
} // anonymous namespace

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
namespace data_structures::masstree {

// Helper macros
#define MASSTREE_ptr reinterpret_cast<Masstree::basic_table<Parameters>*>(m_masstree)
#define MASSTREE (*MASSTREE_ptr)

Parallel::Parallel() : m_masstree(nullptr), m_cardinality(0){
    if(tls_threadinfo != nullptr){ RAISE_EXCEPTION(common::Exception, "[Masstree ctor] The local threadinfo object should be nullptr, this data should have not been initialised or used beforehand on the same thread."); }
    tls_threadinfo = threadinfo::make(threadinfo::TI_MAIN, -1);
    tls_threadinfo->pthread() = pthread_self();

    auto masstree = new Masstree::basic_table<Parameters>();
    masstree->initialize(*tls_threadinfo);
    m_masstree = masstree;
}

Parallel::~Parallel() {
    MASSTREE_ptr->destroy(*tls_threadinfo);
    delete MASSTREE_ptr; m_masstree = nullptr;

    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_MAIN && "Expected to be in the main thread");
    tls_threadinfo = nullptr;
}

// Update the global epoch and the min local epoch (in all worker threads). Invoked as a callback by the linux kernel once each second.
static void masstree_update_epoch(int /*signal_id*/){
    globalepoch += 2;
    active_epoch = threadinfo::min_active_epoch();
}

void Parallel::on_init_main(int num_threads) {
    /**
     * Logically this creates a background thread that, every second, updates the global epoch & active epoch,
     * used for garbage collection purposes. In practice, we rely on the POSIX timer to invoke the callback
     * masstree_update_epoch once in a second, without having to create our own thread.
     *
     * The choice of 1 second comes from the default interval used in the Masstree implementation for its server.
     */

    /* Install timer_handler as the signal handler for . */
    struct sigaction sa;

    memset (&sa, 0, sizeof (sa));
    sa.sa_handler = &masstree_update_epoch;
    sigaction (SIGALRM, &sa, NULL);

    // Invoke the timer the first time after 1 second
    struct itimerval timer;
    timer.it_value.tv_sec = 1;
    timer.it_value.tv_usec = 0;
    // And then again every after 1 seconds
    timer.it_interval.tv_sec = 1;
    timer.it_interval.tv_usec = 0;
    // Install the timer
    int rc = setitimer(ITIMER_REAL /* wall clock timer */, &timer, NULL);
    if(rc != 0) RAISE_EXCEPTION(common::Exception, "[MassTree::on_init_main] Cannot install the timer. Errno: " << strerror(errno) << " (" << errno << ")");
}

void Parallel::on_init_worker(int worker_id) {
    assert(tls_threadinfo == nullptr && "This data structure should have never been initialised on this thread");
    tls_threadinfo = threadinfo::make(threadinfo::TI_PROCESS, worker_id);
    tls_threadinfo->pthread() = pthread_self();
}

void Parallel::on_destroy_worker(int worker_id) {
    assert(tls_threadinfo != nullptr && "Expected to be initialised");
    assert(tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to run on a worker thread");

    // how the heck you're supposed to reclaim the space for both the garbage collector and the threadinfo objects no longer used is unclear atm
    //delete tls_threadinfo;

    tls_threadinfo = nullptr;
}

void Parallel::on_destroy_main(){
    // stop the signal handler
    struct itimerval timer;
    memset(&timer, 0, sizeof(timer));
    int rc = setitimer(ITIMER_REAL /* wall clock timer */, &timer, NULL);
    if(rc != 0) RAISE_EXCEPTION(common::Exception, "[MassTree::on_destroy_main] Cannot stop the timer. Errno: " << strerror(errno) << " (" << errno << ")");
}

/*****************************************************************************
 *                                                                           *
 *   Insert/Delete/Point lookup                                              *
 *                                                                           *
 *****************************************************************************/

// I think Masstree::Str doesn't own any buffer, but it relies on the given key_buf as memory space
static inline
Masstree::Str make_key(uint64_t int_key, uint64_t& key_buf) {
    key_buf = __builtin_bswap64(int_key);
    return Masstree::Str((const char *)&key_buf, sizeof(key_buf));
}

void Parallel::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);
    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to be invoked on a worker thread");

    uint64_t buffer;
    auto encoded_key = make_key(key, buffer);
    Masstree::tcursor<Parameters> cursor(MASSTREE, encoded_key);
    cursor.find_insert(*tls_threadinfo);
    cursor.value() = value;
    cursor.finish(/* 1 = insert, -1 remove, 0 unclear atm */ 1, *tls_threadinfo);
    m_cardinality++;

    // update the active epoch && invoke the garbage collector for this thread
    tls_threadinfo->rcu_quiesce();
}

int64_t Parallel::find(int64_t key) const {
    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to be invoked on a worker thread");

    uint64_t buffer;
    int64_t value = -1; // not found -> default value = -1
    auto encoded_key = make_key(key, buffer);
    MASSTREE.get(encoded_key, value, *tls_threadinfo);

    // update the active epoch && invoke the garbage collector for this thread
    tls_threadinfo->rcu_quiesce();

    return value;
}

int64_t Parallel::remove(int64_t key) {
    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to be invoked on a worker thread");

    uint64_t buffer;
    int64_t value = -1; // default value
    auto encoded_key = make_key(key, buffer);
    Masstree::tcursor<Parameters> cursor(MASSTREE, encoded_key);
    bool found = cursor.find_locked(*tls_threadinfo); // is there a find unlocked ?
    if(found){
        value = cursor.value();
        cursor.finish(/* 1 = insert, -1 remove, 0 unclear atm */ -1, *tls_threadinfo);
        m_cardinality--;
    } else {
        cursor.finish(0, *tls_threadinfo);
    }

    // update the active epoch && invoke the garbage collector for this thread
    tls_threadinfo->rcu_quiesce();

    return value;
}

size_t Parallel::size() const {
    return m_cardinality;
}

bool Parallel::empty() const {
    return m_cardinality == 0;
}

/*****************************************************************************
 *                                                                           *
 *   Scans                                                                   *
 *                                                                           *
 *****************************************************************************/

namespace {
struct SumScanner{
    ::data_structures::Interface::SumResult m_result;
    int64_t m_max;

    template<typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) { /* not sure what this does */ }

    bool visit_value(Masstree::Str key_str, int64_t value, threadinfo&) {
        if(!::data_structures::global_parallel_scan_enabled) return false; // the main thread is asking to stop any scan in execution

        uint64_t key_u = 0;
        memcpy((char*)&key_u, key_str.s, key_str.len);
        uint64_t key_s = __builtin_bswap64(key_u);
        int64_t key = static_cast<int64_t>(key_s);

        if(key > m_max) return false; // stop the iterator

        if(m_result.m_first_key == std::numeric_limits<int64_t>::min())
            m_result.m_first_key = key;
        m_result.m_num_elements++;
        m_result.m_sum_keys += key;
        m_result.m_sum_values += value;
        m_result.m_last_key = key;

        return true;
    }
};
} // anonymous namespace

::data_structures::Interface::SumResult Parallel::sum(int64_t min, int64_t max) const {
    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to be invoked on a worker thread");

    uint64_t buffer;

    SumScanner scanner;
    scanner.m_max = max;
    scanner.m_result.m_first_key = std::numeric_limits<int64_t>::min();
    Masstree::Str firstkey = make_key(min, buffer);
    MASSTREE_ptr->scan(firstkey, true, scanner, *tls_threadinfo);

    // update the active epoch && invoke the garbage collector for this thread
    tls_threadinfo->rcu_quiesce();

    return scanner.m_result;
}

namespace {
struct IteratorScanner{
    std::vector<std::pair<int64_t, int64_t>> m_elements;

    template<typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) { /* not sure what this does */ }

    bool visit_value(Masstree::Str key_str, int64_t value, threadinfo&) {
        uint64_t key_u = 0;
        memcpy((char*)&key_u, key_str.s, key_str.len);
        uint64_t key_s = __builtin_bswap64(key_u);
        int64_t key = static_cast<int64_t>(key_s);
        m_elements.emplace_back(key, value);
        return true;
    }
};

struct MasstreeIterator : public ::data_structures::Iterator{
    std::vector<std::pair<int64_t, int64_t>> m_elements;
    size_t i = 0;

    MasstreeIterator(std::vector<std::pair<int64_t, int64_t>> elements) : m_elements(elements) { }
    bool hasNext() const{ return  i < m_elements.size(); }
    std::pair<int64_t, int64_t> next() { return m_elements[i++]; }
};

} // anonymous namespace


std::unique_ptr<Iterator> Parallel::iterator() const {
    assert(tls_threadinfo != nullptr && tls_threadinfo->purpose() == threadinfo::TI_PROCESS && "Expected to be invoked on a worker thread");

    uint64_t buffer;

    IteratorScanner scanner;
    scanner.m_elements.reserve(m_cardinality);
    Masstree::Str firstkey = make_key(0, buffer);
    MASSTREE_ptr->scan(firstkey, true, scanner, *tls_threadinfo);
    auto iterator = std::make_unique<MasstreeIterator>(move(scanner.m_elements));
    return iterator;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void Parallel::dump() const { // this might not be thread safe
    MASSTREE_ptr->print(nullptr);
}

} // namespace data_structures::masstree
