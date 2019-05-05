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

#include "sequential.hpp"

#include <cinttypes>
#include <cstdio>
#include <iostream>
#include <pthread.h>
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

#include "data_structures/iterator.hpp"

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#if defined(DEBUG)
    #define COUT_DEBUG(msg) std::cout << "[MasstreeSequential::" << __FUNCTION__ << "] " << msg << std::endl
#else
    #define COUT_DEBUG(msg)
#endif

namespace data_structures::masstree {

// Helper macros
#define MASSTREE_ptr reinterpret_cast<Masstree::basic_table<Parameters>*>(m_masstree)
#define MASSTREE (*MASSTREE_ptr)
#define THREAD (*reinterpret_cast<threadinfo*>(m_thread))

namespace {
struct Parameters : public Masstree::nodeparams<15,15> /* defaults */ {
    static constexpr bool concurrent = false;
    typedef int64_t value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef threadinfo threadinfo_type;
};
}

// I think Masstree::Str doesn't own any buffer, but it relies on the given key_buf as memory space
//static inline Masstree::Str make_key(uint64_t key) {
//    key = __builtin_bswap64(key);
//    return Masstree::Str((const char *)&key, sizeof(key));
//}
static inline
Masstree::Str make_key(uint64_t int_key, uint64_t& key_buf) {
    key_buf = __builtin_bswap64(int_key);
    return Masstree::Str((const char *)&key_buf, sizeof(key_buf));
}

Sequential::Sequential() {
    threadinfo* ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    ti->pthread() = pthread_self();
    m_thread = ti;

    auto masstree = new Masstree::basic_table<Parameters>();
    masstree->initialize(*ti);
    m_masstree = masstree;

    m_cardinality = 0;
}

Sequential::~Sequential() {
    MASSTREE_ptr->destroy(THREAD);
    delete MASSTREE_ptr; m_masstree = nullptr;

    // how the heck you are supposed to release a threadinfo object?
}

void Sequential::insert(int64_t key, int64_t value){
    uint64_t buffer;
    auto encoded_key = make_key(key, buffer);
    Masstree::tcursor<Parameters> cursor(MASSTREE, encoded_key);
    cursor.find_insert(THREAD);
    cursor.value() = value;
    cursor.finish(/* 1 = insert, -1 remove, 0 unclear atm */ 1, THREAD);
    m_cardinality++;
}

int64_t Sequential::find(int64_t key) const {
    uint64_t buffer;
    int64_t value = -1; // not found -> default value = -1
    auto encoded_key = make_key(key, buffer);
    MASSTREE.get(encoded_key, value, THREAD);
    return  value;
}

int64_t Sequential::remove(int64_t key) {
    uint64_t buffer;
    int64_t value = -1; // default value
    auto encoded_key = make_key(key, buffer);
    Masstree::tcursor<Parameters> cursor(MASSTREE, encoded_key);
    bool found = cursor.find_locked(THREAD); // is there a find unlocked ?
    if(found){
        value = cursor.value();
        cursor.finish(/* 1 = insert, -1 remove, 0 unclear atm */ -1, THREAD);
        m_cardinality--;
    } else {
        cursor.finish(0, THREAD);
    }

    return value;
}

size_t Sequential::size() const {
    return m_cardinality;
}

bool Sequential::empty() const {
    return m_cardinality == 0;
}


namespace {
struct SumScanner{
    ::data_structures::Interface::SumResult m_result;
    int64_t m_max;

    template<typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) { /* not sure what this does */ }

    bool visit_value(Masstree::Str key_str, int64_t value, threadinfo&) {
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

::data_structures::Interface::SumResult Sequential::sum(int64_t min, int64_t max) const {
    uint64_t buffer;

    SumScanner scanner;
    scanner.m_max = max;
    scanner.m_result.m_first_key = std::numeric_limits<int64_t>::min();
    Masstree::Str firstkey = make_key(min, buffer);
    MASSTREE_ptr->scan(firstkey, true, scanner, THREAD);

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

std::unique_ptr<data_structures::Iterator> Sequential::iterator() const {
    uint64_t buffer;

    IteratorScanner scanner;
    scanner.m_elements.reserve(m_cardinality);
    Masstree::Str firstkey = make_key(0, buffer);
    MASSTREE_ptr->scan(firstkey, true, scanner, THREAD);
    auto iterator = std::make_unique<MasstreeIterator>(move(scanner.m_elements));
    return iterator;
}

void Sequential::dump() const {
    MASSTREE_ptr->print(nullptr);
}

} /* namespace data_structures::masstree */
