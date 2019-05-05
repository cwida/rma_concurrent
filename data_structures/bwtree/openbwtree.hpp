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
 *  This is a wrapper to the Open BwTree implementation by Ziqi Wang
 *  https://github.com/wangziqi2016/index-microbench
 */

#pragma once

#include <atomic>
#include <vector>

#include "interface.hpp"
#include "iterator.hpp"
#include "parallel.hpp"

namespace data_structures::bwtree {

class OpenBwTree: public data_structures::Interface, public data_structures::ParallelCallbacks {
private:
    void* m_pImpl; // pointer to the implementation
    std::atomic<uint64_t> m_cardinality; // number of elements contained
    mutable std::vector<std::vector<int64_t>> m_values; // helper vectors (one per worker thread) to store the intermediate values
    bool m_sequential; // sequential execution ?

public:
    /**
     * Default constructor
     */
    OpenBwTree();

    /**
     * Destructor
     */
    virtual ~OpenBwTree();

    /**
     * Insert the given <key, value> in the container
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    int64_t find(int64_t key) const override;

    /**
     * Remove the element with the given `key' from the PMA. Supported only from a few implementations.
     * Returns the value associated to the given `key', or -1 if not found.
     */
    int64_t remove(int64_t key) override;

    /**
     * Return the number of elements in the container
     */
    std::size_t size() const override;

    /**
     * Check whether the given data structure is empty
     */
    bool empty() const;

    /**
     * Sum all elements in the interval [min, max]
     */
    ::data_structures::Interface::SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Scan all elements in the container
     */
    std::unique_ptr<::data_structures::Iterator> iterator() const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    void dump() const override;

    /**
     * Callback to init & clean up the internal state during execution
     */
    void on_init_main(int num_threads) override; // start the timer to update the global & the (last) active epoch, used for the garbage collector
    void on_init_worker(int worker_id) override; // init the threadinfo* object, keeping local objects marked for deletions and to be removed by the garbage collector
    void on_destroy_worker(int worker_id) override; // delete the local threadinfo* object
    void on_destroy_main() override; // stop the timer to update the global & the (last) active epoch
};

} // namespace

