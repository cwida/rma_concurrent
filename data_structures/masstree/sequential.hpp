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

#pragma once

#include <cinttypes>
#include "data_structures/interface.hpp"

namespace data_structures::masstree {

class Sequential : public ::data_structures::Interface {
private:
    void* m_masstree; // pimpl
    void* m_thread; // threadinfo object
    uint64_t m_cardinality; // number of elements contained

public:
    Sequential();

    ~Sequential();

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
    std::unique_ptr<data_structures::Iterator> iterator() const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    void dump() const override;
};

} // namespace data_structures::masstree::sequential
