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

#include "data_structures/interface.hpp"
#include "data_structures/iterator.hpp"

namespace data_structures::rma::one_by_one {

class Gate; // forward declaration
class PackedMemoryArray; // forward declaration

class Iterator : public data_structures::Iterator {
    const PackedMemoryArray* m_pma;
    Gate* m_gate = nullptr;
    int64_t m_min; // the next minimum to search for
    const int64_t m_max; // the maximum key of the interval
    int64_t m_offset = 0; // the current position in the storage
    int64_t m_stop = -1; // index when the current sequence stops
    bool m_last = false; // whether the iterator has been consumed

    /**
     * Acquire the next extent
     */
    void restart();

    /**
     * Try to acquire the next extent
     */
    void acquire_lock(uint64_t gate_id);

    /**
     * Release the current extent held
     */
    void release_lock();

    /**
     * Set the start & end offset once entered a new extent
     */
    void set_offset();
    void set_offset(uint64_t segmnet_id);

    /**
     * Move the iterator to the next window of elements in the iterator
     */
    void fetch_next_chunk();

public:
    /**
     * Initialise the iterator
     * @param pma the instance attached to this iterator
     * @param min the minimum of the interval (inclusive)
     * @param max the maximum of the interval (inclusive)
     */
    Iterator(const PackedMemoryArray* pma, int64_t min, int64_t max);

    /**
     * Destructor
     */
    virtual ~Iterator();

    /**
     * Does it exist a next element ?
     */
    virtual bool hasNext() const;

    /**
     * Retrieve the next element from the Iterator
     */
    virtual std::pair<int64_t, int64_t> next();
};

} // namespace
