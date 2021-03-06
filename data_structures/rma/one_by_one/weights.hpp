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

#include <cinttypes>
#include <ostream>
#include <vector>

#include "rma/common/memory_pool.hpp"

namespace data_structures::rma::one_by_one {

// forward declaration
class PackedMemoryArray;

struct Interval {
    uint32_t m_start;
    uint16_t m_length;
    int16_t m_weight;
    int32_t m_associated_segment;

    // do not bother with the exact type of numerics
    template <typename T1, typename T2, typename T3, typename T4>
    Interval(T1 start, T2 length, T3 weight, T4 associated_segment) :
        m_start(static_cast<decltype(m_start)>(start)),
        m_length(static_cast<decltype(m_length)>(length)),
        m_weight(static_cast<decltype(m_weight)>(weight)),
        m_associated_segment(static_cast<decltype(m_associated_segment)>(associated_segment)) { };
};

std::ostream& operator<<(std::ostream& out, Interval interval);

/**
 * A vector of Intervals, managed through the custom allocator CachedAllocator
 */
using VectorOfIntervals = std::vector<Interval, common::CachedAllocator<Interval>>;

class Weights {
private:
    PackedMemoryArray& m_pma;
    const uint16_t* m_cardinalities;
    const size_t m_segment_start;
    const size_t m_segment_length;
//    const double m_threshold;

    // intermediate information
    int64_t* m_timestamps = nullptr;
    int64_t m_timestamps_length = 0;
    int32_t* m_prefix_sum_cardinalities = nullptr;

    bool m_output_released = false; // already returned the vector of intervals (a call to ::release())
    VectorOfIntervals m_output; // output
    int32_t m_balance = 0;

    void fetch_detector_keys();

    int64_t rank(size_t position);

    int64_t rank(int64_t* __restrict array, size_t length, size_t position);

    /**
     * Helper function: standard partition method for quick sort
     */
    size_t partition(int64_t* __restrict array, size_t length);

    /**
     * Compute the prefix sum of the cardinalities and store into the member m_prefix_sum_cardinalities
     */
    void prefix_sum_cardinalities();

    /**
     * Get the number of elements in [m_segment_start, m_segment_start + segment_id];
     */
    size_t get_cardinality_upto_incl(size_t segment_id) const;

    /**
     * Get the number of elements in [m_segment_start, m_segment_start + segment_id)
     */
    size_t get_cardinality_upto_excl(size_t segment_id) const;

    /**
     * Get the cardinality of the segment m_segment_start + segment_id
     */
    size_t get_cardinality(size_t segment_id) const;

    /**
     * Find the position of the key in the segment m_segment_start + segment_id, or return -1 if not found.
     */
    int find_key(size_t segment_id, int64_t key) const noexcept;

    /**
     * Identify the intervals hammered and populate them in the vector m_output;
     */
    void detect_hammered(int64_t select_threshold);

    /**
     * Remove neutral intervals. These are intervals whose weight is zero.
     */
    void remove_neutral();

public:
    Weights(PackedMemoryArray& pma, size_t segment_start, size_t segment_length);

    ~Weights();

    VectorOfIntervals release();

    int balance() const noexcept;
};

} // namespace
