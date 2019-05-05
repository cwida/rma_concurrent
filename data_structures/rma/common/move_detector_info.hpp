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
#include <cstddef>
#include <ostream>
#include <utility>

#include "memory_pool.hpp"

namespace data_structures::rma::common {

class MoveDetectorInfo{
    CachedMemoryPool& m_memory_pool;
    int64_t* m_detector_buffer; // input
    const size_t m_detector_entry_size; // size of each entry in the detector buffer
    std::pair<uint32_t, uint32_t>* m_registered_segments; // segments that need to be moved
    size_t m_registered_segments_capacity; // space in the array m_registered_segments
    size_t m_registered_segments_sz; // current number of segments registered

private:
   void move();

public:
   template<typename PackedMemoryArray>
   MoveDetectorInfo(PackedMemoryArray& pma, size_t segment_start);

   MoveDetectorInfo(CachedMemoryPool& memory_pool, int64_t* detector_buffer, const size_t entry_size);

    ~MoveDetectorInfo();

    // Change the capacity of the internal buffer
    void resize(size_t sz);

    // Register a section for the detector
    void move_section(uint32_t from, uint32_t to);

    // Dump the contained information, for debug purposes
    void dump(std::ostream& out) const;
    void dump() const;
};

std::ostream& operator<<(std::ostream& out, const MoveDetectorInfo& mdi);

// implementation detail
template<typename PackedMemoryArray>
MoveDetectorInfo::MoveDetectorInfo(PackedMemoryArray& pma, size_t segment_start) :
    MoveDetectorInfo(pma.memory_pool(), pma.detector().buffer() + segment_start * pma.detector().sizeof_entry(), pma.detector().sizeof_entry()) { }

} // namespace
