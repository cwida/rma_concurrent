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

#include <vector>
#include "rma/common/memory_pool.hpp"
#include "rma/common/partition.hpp"

namespace data_structures::rma::one_by_one {

using Partition = ::data_structures::rma::common::Partition;

/**
 * A vector of Partitions, managed through the custom allocator CachedAllocator
 */
using VectorOfPartitions = std::vector<Partition, common::CachedAllocator<Partition>>;

/**
 * Obtain an empty vector of partitions
 */
inline VectorOfPartitions vector_of_partitions(common::CachedMemoryPool& memory_pool) {
    return VectorOfPartitions{ memory_pool.allocator<Partition>() };
}

} // namespace
