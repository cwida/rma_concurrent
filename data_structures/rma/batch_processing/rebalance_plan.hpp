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

namespace data_structures::rma::batch_processing {

enum class RebalanceOperation { REBALANCE, RESIZE, RESIZE_REBALANCE };
struct RebalancePlan {
    RebalanceOperation m_operation = RebalanceOperation::REBALANCE; // the operation to perform
    int64_t m_window_start = -1; // the first segment to rebalance
    int64_t m_window_length = -1; // the number of segments to rebalance, starting from m_window_start
    int64_t m_cardinality_before = -1; // the cardinality of the window before any insertion/bulk loading
    int64_t m_cardinality_change = 0; // the amount of elements to insert in the pma

    RebalancePlan() { }

    int64_t get_cardinality_before() const {
        return m_cardinality_before;
    }

    int64_t get_cardinality_after() const {
        return m_cardinality_before + m_cardinality_change;
    }

    bool is_insert() const {
        return m_cardinality_change > 0;
    }
};

std::ostream& operator<<(std::ostream& out, const RebalancePlan& plan); // for debugging purposes

} // namespace

