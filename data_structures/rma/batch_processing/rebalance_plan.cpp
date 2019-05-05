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

#include "rebalance_plan.hpp"

namespace data_structures::rma::batch_processing {

std::ostream& operator<<(std::ostream& out, const RebalancePlan& plan){
    out << "{PLAN ";
    switch(plan.m_operation){
    case RebalanceOperation::REBALANCE: out << "REBALANCE"; break;
    case RebalanceOperation::RESIZE: out << "RESIZE"; break;
    case RebalanceOperation::RESIZE_REBALANCE: out << "RESIZE_REBALANCE"; break;
    default: out << "???"; break;
    }
    out << " window start: " << plan.m_window_start << ", length: " << plan.m_window_length << ", "
            "cardinality (before/after): " << plan.get_cardinality_before() << "/" << plan.get_cardinality_after();
    out << "}";

    return out;
}

} // namespace
