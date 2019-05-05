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

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "interface.hpp"

namespace data_structures { class Interface; } // forward declaration

namespace experiments {
namespace { struct ContainerKeys; } // forward decl, impl detail

class ParallelScan : public Interface {
    std::shared_ptr<data_structures::Interface> m_data_structure; // the data structure to evaluate
    const std::chrono::seconds m_execution_time; // the amount of time to run each experiment
    std::unique_ptr<ContainerKeys> m_keys; // map the keys contained in the pma

protected:
    void preprocess() override;

    void run_on_threads(int numa_node, const std::vector<int>& thread_ids, int num_threads);

    void run_on_all_threads();

    void run_on_socket(int numa_node);

    void run() override;

public:
    ParallelScan(std::shared_ptr<data_structures::Interface> data_structure, std::chrono::seconds execution_time);

    virtual ~ParallelScan();
};


} /* namespace experiments */
