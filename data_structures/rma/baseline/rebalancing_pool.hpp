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
#include <mutex>
#include <vector>

namespace data_structures::rma::baseline {

// Forward declarations
class RebalancingMaster;
class RebalancingTask;
class RebalancingWorker;

class RebalancingPool {
    std::vector<RebalancingWorker*> m_workers_idle; // workers awaiting executions
    uint64_t m_num_workers_active; // number of workers acquired from the thread pool, and not release yet
    mutable std::mutex m_mutex; // sync

public:
    RebalancingPool(uint64_t num_workers);

    ~RebalancingPool();

    void start();

    void stop();

    /**
     * Acquires an idle worker from the pool
     * Returns nullptr on failure, i.e. there are no idle workers available
     */
    RebalancingWorker* acquire();

    std::vector<RebalancingWorker*> acquire(size_t num_workers);

//    void release(const std::vector<RebalancingWorker*> workers);

    void release(RebalancingWorker* worker);

    bool active() const;
};

}
