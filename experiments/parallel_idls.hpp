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

#include <cstddef>
#include <cinttypes>
#include <memory>
#include <vector>

#include "interface.hpp"
#include "distributions/idls_distributions.hpp"

namespace data_structures { class Interface; } // forward decl.

namespace experiments {

class ExperimentParallelIDLSThread; // forward declaration

class ParallelIDLS : public Interface {
    std::shared_ptr<data_structures::Interface> m_data_structure; // actual implementation
    const size_t N_initial_inserts; // number of initial elements to insert
    const size_t N_insdel; // number of insert/delete operations, total
    const size_t N_consecutive_operations; // perform N consecutive insertions/deletions at the time
    const distributions::idls::eDistributionType m_distribution_type_insert; // the kind of distribution to employ for inserts
    const double m_distribution_param_alpha_insert; // first parameter of the distribution
    const distributions::idls::eDistributionType m_distribution_type_delete; // the kind of distribution to employ for deletes
    const double m_distribution_param_alpha_delete; // first parameter of the distribution
    const double m_distribution_param_beta; // second parameter of the distribution
    const uint64_t m_distribution_seed; // the seed to use to initialise the distribution
    distributions::idls::DistributionsContainer m_keys_experiment; // the distributions to perform the experiment
    std::vector<ExperimentParallelIDLSThread*> m_insert_threads;
    std::vector<ExperimentParallelIDLSThread*> m_scan_threads;

    /**
     * Init the distribution type from the given parameter
     */
    distributions::idls::eDistributionType get_distribution_type(std::string value) const;

    /**
     * Insert the first `N_initial_inserts' into the data structure
     */
    void run_initial_inserts();

    /**
     * Perform the bulk of the experiment, inserting and/or deleting the elements in the data structure
     */
    void run_insert_deletions();

    /**
     * Insert the given elements in the data structure
     */
    void run_updates(distributions::idls::Distribution<long>* distribution);

protected:
    /**
     * Initialise the experiment. Compute the keys required for the insert/delete/lookup/range queries operations.
     */
    void preprocess() override;

    /**
     * Execute the experiment
     */
    void run() override;

public:
    ParallelIDLS(std::shared_ptr<data_structures::Interface> data_structure, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
            std::string insert_distribution, double insert_alpha,
            std::string delete_distribution, double delete_alpha,
            double beta, uint64_t seed, uint64_t insert_threads, uint64_t scan_threads);

    virtual ~ParallelIDLS();
};

} // namespace experiments
