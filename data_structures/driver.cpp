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
#include <masstree/parallel.hpp>
#include <masstree/sequential.hpp>
#include "driver.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "factory.hpp"
#include "interface.hpp"

#include "common/configuration.hpp"
#include "common/console_arguments.hpp"
#include "common/errorhandling.hpp"
#include "common/miscellaneous.hpp"

#include "experiments/interface.hpp"
#include "experiments/parallel_idls.hpp"
#include "experiments/parallel_insert.hpp"
#include "experiments/parallel_scan.hpp"

// data structures
#include "abtree/parallel/abtree.hpp"
#include "abtree/sequential/abtree.hpp"
#include "bwtree/openbwtree.hpp"
#include "masstree/parallel.hpp"
#include "masstree/sequential.hpp"

#include "rma/baseline/packed_memory_array.hpp"
#include "rma/batch_processing/packed_memory_array.hpp"
#include "rma/common/knobs.hpp"
#include "rma/one_by_one/packed_memory_array.hpp"

using namespace std;
using namespace common;

namespace data_structures {

static bool initialised = false;

void initialise() {
//    if(initialised) RAISE_EXCEPTION(Exception, "Function pma::initialise() already called once");
    if(initialised) return;

    PARAMETER(uint64_t, "inode_block_size").alias("iB");
    PARAMETER(uint64_t, "leaf_block_size").alias("lB");
    PARAMETER(uint64_t, "extent_size").descr("The size of an extent used for memory rewiring. It is defined as a multiple in terms of a page size.");
//    PARAMETER(bool, "abtree_random_permutation")
//        .descr("Randomly permute the nodes in the tree. Only significant for the baseline abtree implementation (btree_v2).");
//    PARAMETER(bool, "record_leaf_statistics")
//        .descr("When deleting the index, record in the table `btree_leaf_statistics' the statistics related to the memory distance among consecutive leaves/segments. Supported only by the algorithms btree_v2, btreecc_pma4 and apma_clocked");
//    PARAMETER(double, "apma_predictor_scale").descr("The scale parameter to re-adjust the capacity of the predictor").set_default(1.0);


    { // APMA knobs
        data_structures::rma::common::Knobs knobs;

        auto param_rank = PARAMETER(double, "apma_rank").hint().descr("Explicitly set the rank "
                "threshold, in [0, 1], for marking segments as hammered. For instance, a value of `0.9', entails that a "
                "segment must have a minimum rank of at least 90% higher than the others to be marked as hammered");
        param_rank.set_default(knobs.get_rank_threshold());
        param_rank.validate_fn([](double value){ return (value >= 0. && value <= 1.); });

        auto param_sampling_rate = PARAMETER(double, "apma_sampling_rate").hint().descr("Sample threshold "
                "to forward an update to the detector, in [0, 1]. The value 1 implies all updates are recorded, 0 while no "
                "updates are recorded.");
        param_sampling_rate.set_default(knobs.get_sampling_rate());
        param_sampling_rate.validate_fn([](double value){ return (value >= 0. && value <= 1.); });
    }


    PARAMETER(uint64_t, "apma_rebalancing_threads").descr("Number of worker threads to use in the Rebalancer. Only used in the algorithm `apma_parallel'")
            .set_default(8).validate_fn([](uint64_t value){ return value >= 1; });
    PARAMETER(uint64_t, "apma_segments_per_lock").descr("Number of contiguous segments covered by a single lock. It must be a power of 2 >= 2. Only used in the algorithm `apma_parallel'")
            .set_default(8).validate_fn([](uint64_t value){ return value >= 2 && is_power_of_2(value); });

//    REGISTER_DATA_STRUCTURE("apma_parallel_update", "Parallel version of APMA/int2 (with the standard thresholds). Set the size of an extent with the option --extent_size=N", [](){
//        uint64_t iB = ARGREF(uint64_t, "iB");
//        uint64_t lB = ARGREF(uint64_t, "lB");
//        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
//        if(!param_extent_mult.is_set())
//            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_parallel] Mandatory parameter --extent size not set.");
//        uint64_t extent_mult = param_extent_mult.get();
//        uint64_t worker_threads_rebalancer = ARGREF(uint64_t, "apma_rebalancing_threads");
//        uint64_t segments_per_lock = ARGREF(uint64_t, "apma_segments_per_lock");
//        LOG_VERBOSE("[apma_parallel_update] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
//                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes), "
//                        "worker threads in the rebalancer: " << worker_threads_rebalancer << ", "
//                        "segments per lock: " << segments_per_lock);
//        auto algorithm = make_unique<adaptive::parallel_v1::PackedMemoryArray>(iB, lB, extent_mult, worker_threads_rebalancer, segments_per_lock);
//
//        // Rank threshold
//        auto argument_rank = ARGREF(double, "apma_rank");
//        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }
//
//        // Right now, it is the same as `apma_parallel_scan'. To only use the standard thresholds:
//        algorithm->knobs().set_thresholds_switch(numeric_limits<int32_t>::max());
//
//        return algorithm;
//    });
    REGISTER_DATA_STRUCTURE("rma_baseline", "Parallel version of APMA/int3 (with Katriel's thresholds). Set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_parallel] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        uint64_t worker_threads_rebalancer = ARGREF(uint64_t, "apma_rebalancing_threads");
        uint64_t segments_per_lock = ARGREF(uint64_t, "apma_segments_per_lock");
        LOG_VERBOSE("[rma_baseline (scan)] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes), "
                        "worker threads in the rebalancer: " << worker_threads_rebalancer << ", "
                        "segments per lock: " << segments_per_lock);
        auto algorithm = make_unique<rma::baseline::PackedMemoryArray>(iB, lB, extent_mult, worker_threads_rebalancer, segments_per_lock);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        return algorithm;
    });

    REGISTER_DATA_STRUCTURE("rma_1by1", "Parallel version of APMA/int3 (with Katriel's thresholds). This version includes asynchronous writes to minimise "
            "the number of writers locked in a gate. Set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_parallel2] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        uint64_t worker_threads_rebalancer = ARGREF(uint64_t, "apma_rebalancing_threads");
        uint64_t segments_per_lock = ARGREF(uint64_t, "apma_segments_per_lock");
        LOG_VERBOSE("[rma_1by1] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes), "
                        "worker threads in the rebalancer: " << worker_threads_rebalancer << ", "
                        "segments per lock: " << segments_per_lock);
        auto algorithm = make_unique<rma::one_by_one::PackedMemoryArray>(iB, lB, extent_mult, worker_threads_rebalancer, segments_per_lock);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        return algorithm;
    });

    PARAMETER(uint64_t, "delay").descr("The minimum amount of time to delay a rebalance in apma_parallel3, in milliseconds").set_default(0);

    REGISTER_DATA_STRUCTURE("rma_batch", "Parallel version of APMA/int3 (with Katriel's thresholds). This version includes asynchronous writes to minimise "
            "the number of writers locked in a gate. Set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_parallel3] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        uint64_t worker_threads_rebalancer = ARGREF(uint64_t, "apma_rebalancing_threads");
        uint64_t segments_per_lock = ARGREF(uint64_t, "apma_segments_per_lock");
        auto rebal_delay = chrono::milliseconds(ARGREF(uint64_t, "delay"));
        LOG_VERBOSE("[rma_batch] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes), "
                        "worker threads in the rebalancer: " << worker_threads_rebalancer << ", "
                        "segments per lock: " << segments_per_lock << ", rebalancer delay: " << rebal_delay.count());
        auto algorithm = make_unique<rma::batch_processing::PackedMemoryArray>(iB, lB, extent_mult, worker_threads_rebalancer, segments_per_lock, rebal_delay);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        return algorithm;
    });

#if defined(HAVE_MASSTREE)
    REGISTER_DATA_STRUCTURE("masstree_seq", "Sequential masstree, default parameters. The runtime values for the inner & leaf nodes are ignored (they need to be set at compile time).", [](){
        LOG_VERBOSE("[sequential/masstree]");
        return make_unique<masstree::Sequential>();
    });
    REGISTER_DATA_STRUCTURE("masstree_par", "Parallel masstree, default parameters. The runtime values for the inner & leaf nodes are ignored (they need to be set at compile time).", [](){
        LOG_VERBOSE("[parallel/masstree]");
        return make_unique<masstree::Parallel>();
    });
#endif

    REGISTER_DATA_STRUCTURE("openbwtree", "OpenBwTree, default parameters. The runtime values for the inner & leaf nodes are ignored (they need to be set at compile time).", [](){
        LOG_VERBOSE("[openbwtree]");
        return make_unique<bwtree::OpenBwTree>();
    });

    REGISTER_DATA_STRUCTURE("abtree_parallel", "Parallel implementation of an ab-tree using ART as secondary index. The parameter for the inner node capacity is ignored.", [](){
        uint64_t leaf_capacity = ARGREF(uint64_t, "lB");
        LOG_VERBOSE("[abtree_parallel] leaf capacity: " << leaf_capacity);
        return make_unique<abtree::parallel::ABTree>(leaf_capacity);
    });

    /**
     * Parallel scan
     */
    PARAMETER(uint64_t, "duration")["D"].hint("secs").descr("The duration of each scan in the experiment parallel_scan, in seconds.").set_default(360);
    REGISTER_EXPERIMENT("parallel_scan", "Perform scans with multiple threads over 1% of the data structure. Use -I to set the size of the data structure and -D the duration of each scan, in seconds", [](shared_ptr<Interface> data_structure){
        return make_unique<experiments::ParallelScan>(data_structure, chrono::seconds( ARGREF(uint64_t, "duration") ));
    });

    /**
     * Parallel insert experiment
     */
    PARAMETER(uint64_t, "thread_inserts").set_default(0).descr("Number of insertion threads for the `parallel_insert' and `parallel_idls' experiments");
    PARAMETER(uint64_t, "thread_scans").set_default(0).descr("Number of scan threads for the `parallel_insert' and `parallel_idls' experiments");
    REGISTER_EXPERIMENT("parallel_insert", "Insert up to -I <size> elements in parallel while the data structure is concurrently scanned. "
            "Set the parallel degree with --thread_inserts for the insertion threads and --thread_scans for the scans", [](shared_ptr<Interface> data_structure){
        auto param_thread_inserts = ARGREF(uint64_t, "thread_inserts");
        auto param_thread_scans = ARGREF(uint64_t, "thread_scans");
        return make_unique<experiments::ParallelInsert>(data_structure, param_thread_inserts, param_thread_scans);
    });
    REGISTER_EXPERIMENT("parallel_idls", "Perform `initial_size' insertions in the data structure at the start. Afterward perform `num_insertions' operations split in groups of `idls_group_size' consecutive inserts/deletes.",
        [](shared_ptr<Interface> data_structure){
        auto N_initial_inserts = ARGREF(int64_t, "initial_size");
        auto N_insdel = ARGREF(int64_t, "I");
        auto N_lookups = ARGREF(int64_t, "L");
        if(N_lookups != 0) std::cerr << "[WARNING] Argument -L (--num_lookups) ignored in this experiment" << std::endl;
        auto N_scans = ARGREF(int64_t, "S");
        if(N_scans != 0) std::cerr << "[WARNING] Argument -S (--num_scans) ignored in this experiment" << std::endl;

        auto N_consecutive_operations = ARGREF(int64_t, "idls_group_size");

        auto insert_distribution = ARGREF(string, "distribution");
        auto insert_alpha = ARGREF(double, "alpha");
        auto param_delete_distribution = ARGREF(string, "idls_delete_distribution");
        auto param_delete_alpha = ARGREF(double, "idls_delete_alpha");
        string delete_distribution = param_delete_distribution.is_set() ? param_delete_distribution.get() : insert_distribution.get();
        double delete_alpha = param_delete_alpha.is_set() ? param_delete_alpha.get() : insert_alpha.get();
        auto beta = ARGREF(double, "beta");
        auto seed = ARGREF(uint64_t, "seed_random_permutation");

        auto param_thread_inserts = ARGREF(uint64_t, "thread_inserts");
        auto param_thread_scans = ARGREF(uint64_t, "thread_scans");

        LOG_VERBOSE("parallel_idls, inserts: " << insert_distribution.get() << " (" << insert_alpha.get() << "), deletes: " << delete_distribution << " (" << delete_alpha << "), range: " << static_cast<int64_t>(beta.get()));
        LOG_VERBOSE("parallel_idls, update threads: " << param_thread_inserts << ", scan threads: " << param_thread_scans);

        return make_unique<experiments::ParallelIDLS>(data_structure, N_initial_inserts, N_insdel, N_consecutive_operations,
                insert_distribution, insert_alpha,
                delete_distribution, delete_alpha,
                beta, seed,
                param_thread_inserts, param_thread_scans);
    });

    { // the list of available algorithms
        stringstream helpstr;
        helpstr << "The algorithm to evaluate. The possible choices are:";
        for(size_t i = 0; i < factory().algorithms().size(); i++){
            auto& item = factory().algorithms()[i];
            helpstr << "\n- " << item->name() << ": " << item->description();
        }

        PARAMETER(string, "algorithm")["a"].hint().required().record(false)
                .descr(helpstr.str())
                .validate_fn([](const std::string& algorithm){
            auto& list = factory().algorithms();
            auto res = find_if(begin(list), end(list), [&algorithm](auto& impl){
                return impl->name() == algorithm;
            });
            if(res == end(list))
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid algorithm: " << algorithm);
            return true;
        });
    }

    PARAMETER(uint64_t, "inode_block_size")["b"].hint().set_default(64)
                        .descr("The block size for the intermediate nodes");
    PARAMETER(uint64_t, "leaf_block_size")["l"].hint().set_default(128)
                        .descr("The block size of the leaves");

    // IDLS experiment
    PARAMETER(int64_t, "idls_group_size").hint("N >= 1").set_default(1)
            .descr("Size of consecutive inserts/deletes in the IDLS experiment.")
            .validate_fn([](int64_t value){ return value >= 1; });
    PARAMETER(string, "idls_delete_distribution")
            .descr("The distribution for the deletions in the IDLS experiment. By default it's the same as inserts. Valid values are `uniform' and `zipf'.");
    PARAMETER(double, "idls_delete_alpha")
            .descr("Rho factor in case the delete distribution is Zipf");

    // Density constraints
    PARAMETER(double, "rho_0").hint().set_default(0.08)
            .descr("Lower density in the PMA for the lowest level of the calibrator tree, i.e. the segments.");
    PARAMETER(double, "rho_h").hint().set_default(0.3)
            .descr("Lower density in the PMA for the highest level of the calibrator tree, i.e. the root.");
    PARAMETER(double, "theta_h").hint().set_default(0.75)
            .descr("Upper density in the PMA for the highest level of the calibrator tree, i.e. the root");
    PARAMETER(double, "theta_0").hint().set_default(1.0)
            .descr("Upper density in the PMA for the lowest level of the calibrator tree, i.e. the segments");

    { // the list of experiments
        stringstream helpstr;
        helpstr << "The experiment to perform. The possible choices are: ";
        for(size_t i = 0; i < factory().experiments().size(); i++){
            auto& e = factory().experiments()[i];
            if(!e->is_display()) continue;
            helpstr << "\n- " << e->name() << ": " << e->description();
        }

        PARAMETER(string, "experiment")["e"].hint().required().record(false)
                .descr(helpstr.str())
                .validate_fn([](const std::string& experiment){
            auto& list = factory().experiments();
            auto res = find_if(begin(list), end(list), [&experiment](auto& impl){
                return impl->name() == experiment;
            });
            if(res == end(list))
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid experiment: " << experiment);
            return true;
        });
    }

    initialised = true;
}


void execute(){
    if(!initialised) RAISE_EXCEPTION(Exception, "::data_structures::initialise() has not been called");

    string name_algorithm = ARGREF(string, "algorithm");
    string name_experiment = ARGREF(string, "experiment");
    shared_ptr<experiments::Interface> experiment;

    experiment = factory().make_experiment(name_experiment, factory().make_algorithm(name_algorithm));

    experiment->execute();
}


void prepare_parameters() {
    // nop
    // used in the main driver to validate the args of the STX-Tree and of the bulk loading experiment
    // these are not included in this package...
}

} // namespace data_structures


