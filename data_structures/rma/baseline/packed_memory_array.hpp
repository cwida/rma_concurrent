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


#include <atomic>
#include <mutex>
#include <type_traits>
#include <vector>

#include "data_structures/interface.hpp"
#include "data_structures/iterator.hpp"
#include "data_structures/parallel.hpp"
#include "rma/common/density_bounds.hpp"
#include "rma/common/detector.hpp"
#include "rma/common/knobs.hpp"
#include "rma/common/memory_pool.hpp"
#include "rma/common/static_index.hpp"
#include "pointer.hpp"
#include "rebalance_plan.hpp"
#include "storage.hpp"
#include "thread_context.hpp"

namespace data_structures::rma::baseline {

// forward declarations
class Gate;
class GarbageCollector;
class Iterator;
class RebalancingMaster;
class RebalancingTask;
class RebalancingWorker;
class SpreadWithRewiring; // forward decl.
class Weights;

class PackedMemoryArray : public data_structures::InterfaceRQ, public data_structures::ParallelCallbacks {
friend class GarbageCollector;
friend class Iterator;
friend class RebalancingMaster;
friend class RebalancingTask;
friend class RebalancingWorker;
friend class SpreadWithRewiring;
friend class Weights;

// aliases
using CachedDensityBounds = data_structures::rma::common::CachedDensityBounds;
using Knobs = data_structures::rma::common::Knobs;
using StaticIndex = data_structures::rma::common::StaticIndex;

protected:
    std::atomic<int64_t> m_cardinality = 0; // the number of elements contained in the data structure
    Storage m_storage; // actual content. There is no need to further protect its access, workers/rebalancers need to hold a lock to the related extent to alter it
    Pointer<StaticIndex> m_index; // the static index
    Pointer<Gate> m_locks; // array of locks, to protect access to the single chunks of the PMA
    Knobs m_knobs; // APMA settings
    common::Detector m_detector; // Record updates
    CachedDensityBounds m_density_bounds0; // user thresholds (for num_segments<=balanced_thresholds_cutoff())
    CachedDensityBounds m_density_bounds1; // primary thresholds (for num_segmnets>balanced_thresholds_cutoff())
    bool m_primary_densities = false; // use the primary thresholds?
    common::CachedMemoryPool m_memory_pool;
    RebalancingMaster* m_rebalancer;
    GarbageCollector* m_garbage_collector; // garbage collector
    ThreadContextList m_thread_contexts; // the list of thread contexts, to keep track of the thread epochs
    const uint64_t m_segments_per_lock; // number of contiguous segments per lock

    // Check this is the correct lock
    bool check_fence_keys(Gate& gate, uint64_t& gate_id, int64_t key) const;

    // Common procedures for concurrency
    Gate* writer_on_entry(int64_t key);
    void writer_on_exit(Gate* gate, bool is_insert, bool is_delete, bool rebalance);
    Gate* reader_on_entry(int64_t key, int64_t gate_id = -1) const;
    void reader_on_exit(Gate* gate) const;

    /**
     * State machine to insert the given key/value
     */
    Gate* insert_on_entry(int64_t key);
    bool do_insert(Gate* gate, int64_t key, int64_t value);
    void insert_on_exit(Gate* gate);

    /**
     * State machine to remove an element from the data structure
     */
    Gate* remove_on_entry(int64_t key);
    bool do_remove(Gate* gate, int64_t key, int64_t* out_value);
    void remove_on_exit(Gate* gate, bool successful, bool global_rebalance);

    /**
     * State machine to find an element in the data structure
     */
    Gate* find_on_entry(int64_t key) const;
    int64_t do_find(Gate* gate, int64_t key) const;
    void find_on_exit(Gate* gate) const;

    /**
     * State machine for the method #sum
     */
    Gate* sum_on_entry(uint64_t gate_id, int64_t min, int64_t max, bool* out_readall) const;
    void do_sum(uint64_t start_gate, int64_t& next_min, int64_t max, ::data_structures::Interface::SumResult* __restrict result) const;
    void sum_on_exit(Gate* gate) const;

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA at the given segment_id
    bool insert_common(size_t segment_id, int64_t key, int64_t value);

    // Insert an element in the given segment. It assumes that there is still room available
    // It returns true if the inserted key is the minimum in the interval
    bool storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value);

    // Set the separator key for the given segment
    void set_separator_key(size_t segment_id, int64_t key);

    // Perform a rebalance operation with the current client
    bool rebalance_local(size_t segment_id, int64_t* key, int64_t* value);

    // Perform a rebalance operation with the RebalancingMaster
    void rebalance_global(Gate* gate);

    // Determine the window to rebalance
    bool rebalance_find_window(size_t segment_id, bool is_insert, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const;

    // Determine whether to rebalance or resize the underlying storage
    RebalancePlan rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const;

    // Determine the size of each partition according to the APMA algorithm
    void rebalance_run_apma(RebalancePlan& md, bool fill_segments, int64_t storage_window_length = -1);

    // Perform the rebalancing action
    void do_rebalance_local(const RebalancePlan& action);

    // Retrieve the lower & higher thresholds of the calibrator tree
    std::pair<double, double> get_thresholds(int height) const;

    // Reset the thresholds for the calibrator tree
    void set_thresholds(int height_calibrator_tree);

    // Update the thresholds for the calibrator tree
    void set_thresholds(const RebalancePlan& md);

    // Rebuild the underlying storage to hold m_elements
    void resize_local(const RebalancePlan& action);

    // Spread (with rewiring) the elements in the given window
    void resize_rebalance(const RebalancePlan& action);

    // Spread (without rewiring) the elements in the given window
    void spread_local(const RebalancePlan& action);
    struct spread_detector_record{ int64_t m_position; int64_t m_predecessor; int64_t m_successor; };
    void spread_load(const RebalancePlan& action, int64_t* __restrict keys_to, int64_t* __restrict values_to, int64_t* out_insert_position = nullptr);
    void spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);
    void spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);
    size_t spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value);

    // Helper, create the data structure detector record, based on the position the new key has been inserted
    spread_detector_record spread_create_detector_record(int64_t* keys, int64_t size, int64_t position);

    // Retrieve the number of segments after that the primary thresholds are used
    size_t balanced_thresholds_cutoff() const;

    // Dump the content of the locks
    void dump_locks(std::ostream& out, bool* integrity_check) const;

    // Dump the content of the storage
    void dump_storage(std::ostream& out, bool* integrity_check) const;

protected:
    // Find the position of the key in the given segment, or return -1 if not found.
    // Helper for the class Weights. This method is not thread safe.
    int find_position(size_t segment_id, int64_t key) const noexcept;

public:
    /**
     * Create a new instance of this data structure
     * @param indexB fixed number of elements in the static index (block size)
     * @param pma_segment_size the maximum capacity, in terms of keys, of a single segment
     * @param pages_per_extent the number of contiguous O.S. pages that compose an extent, the minimum granularity for rewiring
     * @param num_worker_threads the number of workers in the rebalancer
     * @param segments_per_lock the number of continguous segments protected by a single lock/gate
     */
    PackedMemoryArray(size_t index_B, size_t pma_segment_size, size_t pages_per_extent, size_t num_worker_threads, size_t segments_per_lock);

    /**
     * Destructor
     */
    virtual ~PackedMemoryArray();

    /**
     * Insert the given key/value
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Remove the given key from the data structure. Returns its value if found, otherwise -1.
     */
    int64_t remove(int64_t key) override;

    /**
     * Is this data structure empty
     */
    bool empty() const noexcept;

    /**
     * Retrieve the number of elements stored
     */
    virtual size_t size() const noexcept override;

    /**
     * Find the element with the given `key'. It returns its value if found, otherwise the value -1.
     * In case of duplicates, which element is returned is unspecified.
     */
    virtual int64_t find(int64_t key) const override;

    /**
     * Retrieve all elements in the range [min, max].
     */
    virtual std::unique_ptr<::data_structures::Iterator> find(int64_t min, int64_t max) const override;

    /**
     * Sum all elements in the range [min, max]
     */
    virtual ::data_structures::Interface::SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Return an iterator over all elements of the PMA
     */
    virtual std::unique_ptr<::data_structures::Iterator> iterator() const override;

    /**
     * Accessor to the underlying memory pool
     */
    common::CachedMemoryPool& memory_pool();

    /**
     * Accessor to the underlying predictor/detector
     */
    common::Detector& detector();

    /**
     * APMA settings
     */
    Knobs& knobs();

    /**
     * Retrieve the densities currently in use
     */
    const CachedDensityBounds& get_thresholds() const;

    /**
     * Retrieve the maximum capacity (in terms of number of elements) of a segment
     */
    size_t get_segment_capacity() const noexcept;

    /**
     * Instance to the garbage collector
     */
    GarbageCollector* GC() const noexcept;

    /**
     * Retrieve the context for the current thread
     */
    ThreadContext* get_context() const;

    /**
     * Retrieve the granularity of a single lock, in terms of number of contiguous segments
     */
    size_t get_segments_per_lock() const noexcept;

    /**
     * Retrieve the current number of locks
     */
    size_t get_number_locks() const noexcept;

    /**
     * Set the maximum number of worker threads
     */
    void set_max_number_workers(size_t num_workers);

    /**
     * Register the client thread
     */
    void register_thread(uint32_t client_id);

    /**
     * Unregister the client thread
     */
    void unregister_thread();

    /**
     *  Dump the content of the data structure (for debugging purposes)
     *  This method is not thread safe
     */
    virtual void dump(std::ostream& output_stream) const;
    virtual void dump() const override; // to stdout

    /**
     * Callbacks for the parallel interface
     */
    void on_init_main(int num_threads) override;
    void on_init_worker(int worker_id) override;
    void on_destroy_worker(int worker_id) override;
    void on_destroy_main() override;

    /**
     * Memory footprint
     */
    size_t memory_footprint() const override;
};

} // namespace
