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
#include <utility>

#include "garbage_collector.hpp"
#include "interface.hpp"
#include "iterator.hpp"
#include "latch.hpp"
#include "parallel.hpp"
#include "thread_context.hpp"

#include "third-party/art-olc/Tree.h"

namespace data_structures::abtree::parallel {

/**
 * A parallel implementation of an B+ tree, with secondary index ART+OLC
 */
class ABTree : public data_structures::Interface, public ParallelCallbacks {
    struct Leaf; // forward declaration
    ART_OLC::Tree m_index; // secondary index
    Leaf* m_first; // the first leaf of the linked list
    const uint64_t m_leaf_block_size; // the size of each block
    std::atomic<uint64_t> m_cardinality; // the total amount of elements stored in the tree
    ThreadContextList m_thread_contexts; // the list of thread contexts, to keep track of the thread epochs
    GarbageCollector m_garbage_collector; // garbage collector

    // Elements are ultimately stored in a linked list of blocks
    struct Leaf {
        // remove the ctors
        Leaf() = delete;
        Leaf(const Leaf&) = delete;
        Leaf& operator=(const Leaf&) = delete;

        mutable Latch m_latch; // concurrency protection
        size_t m_cardinality; // number of elements stored in this Leaf
        Leaf* m_next; // next leaf
    };
    int64_t* KEYS(const Leaf* leaf) const; // retrieve the keys stored in the leaf
    int64_t* VALUES(const Leaf* leaf) const; // retrieve the values stored in the leaf

    // Create a new leaf
    Leaf* create_leaf();

    // Get the memory size of an internal node / leaf
    size_t memsize_leaf() const;

    // Get the minimum [first] key stored in the given leaf
    int64_t get_pivot(Leaf* leaf) const;

    // Find the first element that matches the given key in the leaf. Return the position
    // of the element, or -1 if it's not found
    int64_t leaf_find(Leaf* leaf, int64_t key) const noexcept;

    // Insert the given separator key into the secondary index
    void index_insert(int64_t separator_key, Leaf* payload);

    // Find the leaf having the greatest pivot that is less or equal than the given key
    Leaf* index_find_leq(int64_t key) const;

    // Remove the given key from the index
    void index_remove(int64_t key);

    // Attempt to insert a key/value into the tree. Aborts in case it encounters a deleted node
    void do_insert(int64_t key, int64_t value);

    // Insert the given key in the leaf. Return the current minimum, i.e. the first element, in the leaf
    // The latch to the leaf must be already acquired before invoking this method
    int64_t leaf_insert_element(Leaf* leaf, int64_t key, int64_t value);

    // Insert the key/value in the leaf, update the index properly
    void leaf_handle_insert(Leaf* leaf, int64_t key, int64_t value);

    // Split the given leaf. Return the key & the ptr to the new leaf created.
    std::pair<int64_t, Leaf*> leaf_split(Leaf* leaf);

    // Attempt to remove the given key from the tree. Aborts in case it finds an invalid node along the way
    void do_remove(int64_t key, int64_t* out_value);

    // Remove the element with the given `key' from the leaf, return its value, or -1 if not found
    int64_t leaf_remove_element(int64_t key, Leaf* leaf, bool* out_update_min);

    // Rebalance a leaf, by merging or sharing its elts with one of its siblings, when its cardinality is less than m_leaf_block_size/2
    void leaf_rebalance(Leaf* leaf);
    void leaf_share(Leaf* leaf, int64_t need); // Steal `need' elements from the right sibling
    void leaf_merge(Leaf* leaf); // Merge the leaf with its right sibling

    // Check that this is the correct leaf to contain the given key
    void validate_entry_leaf(int64_t key, Leaf*& leaf, WriteLatch& latch);
    void validate_entry_leaf(int64_t key, Leaf*& leaf, ReadLatch& latch) const;
    // Attempt to find the given key in the tree
    int64_t do_find(int64_t key) const;

    // Iterator
    class Iterator : public data_structures::Iterator {
      const ABTree* m_tree; // instace
      Leaf* m_leaf; // current leaf
      ReadLatch m_latch; // latch held for the current leaf
      int64_t m_position; // position inside the leaf
    public:
      Iterator(const ABTree* tree);
      ~Iterator();
      virtual bool hasNext() const override;
      virtual std::pair<int64_t, int64_t> next() override;
    };
    friend class Iterator;

    // Attempt a range scan in [min, max]
    SumResult do_sum(int64_t min, int64_t max) const;

    // Dump helpers
    void dump_leaves() const;

public:
    /**
     * Initialise the tree
     * @param leaf_block_size: the maximum capacity, in number of elements, of each leaf
     */
    ABTree(uint64_t leaf_block_size = 256);

    /**
     * Destructor
     */
    ~ABTree();

    /**
     * Insert the given key/value in the tree
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Remove the given key from the tree.
     * Returns the value associated to the key removed, or -1 if the key was not found.
     */
    int64_t remove(int64_t key) override;

    /**
     * Find the given key in the tree
     */
    int64_t find(int64_t key) const override;

    /**
     * Scan all elements in the tree
     */
    std::unique_ptr<data_structures::Iterator> iterator() const override;

    /**
     * Sum all elements in the interval [min, max]
     */
    SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Report the current cardinality of the tree
     */
    size_t size() const override;

    /**
     * Returns true if the tree is empty, false otherwise
     */
    bool empty() const noexcept;

    /**
     * Dumps to stdout the content of the tree. This method is not thread-safe.
     */
    void dump() const override;

    /**
     * Callbacks for the parallel interface
     */
    void on_init_main(int num_threads) override;
    void on_init_worker(int worker_id) override;
    void on_destroy_worker(int worker_id) override;
    void on_destroy_main() override;
};

} // data_structures::abtree::parallel
