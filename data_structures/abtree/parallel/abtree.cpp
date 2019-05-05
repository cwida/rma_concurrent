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

#include "abtree.hpp"

#include <cstdlib>
#include <cstring> // memcpy, memset
#include <iomanip>
#include <iostream>
#include <mutex>

#include "common/miscellaneous.hpp"

using namespace std;
using namespace common;

namespace data_structures::abtree::parallel {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    static std::mutex mutex_cout_debug;
    #define COUT_DEBUG(msg) { scoped_lock<mutex> lock(mutex_cout_debug); cout << "[ABTree::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << endl; }
#else
    #define COUT_DEBUG(msg)
#endif



/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
ABTree::ABTree(uint64_t leaf_block_size) : m_first(nullptr), m_leaf_block_size(leaf_block_size),
        m_cardinality(0), m_thread_contexts(), m_garbage_collector(m_thread_contexts) {
    m_first = create_leaf();
    m_garbage_collector.start();

    on_init_main(1);
}

ABTree::~ABTree() {
    m_garbage_collector.stop();

    // delete the remaining leaves
    Leaf* leaf = m_first;
    while(leaf != nullptr){
        Leaf* pointer = leaf;
        leaf = leaf->m_next; // next element

        free(pointer);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/

size_t ABTree::size() const {
    return m_cardinality;
}

bool ABTree::empty() const noexcept {
    return m_cardinality == 0;
}

size_t ABTree::memsize_leaf() const {
    return sizeof(Leaf) + sizeof(int64_t) * 2 /* key/value */ * m_leaf_block_size;
}

ABTree::Leaf* ABTree::create_leaf() {
    static_assert(!is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + latch + ptr next");

    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_leaf());
    if(rc != 0) throw std::runtime_error("ABTree::create_leaf, cannot obtain a chunk of aligned memory");
    memset(ptr, 0, sizeof(Leaf)); // set all fields (cardinality, latch, next) to 0

    return ptr;
}

int64_t ABTree::get_pivot(Leaf* pointer) const {
    Leaf* leaf = reinterpret_cast<Leaf*>(pointer);
    assert(leaf->m_cardinality > 0 && "This leaf is empty!");
    return KEYS(leaf)[0];
}

int64_t* ABTree::KEYS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

int64_t* ABTree::VALUES(const Leaf* leaf) const {
    return KEYS(leaf) + m_leaf_block_size;
}

void ABTree::validate_entry_leaf(int64_t key, Leaf*& leaf, WriteLatch& latch) {
    COUT_DEBUG("key: " << key << ", leaf: " << leaf);

    if(leaf->m_cardinality == 0){
        if(leaf == m_first) {
            assert(empty() && "The whole tree should be empty");
            return; // only the first leaf is allowed to be empty (that is, the whole tree is empty)
        } else {
            throw Latch::Abort{}; // restart
        }
    } else if (key < get_pivot(leaf)){ // great, it should have gone to the previous leaf
        if(leaf != m_first) throw Latch::Abort{}; // restart
    } else { // shall we follow the next leaf?
        while(leaf->m_next != nullptr && KEYS(leaf)[leaf->m_cardinality -1] < key){
            WriteLatch follow (leaf->m_next->m_latch);
            if(leaf->m_next->m_cardinality == 0){ // recycle an empty leaf at the end of the linked list
                follow.invalidate();
                m_garbage_collector.mark(leaf->m_next);
                leaf->m_next = nullptr;
            } else {
                leaf = leaf->m_next;
                latch = move(follow);
            }
        }

    }
}

void ABTree::validate_entry_leaf(int64_t key, Leaf*& leaf, ReadLatch& latch) const {
//    COUT_DEBUG("entry key: " << key << ", leaf: " << leaf);
    if(leaf->m_cardinality == 0){
//        COUT_DEBUG("branch #1, cardinality 0");
        if(leaf == m_first) {
            assert(empty() && "The whole tree should be empty");
            return; // only the first leaf is allowed to be empty (that is, the whole tree is empty)
        } else {
            throw Latch::Abort{}; // restart
        }
    } else if (key < get_pivot(leaf)){ // great, it should have gone to the previous leaf
//        COUT_DEBUG("branch #2, key: " << key << "< pivot: " << get_pivot(leaf));
        if(leaf != m_first) throw Latch::Abort{}; // restart
    } else { // shall we follow the next leaf?
//        COUT_DEBUG("branch #3, next leaf?");
        while(leaf->m_next != nullptr && KEYS(leaf)[leaf->m_cardinality -1] < key){
            ReadLatch follow (leaf->m_next->m_latch);
            if(leaf->m_next->m_cardinality > 0){
                leaf = leaf->m_next;
                latch = move(follow);
            }
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *   Secondary index                                                         *
 *                                                                           *
 *****************************************************************************/
void ABTree::index_insert(int64_t key, Leaf* value){
//    COUT_DEBUG("key: " << key << ", value: " << value);
    assert(value != nullptr && "Invalid value!");
    ART_OLC::ThreadInfo ti = m_index.getThreadInfo(ThreadContext::thread_id);
    m_index.insert(key, value, ti);
}

void ABTree::index_remove(int64_t key){
//    COUT_DEBUG("key: " << key);

    ART_OLC::ThreadInfo ti = m_index.getThreadInfo(ThreadContext::thread_id);
    m_index.remove(key, ti);
}

ABTree::Leaf* ABTree::index_find_leq(int64_t key) const {
    ART_OLC::ThreadInfo ti = m_index.getThreadInfo(ThreadContext::thread_id);
    Leaf* value = (Leaf*) m_index.findLessOrEqual(key, ti);
    COUT_DEBUG("key: " << key << ", leaf: " << value);

    if (value == nullptr)
        return m_first;
    else
        return value;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

void ABTree::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);

    bool success = false;
    do {
        ScopedContext context { m_thread_contexts }; // join a new epoch
        try {
            do_insert(key, value);
            success = true;
        } catch(Latch::Abort){ /* try again ... */ }
    } while (!success);

//#if defined(DEBUG)
//    dump();
//#endif
}

void ABTree::do_insert(int64_t key, int64_t value){
    Leaf* leaf = index_find_leq(key);
    WriteLatch latch(leaf->m_latch);
    validate_entry_leaf(key, /* in/out */ leaf, /* in/out*/ latch);

    if(m_first == leaf && leaf->m_cardinality == 0){
        leaf_insert_element(m_first, key, value);
        index_insert(key, leaf); // great, the tree is empty
    } else if (leaf->m_cardinality == m_leaf_block_size) { // this leaf is full
        auto l2_pivot_addr = leaf_split(leaf); // split in half, returns the pivot and the address of the new leaf

        // where do we need to insert the new element ?
        if(key >= l2_pivot_addr.first){
           leaf_insert_element(l2_pivot_addr.second, key, value);
           // the pivot is still the old one as key >= l2_pivot_addr.first
        } else {
           leaf_handle_insert(leaf, key, value);
        }

        index_insert(l2_pivot_addr.first, l2_pivot_addr.second);
    } else { // standard case
        leaf_handle_insert(leaf, key, value);
    }

}

void ABTree::leaf_handle_insert(Leaf* leaf, int64_t key, int64_t value){
    assert(leaf->m_cardinality > 0 && "This leaf is empty");
    assert(leaf->m_cardinality < m_leaf_block_size && "This leaf is full");

    int64_t pivot_old = KEYS(leaf)[0];
    int64_t pivot_new = leaf_insert_element(leaf, key, value);

    // update the radix tree
    if(pivot_old != pivot_new){
        index_remove(pivot_old);
        index_insert(pivot_new, leaf);
    }
}

int64_t ABTree::leaf_insert_element(Leaf* leaf, int64_t key, int64_t value){
    COUT_DEBUG("leaf: " << leaf << ", key: " << key << ", value: " << value);
    assert(leaf->m_cardinality < m_leaf_block_size && "The leaf is full");
    size_t i = leaf->m_cardinality;
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);
    while(i > 0 && keys[i-1] > key){
        keys[i] = keys[i-1];
        values[i] = values[i-1];
        i--;
    }
    keys[i] = key;
    values[i] = value;

    leaf->m_cardinality++;
    m_cardinality += 1;

    return keys[0];
}

pair<int64_t, ABTree::Leaf*> ABTree::leaf_split(Leaf* l1){
    assert(l1->m_cardinality <= m_leaf_block_size && "Invalid cardinality");
    assert(l1->m_cardinality == m_leaf_block_size && "The capacity of this leaf is not filled, no reason to split!");
    COUT_DEBUG("split: " << l1);

    Leaf* l2 = create_leaf();

    size_t thres = (l1->m_cardinality +1) /2;
    l2->m_cardinality = l1->m_cardinality - thres;
    l1->m_cardinality = thres;

    // move the elements from l1 to l2
    memcpy(KEYS(l2), KEYS(l1) + thres, l2->m_cardinality * sizeof(KEYS(l2)[0]));
    memcpy(VALUES(l2), VALUES(l1) + thres, l2->m_cardinality * sizeof(VALUES(l2)[0]));

    // adjust the links
    l2->m_next = l1->m_next;
    l1->m_next = l2;

    int64_t pivot2 = KEYS(l2)[0];

    return {pivot2, l2};
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t ABTree::remove(int64_t key) {
    COUT_DEBUG("key: " << key);
    int64_t result { -1 };

    bool success = false;
    do {
        ScopedContext context { m_thread_contexts }; // join a new epoch
        try {
            do_remove(key, &result);
            success = true;
        } catch(Latch::Abort){ /* try again ... */ }
    } while (!success);

    return result;
}

void ABTree::do_remove(int64_t key, int64_t* out_value) {
    int64_t value = -1; // the payload associated to the key removed

    Leaf* leaf = index_find_leq(key);
    WriteLatch latch(leaf->m_latch);
    validate_entry_leaf(key, leaf, latch);
    if(leaf->m_cardinality > 0){
        bool update_min;
        value = leaf_remove_element(key, leaf, &update_min); // updates leaf->m_cardinality and m_cardinality

        if(update_min){
            index_remove(key);
            if(leaf->m_cardinality > 0){ index_insert(get_pivot(leaf), leaf); };
        }

        leaf_rebalance(leaf);
    }

    if(out_value != nullptr)
        *out_value = value;

//#if defined(DEBUG)
//    dump();
//#endif
}

int64_t ABTree::leaf_remove_element(int64_t key, Leaf* leaf, bool* out_update_min){
    assert(leaf != nullptr && leaf->m_cardinality > 0 && "Empty leaf");
    COUT_DEBUG("key: " << key << ", leaf: " << leaf);
    bool update_min = false;
    int64_t value = -1;

    int64_t N = leaf->m_cardinality;
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);

    if(keys[N-1] >= key){
        int64_t i = 0;
        while(i < N && keys[i] < key) i++;
        if(i < N && keys[i] == key){
            value = values[i];
            for(size_t j = i; j < N -1; j++){
                keys[j] = keys[j+1];
                values[j] = values[j+1];
            }
            leaf->m_cardinality -= 1;
            m_cardinality--;
            update_min = (i == 0);
        }
    }

    if(out_update_min != nullptr){ *out_update_min = update_min; }
    return value;
}

void ABTree::leaf_rebalance(Leaf* leaf){
    if( leaf->m_cardinality >= m_leaf_block_size/2 /* a rebalance is needed ? */
        || leaf->m_next == nullptr /* there is no leaf to merge with */) return;

    const int64_t lowerbound = m_leaf_block_size/2;
    int64_t need = lowerbound - leaf->m_cardinality; // amount of elements needed

    Leaf* sibling = leaf->m_next;
    WriteLatch latch_sibling(sibling->m_latch);

    if(sibling->m_cardinality >= lowerbound + need){
        leaf_share(leaf, need); // steal `need' elements from its right sibling
    } else {
        leaf_merge(leaf); // merge the current leaf with its sibling

        // invalidate the sibling node through its latch
        latch_sibling.invalidate();
        m_garbage_collector.mark(sibling);
    }
}

void ABTree::leaf_share(Leaf* leaf, int64_t need){
    assert(leaf != nullptr && "Null pointer (leaf)");
    assert(leaf->m_next != nullptr && "Null pointer (right link)");
    assert(leaf->m_next->m_cardinality >= m_leaf_block_size/2 + need && "The right sibling doesn't contain `need' elements to share");

    Leaf* l1 = leaf;
    Leaf* l2 = leaf->m_next;

    int64_t* __restrict l1_keys = KEYS(l1);
    int64_t* __restrict l1_values = VALUES(l1);
    int64_t* __restrict l2_keys = KEYS(l2);
    int64_t* __restrict l2_values = VALUES(l2);

    // remove the current separator key from the index
    index_remove(l2_keys[0]);

    // move `need' elements of l2 in l1
    for(size_t i = 0; i < need; i++){
        l1_keys[l1->m_cardinality + i] = l2_keys[i];
        l1_values[l1->m_cardinality + i] = l2_values[i];
    }

    // left shift elements by `need' in l2
    for(int64_t i = 0, sz = l2->m_cardinality -need; i < sz; i++){
        l2_keys[i] = l2_keys[i+need];
        l2_values[i] = l2_values[i+need];
    }

    // re-insert the new separator key in the index
    index_insert(l2_keys[0], l2);

    // update the cardinalities of the nodes
    l1->m_cardinality += need;
    l2->m_cardinality -= need;
}

void ABTree::leaf_merge(Leaf* leaf){
    assert(leaf != nullptr && "Null pointer (leaf)");
    assert(leaf->m_next != nullptr && "Null pointer (leaf's sibling)");
    assert(leaf->m_cardinality + leaf->m_next->m_cardinality <= m_leaf_block_size && "Overflow");

    Leaf* l1 = leaf;
    Leaf* l2 = leaf->m_next;

    // move all elements from l2 to l1
    memcpy(KEYS(l1) + l1->m_cardinality, KEYS(l2), l2->m_cardinality * sizeof(KEYS(l2)[0]));
    memcpy(VALUES(l1) + l1->m_cardinality, VALUES(l2), l2->m_cardinality * sizeof(VALUES(l2)[0]));

    // update the cardinalities of the two leaves
    l1->m_cardinality += l2->m_cardinality;
    l2->m_cardinality = 0;

    // adjust the links
    l1->m_next = l2->m_next;

    // update the index
    index_remove(KEYS(l2)[0]);

    // free the memory from l2
//    free(l2); l2 = nullptr; // let it do by the invoker after the node is invalidated
}

/*****************************************************************************
 *                                                                           *
 *   Lookup                                                                  *
 *                                                                           *
 *****************************************************************************/

int64_t ABTree::find(int64_t key) const {
    int64_t value { -1 };

    bool success = false;
    do {
        ScopedContext context { m_thread_contexts }; // join a new epoch
        try {
            value = do_find(key);
            success = true;
        } catch(Latch::Abort){ /* try again ... */ }
    } while (!success);

    return value;
}

int64_t ABTree::do_find(int64_t key) const {
    Leaf* leaf = index_find_leq(key);
//    COUT_DEBUG("Lookup: " << key << ", leaf: " << leaf);

    ReadLatch latch{ leaf->m_latch };
    validate_entry_leaf(key, leaf, latch);

    int64_t index = leaf_find(leaf, key);
//    COUT_DEBUG("key: " << key << ", leaf: " << leaf << ", index: " << index);
    if(index < 0) return -1;
    return VALUES(leaf)[index];
}

int64_t ABTree::leaf_find(Leaf* leaf, int64_t key) const noexcept {
//    COUT_DEBUG("leaf: " << leaf << ", key: " << key);
    size_t i = 0, N = leaf->m_cardinality;
    int64_t* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;

    return (i < N && keys[i] == key) ? i : -1;
}

/*****************************************************************************
 *                                                                           *
 *   Iterator (for debugging purposes)                                       *
 *                                                                           *
 *****************************************************************************/
std::unique_ptr<data_structures::Iterator> ABTree::iterator() const {
    return std::unique_ptr<data_structures::Iterator> { new ABTree::Iterator{this} };
}

ABTree::Iterator::Iterator(const ABTree* tree) :
        m_tree(tree), m_leaf(tree->m_first), m_latch(m_leaf->m_latch), m_position(0){ }

ABTree::Iterator::~Iterator() { }

bool ABTree::Iterator::hasNext() const {
    return m_position < m_leaf->m_cardinality;
}

std::pair<int64_t, int64_t> ABTree::Iterator::next() {
    assert(m_position < m_leaf->m_cardinality);

    std::pair<int64_t, int64_t> result;
    result.first = m_tree->KEYS(m_leaf)[m_position];
    result.second = m_tree->VALUES(m_leaf)[m_position];
    m_position++;

    // move to the next leaf
    if(m_position >= m_leaf->m_cardinality && m_leaf->m_next != nullptr){
        Leaf* sibling = m_leaf->m_next;
        m_latch.traverse(sibling->m_latch);
        m_leaf = sibling;
        m_position = 0;
    }

    return result;
}


/*****************************************************************************
 *                                                                           *
 *   Range scan                                                              *
 *                                                                           *
 *****************************************************************************/
data_structures::Interface::SumResult ABTree::sum(int64_t min, int64_t max) const {
    SumResult result;

    if(min <= max) {
        bool success = false;
        do {
            ScopedContext context { m_thread_contexts };
            try {
                result = do_sum(min, max);
                success = true;
            } catch (Latch::Abort){ /* try again.. */ }
        } while (!success);
    }

    return result;
}

data_structures::Interface::SumResult ABTree::do_sum(int64_t min, int64_t max) const {
    Leaf* leaf = index_find_leq(min);
    COUT_DEBUG("min: " << min << ", max: " << max << ", entry: " << leaf);
    assert(leaf != nullptr);

    // here is where exceptions can fire..
    ReadLatch latch { leaf->m_latch };
    validate_entry_leaf(min, leaf, latch);

    // we are safe now, no need to check to exploit the mechanism of epochs
    m_thread_contexts.my_context().bye();

    // edge case, the interval should have started before this leaf
    if (leaf->m_cardinality == 0 || KEYS(leaf)[0] > max || ! ::data_structures::global_parallel_scan_enabled) return SumResult{};

    // standard case, find the first key that satisfies the interval
    int64_t i = 0;
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);
    int64_t N = leaf->m_cardinality;
    while(i < N && keys[i] < min) i++;

    SumResult result;
    result.m_first_key = keys[i];

    do {
        if(KEYS(leaf)[N-1] <= max) { // the max is not in this leaf, read everything
            while(i < N){
                assert(keys[i] <= max && "Key outside the search range [min, max]");
                result.m_sum_keys += keys[i];
                result.m_sum_values += values[i];
                result.m_num_elements++;
                i++;
            }
            result.m_last_key = keys[i -1]; // just in case
        } else { // the max must be in this leaf
            while(i < N && keys[i] <= max){
                assert(keys[i] <= max && "Key outside the search range [min, max]");
                result.m_sum_keys += keys[i];
                result.m_sum_values += values[i];
                result.m_num_elements++;
                i++;
            }
        }

        if(i > 0)
            result.m_last_key = keys[i -1]; // just in case

        // move to the next leaf
        if(i >= N && leaf->m_next != nullptr && ::data_structures::global_parallel_scan_enabled){
            leaf = leaf->m_next;

            // lock coupling: acquire the latch for the new leaf, release the latch for the old leaf
            latch.traverse(leaf->m_latch);

            keys = KEYS(leaf);
            values = VALUES(leaf);
            i = 0;
            N = leaf->m_cardinality;
            if(N == 0 || keys[0] > max) leaf = nullptr; // we're done
        } else {
            leaf = nullptr;
        }

        if(leaf != nullptr){
            // prefetch the next next leaf :!)
            PREFETCH(leaf->m_next);
            // prefetch the first two blocks for the keys
            PREFETCH(KEYS(leaf->m_next));
            PREFETCH(KEYS(leaf->m_next) + 8);
            // prefetch the first two blocks for the values
            PREFETCH(VALUES(leaf->m_next));
            PREFETCH(VALUES(leaf->m_next) + 8);
        }
    } while (leaf != nullptr);

    return result;
}

/******************************************************************************
 *                                                                            *
 *   Dump                                                                     *
 *                                                                            *
 *****************************************************************************/

static void dump_tabs(int depth){
    auto flags = cout.flags();
    cout << setw(depth * 2) << setfill(' ') << ' ';
    cout.setf(flags);
};

void ABTree::dump_leaves() const {
    size_t i = 0;
    Leaf* leaf = m_first;
#if !defined(NDEBUG)
    int64_t key_previous = numeric_limits<int64_t>::min();
#endif

    while(leaf != nullptr){
        cout << "[Leaf #" << i << "] " << leaf << ", cardinality: " << leaf->m_cardinality << ", latch: " << leaf->m_latch.value() << ", next: " << leaf->m_next << "\n";

        dump_tabs(2);
        for(size_t i = 0; i < leaf->m_cardinality; i++){
            if(i > 0) cout << ", ";
            cout << "<" << KEYS(leaf)[i] << ", " << VALUES(leaf)[i] << ">";

#if !defined(NDEBUG)
            assert(key_previous <= KEYS(leaf)[i] && "Sorted order not respected");
            key_previous = KEYS(leaf)[i];
#endif
        }
        cout << "\n";

        // move to the next leaf
        leaf = leaf->m_next;
        i++;
    }
}

void ABTree::dump() const {
    cout << "Parallel/ABTree layout, leaf_block_size: " << m_leaf_block_size << ", cardinality: " << size() << endl;

    // Dump the index
    cout << "Secondary index: " << endl;
    m_index.dump();

    // Dump the leaves
    cout << "Leaves: " << endl;
    dump_leaves();
}

/*****************************************************************************
 *                                                                           *
 *   Parallel Callbacks                                                      *
 *                                                                           *
 *****************************************************************************/
void ABTree::on_init_main(int num_threads) {
    // in ART the number of threads cannot be reduced
    if(m_index.getNumThreads() < num_threads) {
        m_index.setNumThreads(num_threads);
    }

    m_thread_contexts.resize(num_threads);
}
void ABTree::on_init_worker(int worker_id) {
    COUT_DEBUG("ENTER: " << worker_id);
    ThreadContext::thread_id = worker_id;
}
void ABTree::on_destroy_worker(int worker_id) {
    COUT_DEBUG("EXIT: " << worker_id);
    ThreadContext::thread_id = -1;
}
void ABTree::on_destroy_main() {
//    m_index.setNumThreads(0); // the number of threads cannot be reset
    m_thread_contexts.resize(0);
}


} // namespace abtree::parallel
