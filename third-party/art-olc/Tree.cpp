#include <assert.h>
#include <algorithm>
#include <functional>
#include "N_impl.hpp"
#include "Tree.h"
#include "Key.h"

namespace ART_OLC {

    Tree::Tree() : root(new N256( nullptr, 0)) { }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo(uint64_t thread_id) const {
        if(thread_id >= epoche.getNumThreads()) throw std::invalid_argument("invalid thread_id");
        return ThreadInfo(this->epoche, thread_id);
    }

    void Tree::setNumThreads(uint64_t n){
        epoche.setNumThreads(n);
    }

    uint64_t Tree::getNumThreads() const {
        return epoche.getNumThreads();
    }

    void* Tree::lookup(int64_t key, ThreadInfo& threadEpocheInfo) const {
        Key encoded_key = Key::encode(key);
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
restart:
        bool needRestart = false;

        N *node;
        N *parentNode = nullptr;
        uint64_t v;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        node = root;
        v = node->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        while (true) {
            switch (checkPrefix(node, encoded_key, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (encoded_key.getKeyLen() <= level) {
                        return 0;
                    }
                    parentNode = node;
                    node = N::getChild(encoded_key[level], parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart) goto restart;

                    if (node == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        Leaf* tid = N::getLeaf(node);
                        if(tid->m_key == key){ // this is the searched key?
                            return tid->m_value;
                        } else {
                            return nullptr;
                        }
                    }
                    level++;
            }
            uint64_t nv = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            v = nv;
        }
    }

//    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo) {
    void Tree::insert(int64_t key, void* value, ThreadInfo &epocheInfo) {
        Key encoded_key = Key::encode(key);
        Leaf* leaf = new Leaf{key, value}; // the item to insert
        EpocheGuard epocheGuard(epocheInfo);
restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            auto res = checkPrefixPessimistic(node, encoded_key, nextLevel, nonMatchingKey, remainingPrefix, needRestart); // increases level
            if (needRestart) goto restart;
            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {
                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    if (needRestart) goto restart;

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
                        goto restart;
                    }
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level);

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(encoded_key[nextLevel], N::setLeaf(leaf));
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));

                    node->writeUnlock();
                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = encoded_key[level];
            nextNode = N::getChild(nodeKey, node);
            node->checkOrRestart(v,needRestart);
            if (needRestart) goto restart;

            if (nextNode == nullptr) {
                N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(leaf), needRestart, epocheInfo);
                if (needRestart) goto restart;
                return;
            }

            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) goto restart;
            }

            if (N::isLeaf(nextNode)) {
                node->upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart) goto restart;

                Key key_sibling = Key::encode(N::getLeaf(nextNode)->m_key);

                level++;
                uint32_t prefixLength = 0;
                while (key_sibling[level + prefixLength] == encoded_key[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(&encoded_key[level], prefixLength);
                n4->insert(encoded_key[level + prefixLength], N::setLeaf(leaf));
                n4->insert(key_sibling[level + prefixLength], nextNode);
                N::change(node, encoded_key[level - 1], n4);
                node->writeUnlock();
                return;
            }
            level++;
            parentVersion = v;
        }
    }

//    void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
    void* Tree::remove(int64_t key, ThreadInfo &threadInfo) {
        Key encoded_key = Key::encode(key);
        EpocheGuard epocheGuard(threadInfo);
restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, encoded_key, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return nullptr; // not found
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = encoded_key[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return nullptr;
                    }
                    if (N::isLeaf(nextNode)) {
                        Leaf* leaf = N::getLeaf(nextNode);
                        if (leaf->m_key != key) {
                            return nullptr; // not found
                        }
                        void* return_value = leaf->m_value;
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();

                                secondNodeN->addPrefixBefore(node, secondNodeK);
                                secondNodeN->writeUnlock();

                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            }
                        } else {
                            N::removeAndUnlock(node, v, encoded_key[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
                            if (needRestart) goto restart;
                        }
                        this->epoche.markNodeForDeletion(leaf, threadInfo);
                        return return_value;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (n->hasPrefix()) {
            if (k.getKeyLen() <= level + n->getPrefixLength()) {
                return CheckPrefixResult::NoMatch;
            }
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level = level + (n->getPrefixLength() - maxStoredPrefixLength);
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        bool &needRestart) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    Leaf* leaf = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return CheckPrefixPessimisticResult::Match;
                    kt = Key::encode(leaf->m_key);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (n->getPrefixLength() > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            Leaf* leaf = N::getAnyChildTid(n, needRestart);
                            if (needRestart) return CheckPrefixPessimisticResult::Match;
                            kt = Key::encode(leaf->m_key);
                        }
                        memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                                           maxStoredPrefixLength));
                    } else {
                        memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, n->getPrefixLength() - i - 1);
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                          bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    Leaf* leaf = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCCompareResults::Equal;
                    kt = Key::encode(leaf->m_key);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                      bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    Leaf* leaf = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCEqualsResults::BothMatch;
                    kt = Key::encode(leaf->m_key);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }


    void* Tree::findLessOrEqual(const Key& key, N* node, uint32_t level, bool& needRestart) const {
//        std::cout << "[Tree::findLessOrEqual] key: " << key << ", node: " << node << ", level: " << level << std::endl;

        assert(node != nullptr);
        assert(needRestart == false);
        uint64_t node_version = node->readLockOrRestart(needRestart); if (needRestart) return nullptr;
        void* result { nullptr };

        // first check the damn prefix
        auto prefixResult = checkPrefixCompare(node, key, 0, level, needRestart);
        if (needRestart) return nullptr; // it doesn't matter what it returns, it needs to restart the search again
        switch(prefixResult){
        case PCCompareResults::Smaller: {
            // counterintuitively, it means that the prefix of the node is lesser than the key
            // i.e. the key is bigger than any element in this node
            Leaf* leaf = N::getMaxLeaf(node, node_version, needRestart); if(needRestart) return nullptr;
            return leaf->m_value;
        } break;
        case PCCompareResults::Equal: {
            /* nop */
        } break;
        case PCCompareResults::Bigger: {
            // counterintuitively, it means that the prefix of the node is greater than the key
            // ask the parent to return the max for the sibling that precedes this node
            return nullptr;
        } break;
        } // end switch

        // so far so good?
        node->checkOrRestart(node_version, needRestart); if (needRestart) return nullptr;

        // second, find the next node to percolate in the tree
        bool exact_match; // set by N::getChildLessOrEqual as side effect
        auto child = N::getChildLessOrEqual(node, key[level], &exact_match);
        node->checkOrRestart(node_version, needRestart); if (needRestart) return nullptr;

        if(child == nullptr){
            return nullptr; // again, ask the parent to return the maximum of the previous sibling
        } else if (exact_match || N::isLeaf(child)){

            // if we picked a leaf, check whether the search key to search is >= the the leaf's key. If not, our
            // target leaf will be the sibling of the current node
            if(N::isLeaf(child)){
                Leaf* leaf = N::getLeaf(child);
                node->checkOrRestart(node_version, needRestart); if (needRestart) return nullptr;
                if(leaf->m_key <= Key::decode(key)) return leaf->m_value;

                // otherwise, check the sibling ...
            } else {
                // the other case is the current byte is equal to the byte indexing this node, we need to traverse the tree
                // and see whether further down they find a suitable leaf. If not, again we need to check the sibling
                result = findLessOrEqual(key, child, level +1, needRestart);
                if (/* bad luck, restart from scratch */ needRestart || /* item found */ result != nullptr) return result;

                // otherwise check the left sibling ...
            }

            // then the correct is the maximum of the previous sibling
            auto sibling = N::getPredecessor(node, key[level]);

            // is the information we read valid ?
            node->checkOrRestart(node_version, needRestart); if (needRestart) return nullptr;

            if(sibling != nullptr){
                if(N::isLeaf(sibling)){
                    Leaf* leaf = N::getLeaf(sibling);

                    // last check
                    node->readUnlockOrRestart(node_version, needRestart); if (needRestart) return nullptr;

                    result = leaf->m_value;
                } else {
                    auto sibling_version = sibling->readLockOrRestart(needRestart); if (needRestart) return nullptr;

                   Leaf* leaf = N::getMaxLeaf(sibling, sibling_version, needRestart); if (needRestart) return nullptr;

                   result = leaf->m_value;
                }

                return result;
            } else {
                // ask the parent
                return nullptr;
            }
        } else { // key[level] > child[level], but it is lower than all other children => return the max from the given child
            auto child_version = child->readLockOrRestart(needRestart); if(needRestart) return nullptr; // acquire the version of the child
            node->readUnlockOrRestart(node_version, needRestart); if (needRestart) return nullptr; // release the current node
            Leaf* leaf = N::getMaxLeaf(child, child_version, needRestart); if (needRestart) return nullptr;
            return leaf->m_value;
        }
    }

    void* Tree::findLessOrEqual(int64_t key, ThreadInfo &epocheInfo) const {
        bool needRestart;
        void* result {nullptr}; // output
        Key encoded_key = Key::encode(key);
        do {
            EpocheGuard guard{epocheInfo};
            needRestart = false;
            result = findLessOrEqual(encoded_key, root, 0, needRestart);
        } while (needRestart);

        return result;
    }

    void Tree::dump() const {
        N::dump(root, 0, 0);
    }
}
