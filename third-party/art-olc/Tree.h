//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H

#include "N.h"

namespace ART_OLC {


    class Tree {
    private:
        N *const root;
        mutable Epoche epoche{256};

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   bool &needRestart);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint8_t fillKey, uint32_t &level, bool &needRestart);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, bool &needRestart);

        void* findLessOrEqual(const Key& key, N* node, uint32_t level, bool& needRestart) const;

    public:

        Tree();

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root) { }

        ~Tree();

        ThreadInfo getThreadInfo(uint64_t thread_id) const;

        void setNumThreads(uint64_t n);

        uint64_t getNumThreads() const;

        void* lookup(int64_t key, ThreadInfo &threadEpocheInfo) const;

//        bool lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[], std::size_t resultLen,
//                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

        void insert(int64_t key, void* value, ThreadInfo &epocheInfo);

        void* remove(int64_t key, ThreadInfo &epocheInfo);

        void* findLessOrEqual(int64_t key, ThreadInfo &epocheInfo) const;

        void dump() const;
    };
}
#endif //ART_OPTIMISTICLOCK_COUPLING_N_H
