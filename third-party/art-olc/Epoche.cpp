//
// Created by florian on 22.10.15.
//
#ifndef EPOCHE_CPP
#define EPOCHE_CPP

#include <cassert>
#include <cinttypes>
#include <iostream>
#include <iterator>
#include <mutex>
#include <vector>
#include "Epoche.h"

using namespace ART_OLC;
using namespace std;

DeletionList::~DeletionList() {
    assert(deletitionListCount == 0 && headDeletionList == nullptr);
    LabelDelete *cur = nullptr, *next = freeLabelDeletes;
    while (next != nullptr) {
        cur = next;
        next = cur->next;
        delete cur;
    }
    freeLabelDeletes = nullptr;
}

std::size_t DeletionList::size() {
    return deletitionListCount;
}

void DeletionList::remove(LabelDelete *label, LabelDelete *prev) {
    if (prev == nullptr) {
        headDeletionList = label->next;
    } else {
        prev->next = label->next;
    }
    deletitionListCount -= label->nodesCount;

    label->next = freeLabelDeletes;
    freeLabelDeletes = label;
    deleted += label->nodesCount;
}

void DeletionList::add(void *n, uint64_t globalEpoch) {
    deletitionListCount++;
    LabelDelete *label;
    if (headDeletionList != nullptr && headDeletionList->nodesCount < headDeletionList->nodes.size()) {
        label = headDeletionList;
    } else {
        if (freeLabelDeletes != nullptr) {
            label = freeLabelDeletes;
            freeLabelDeletes = freeLabelDeletes->next;
        } else {
            label = new LabelDelete();
        }
        label->nodesCount = 0;
        label->next = headDeletionList;
        headDeletionList = label;
    }
    label->nodes[label->nodesCount] = n;
    label->nodesCount++;
    label->epoche = globalEpoch;

    added++;
}

LabelDelete *DeletionList::head() {
    return headDeletionList;
}

void Epoche::enterEpoche(ThreadInfo &epocheInfo) {
    unsigned long curEpoche = currentEpoche.load(std::memory_order_relaxed);
    epocheInfo.getDeletionList().localEpoche.store(curEpoche, std::memory_order_release);
}

void Epoche::markNodeForDeletion(void *n, ThreadInfo &epocheInfo) {
    epocheInfo.getDeletionList().add(n, currentEpoche.load());
    epocheInfo.getDeletionList().thresholdCounter++;
}

void Epoche::exitEpocheAndCleanup(ThreadInfo &epocheInfo) {
    DeletionList &deletionList = epocheInfo.getDeletionList();
    if ((deletionList.thresholdCounter & (64 - 1)) == 1) {
        currentEpoche++;
    }
    if (deletionList.thresholdCounter > startGCThreshhold) {
        if (deletionList.size() == 0) {
            deletionList.thresholdCounter = 0;
            return;
        }
        deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());

        uint64_t oldestEpoche = getMinEpoch();

        LabelDelete *cur = deletionList.head(), *next, *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            if (cur->epoche < oldestEpoche) {
                for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                    operator delete(cur->nodes[i]);
                }
                deletionList.remove(cur, prev);
            } else {
                prev = cur;
            }
            cur = next;
        }
        deletionList.thresholdCounter = 0;
    }
}

Epoche::Epoche(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold) {

}

Epoche::~Epoche() {
#if !defined(NDEBUG)
    uint64_t oldestEpoche = getMinEpoch();
#endif
    for (size_t i = 0; i < deletionListsCount; i++) {
        DeletionList& d = deletionLists[i];
        LabelDelete *cur = d.head(), *next, *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            assert(cur->epoche < oldestEpoche);
            for (std::size_t j = 0; j < cur->nodesCount; ++j) {
                operator delete(cur->nodes[j]);
            }
            d.remove(cur, prev);
            cur = next;
        }
    }
}

void Epoche::showDeleteRatio() {
    for (size_t i = 0; i < deletionListsCount; i++) {
        auto& d = deletionLists[i];
        std::cout << "deleted " << d.deleted << " of " << d.added << std::endl;
    }
}

uint64_t Epoche::getMinEpoch() const {
    uint64_t oldest = std::numeric_limits<uint64_t>::max();
    for(size_t i = 0; i < deletionListsCount; i++){
        uint64_t e = deletionLists[i].localEpoche.load();
        if(e < oldest) oldest = e;
    }
    return oldest;
}

uint64_t Epoche::getNumThreads() const {
    return deletionListsCount;
}
void Epoche::setNumThreads(uint64_t n){
    if(deletionListsCount > n || n > deletionLists.size())
        throw std::invalid_argument("Invalid value");
    deletionListsCount = n;
}

ThreadInfo::ThreadInfo(Epoche &epoche, uint64_t thread_id)
        : epoche(epoche), deletionList(epoche.deletionLists[thread_id]) { }

DeletionList &ThreadInfo::getDeletionList() const {
    return deletionList;
}

Epoche &ThreadInfo::getEpoche() const {
    return epoche;
}

#endif //EPOCHE_CPP
