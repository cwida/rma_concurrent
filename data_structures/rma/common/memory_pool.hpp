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

#ifndef RMA_MEMORY_POOL_HPP_
#define RMA_MEMORY_POOL_HPP_

//#include <cassert>
#include <cstddef>
#include <cstdlib>
//#include <iostream>
#include <mutex>
#include <new> // std::bad_alloc
#include <stdexcept>

#include "common/spin_lock.hpp"

namespace data_structures::rma::common {

// forward declarations
class MemoryPool;
class CachedMemoryPool_ThreadUnsafe;
template<typename T> class CachedAllocator_ThreadUnsafe; // forward decl
class CachedMemoryPool_ThreadSafe;
template<typename T> class CachedAllocator_ThreadSafe; // forward decl

// aliases
using CachedMemoryPool = CachedMemoryPool_ThreadSafe;
template<typename T> using CachedAllocator = CachedAllocator_ThreadSafe<T>;

/**
 * A simple memory pool
 */
class MemoryPool {
private:
    char* m_buffer; // underlying storage
    size_t m_offset; // current offset in the buffer
    const size_t m_capacity; // capacity of the underlying buffer

public:
    /**
     * Create a new memory pool with the given capacity
     */
    MemoryPool(size_t capacity);

    /**
     * Destructor
     */
    ~MemoryPool();

    /**
     * Acquire a piece of `n' bytes of memory from the pool. It returns
     * the pointer to the memory on success, otherwise nullptr.
     */
    void* allocate(size_t n){
        // not enough space
        if(m_capacity - m_offset < n){ return nullptr; }

        char* ptr = m_buffer + m_offset;
        m_offset += n;

        return ptr;
    }

    /**
     * Release the whole memory previously acquired from the pool
     */
    void release(){ m_offset = 0; }

    /**
     * Return the pointer to the start of the underlying buffer
     */
    void* begin() const { return m_buffer; };

    /*
     * Return the pointer to the end of the underlying buffer
     */
    void* end() const { return m_buffer + m_capacity; }
};


/**
 * Try to serve an allocation request from the memory pool. Otherwise
 * it acquires storage from stdlib through malloc
 */
class CachedMemoryPool_ThreadUnsafe {
    MemoryPool m_pool; // underlying memory pool
    int m_counter; // number of allocations currently in use from the pool

    /**
     * Obtain a chunk of n bytes in the heap
     */
    void* allocate_raw(size_t n);

    /**
     * Deallocate the given chunk of memory
     */
    void deallocate_raw(void* ptr);

public:
    /**
     *  Initialise a memory pool with the capacity defined by the console argument --memory_pool,
     *  or the default 64MB if not set
     */
    CachedMemoryPool_ThreadUnsafe();

    /**
     * Initialise a memory pool with the given capacity
     */
    CachedMemoryPool_ThreadUnsafe(size_t capacity);

    /**
     * Allocate the space of n objects of type T
     */
    template<typename T>
    T* allocate(size_t n){
        return (T*) allocate_raw(n * sizeof(T));
    }

    /**
     * Deallocate the given object
     */
    void deallocate(void* ptr){
        deallocate_raw(ptr);
    }

    /**
     * Get a C++ allocator for this memory pool
     */
    template <typename T>
    CachedAllocator_ThreadUnsafe<T> allocator(){ return CachedAllocator_ThreadUnsafe<T>(this); }

    /**
     * Equality operator
     */
    bool operator==(const CachedMemoryPool_ThreadUnsafe& other){
        return m_pool.begin() == other.m_pool.begin();
    }

    /**
     * Check whether there is any allocation not released yet
     */
    bool empty() const { return m_counter == 0; }
};


/**
 * An implementation of the C++ Allocator concept, relying on a CachedMemoryPool.
 */
template<typename T>
class CachedAllocator_ThreadUnsafe {
    CachedMemoryPool_ThreadUnsafe* m_pool;
public:
    using value_type = T;

    /**
     * Constructor
     */
    CachedAllocator_ThreadUnsafe<T>(CachedMemoryPool_ThreadUnsafe* pool);

    /**
     * Allocate storage suitable for n objects
     */
    T* allocate(size_t n);

    /**
     * Release the given pointer
     */
    void deallocate(T* ptr, size_t) noexcept;

    /**
     * Required by the C++ concept
     */
    bool operator==(const CachedAllocator_ThreadUnsafe<T>& other) const ;
    bool operator!=(const CachedAllocator_ThreadUnsafe<T>& other) const ;
};


/**
 * Thread safe variant of the memory pool
 */
class CachedMemoryPool_ThreadSafe {
    template<typename T> friend class CachedAllocator_ThreadSafe;
    CachedMemoryPool_ThreadUnsafe m_memory_pool;
    mutable ::common::SpinLock m_mutex;

public:
    /**
     *  Initialise a memory pool with the capacity defined by the console argument --memory_pool,
     *  or the default 64MB if not set
     */
    CachedMemoryPool_ThreadSafe();

    /**
     * Initialise a memory pool with the given capacity
     */
    CachedMemoryPool_ThreadSafe(size_t capacity);

    /**
     * Allocate the space of n objects of type T
     */
    template<typename T>
    T* allocate(size_t n);

    /**
     * Deallocate the given object
     */
    void deallocate(void* ptr);

    /**
     * Get a C++ allocator for this memory pool
     */
    template <typename T>
    CachedAllocator_ThreadSafe<T> allocator();

    /**
     * Check whether there is any allocation not released yet
     */
    bool empty() const;
};

/**
 * Thread safe variant of a CachedAllocator
 */
template<typename T>
class CachedAllocator_ThreadSafe {
    CachedMemoryPool_ThreadSafe* m_thread_safe_pool;
    CachedAllocator_ThreadUnsafe<T> m_allocator;
public:
    using value_type = T;

    /**
     * Constructor
     */
    CachedAllocator_ThreadSafe(CachedMemoryPool_ThreadSafe* ts_pool);

    /**
     * Allocate storage suitable for n objects
     */
    T* allocate(size_t n);

    /**
     * Release the given pointer
     */
    void deallocate(T* ptr, size_t n) noexcept;

    /**
     * Required by the C++ concept
     */
    bool operator==(const CachedAllocator_ThreadSafe<T>& other) const;
    bool operator!=(const CachedAllocator_ThreadSafe<T>& other) const;
};


/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/
template<typename T>
CachedAllocator_ThreadUnsafe<T>::CachedAllocator_ThreadUnsafe(CachedMemoryPool_ThreadUnsafe* pool) : m_pool(pool) {
    if (pool == nullptr){ throw std::invalid_argument("Null pointer"); }
}

template<typename T>
T* CachedAllocator_ThreadUnsafe<T>::allocate(size_t n){
    return m_pool->allocate<T>(n);
}

template<typename T>
void CachedAllocator_ThreadUnsafe<T>::deallocate(T* ptr, size_t) noexcept {
    m_pool->deallocate(ptr);
}

template<typename T>
bool CachedAllocator_ThreadUnsafe<T>::operator==(const CachedAllocator_ThreadUnsafe<T>& other) const{
    return this->m_pool == other.m_pool;
}
template<typename T>
bool CachedAllocator_ThreadUnsafe<T>::operator!=(const CachedAllocator_ThreadUnsafe<T>& other) const {
    return !((*this) == other);
}

inline
void* CachedMemoryPool_ThreadUnsafe::allocate_raw(size_t n){
    if(n == 0) return nullptr;

    void* ptr = m_pool.allocate(n);
    if(ptr == nullptr){ // failure
        ptr = (char*) malloc(n);
        if(ptr == nullptr) throw std::bad_alloc();
    } else { // success
        m_counter++;
    }

//    std::cout << "[CachedMemoryPool::allocate_raw] ptr: " << ptr << ", n: " << n << std::endl;
    return ptr;
}

inline
void CachedMemoryPool_ThreadUnsafe::deallocate_raw(void* ptr){
//    std::cout << "[CachedMemoryPool::deallocate_raw] ptr: " << ptr << std::endl;
    if(m_pool.begin() <= ptr && ptr <= m_pool.end()){
        m_counter--;
        if(m_counter == 0) m_pool.release();
    } else { // rely on the standard library
        free(ptr);
    }
}


inline CachedMemoryPool_ThreadSafe::CachedMemoryPool_ThreadSafe() { }
inline CachedMemoryPool_ThreadSafe::CachedMemoryPool_ThreadSafe(size_t capacity) : m_memory_pool(capacity) { }
template<typename T>
T* CachedMemoryPool_ThreadSafe::allocate(size_t n){
    std::scoped_lock<decltype(m_mutex)> lock(m_mutex);
    return m_memory_pool.allocate<T>(n);
}
inline void CachedMemoryPool_ThreadSafe::deallocate(void* ptr){
    std::scoped_lock<decltype(m_mutex)> lock(m_mutex);
    m_memory_pool.deallocate(ptr);
}

template <typename T>
CachedAllocator_ThreadSafe<T> CachedMemoryPool_ThreadSafe::allocator(){
    std::scoped_lock<decltype(m_mutex)> lock(m_mutex);
    return CachedAllocator_ThreadSafe<T>(this);
}

inline bool CachedMemoryPool_ThreadSafe::empty() const {
    std::scoped_lock<decltype(m_mutex)> lock(m_mutex);
    return m_memory_pool.empty();
}
template<typename T>
CachedAllocator_ThreadSafe<T>::CachedAllocator_ThreadSafe(CachedMemoryPool_ThreadSafe* ts_pool) : m_thread_safe_pool(ts_pool), m_allocator(&(m_thread_safe_pool->m_memory_pool)){
    /* nop */
}
template<typename T>
T* CachedAllocator_ThreadSafe<T>::allocate(size_t n){
    std::scoped_lock<decltype(m_thread_safe_pool->m_mutex)> lock(m_thread_safe_pool->m_mutex);
    return m_allocator.allocate(n);
}
template<typename T>
void CachedAllocator_ThreadSafe<T>::deallocate(T* ptr, size_t n) noexcept {
    std::scoped_lock<decltype(m_thread_safe_pool->m_mutex)> lock(m_thread_safe_pool->m_mutex);
    m_allocator.deallocate(ptr, n);
}
template<typename T>
bool CachedAllocator_ThreadSafe<T>::operator==(const CachedAllocator_ThreadSafe<T>& other) const {
    return m_allocator.operator ==(other.m_allocator);
}
template<typename T>
bool CachedAllocator_ThreadSafe<T>::operator!=(const CachedAllocator_ThreadSafe<T>& other) const {
    return m_allocator.operator !=(other.m_allocator);
}

} // data_structures::rma::common

#endif /* RMA_MEMORY_POOL_HPP_ */
