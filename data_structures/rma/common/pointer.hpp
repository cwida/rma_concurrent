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

#include <cassert>
#include "common/miscellaneous.hpp"

#include "abort.hpp"

namespace data_structures::rma::common {

template <typename T, typename ThreadContext>
class Pointer {
    uint64_t m_timestamp;
    T* m_pointer;

public:
    Pointer(T* pointer) : m_timestamp(::common::rdtscp()), m_pointer(pointer) { }

    T* get_unsafe() const {
        return m_pointer;
    }

    T* get(const ThreadContext& context) const {
        if(context.epoch() < m_timestamp) throw Abort{};
        return m_pointer;
    }

    T* get(const ThreadContext* context) const {
        if(context == nullptr) throw Abort{};
        return get(*context);
    }

    void set(T* pointer){
        m_pointer = pointer;
    }

    uint64_t& timestamp() { return m_timestamp; }
};

} // namespace
