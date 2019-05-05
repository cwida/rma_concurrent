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

#include "apma_distributions.hpp"

#include <cmath>
#include <stdexcept>

#include "common/console_arguments.hpp"

#include "random_permutation.hpp"

using namespace std;

namespace distributions {

/*****************************************************************************
 *                                                                           *
 *   SequentialForward                                                       *
 *                                                                           *
 *****************************************************************************/

SequentialForward::SequentialForward(int64_t start, int64_t end): m_begin(start), m_end(end) {
    if(start > end) throw std::invalid_argument("start > end");
}
size_t SequentialForward::size() const { return m_end - m_begin; }
int64_t SequentialForward::key(size_t offset) const {
    assert(offset < (m_end - m_begin));
    return m_begin + offset;
}
unique_ptr<Interface> SequentialForward::view(size_t start, size_t length) { return make_unique<SequentialForward>(start, start + length); }
bool SequentialForward::is_dense() const { return true; }

/*****************************************************************************
 *                                                                           *
 *   SequentialBackwards                                                     *
 *                                                                           *
 *****************************************************************************/
SequentialBackwards::SequentialBackwards(int64_t start, int64_t end): m_begin(start), m_end(end){
    if(start > end) throw std::invalid_argument("start > end");
}
size_t SequentialBackwards::size() const { return m_end - m_begin; }
int64_t SequentialBackwards::key(size_t offset) const { return m_end -1 - offset; }
unique_ptr<Interface> SequentialBackwards::view(size_t start, size_t length) { return make_unique<SequentialBackwards>(start, start + length); }
bool SequentialBackwards::is_dense() const { return true; }

} // namespace distributions
