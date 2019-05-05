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

#ifndef DISTRIBUTIONS_CBYTEVIEW_HPP_
#define DISTRIBUTIONS_CBYTEVIEW_HPP_

#include <memory>

#include "cbytearray.hpp"
#include "interface.hpp"

namespace distributions {

class CByteView : public Interface {
protected:
    std::shared_ptr<CByteArray> container_ptr; // reference counting
    CByteArray* container; // avoid the overhead of reference counting
    size_t begin; // inclusive
    size_t end; // exclusive
    bool dense; // is the sequence dense?

    CByteView();

public:
    /**
     * Create a view for the whole container
     */
    CByteView(std::shared_ptr<CByteArray> container);

    /**
     * Create a view for the container in [begin, end)
     */
    CByteView(std::shared_ptr<CByteArray> container, size_t begin, size_t end);

    size_t size() const override;

    KeyValue get(size_t index) const override;
    int64_t key(size_t index) const override;

    void sort();

    std::unique_ptr<Interface> view(size_t start, size_t length) override;

    void set_dense(bool value);

    bool is_dense() const override;
};

} // namespace distributions

#endif /* DISTRIBUTIONS_CBYTEVIEW_HPP_ */
