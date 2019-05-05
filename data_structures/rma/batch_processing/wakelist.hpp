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

#include <future>
#include <vector>

namespace data_structures::rma::batch_processing {

class Gate; // forward declaration;

/**
 * This class is a mere optimisation. When waking up a bunch of workers from a Gate, it is more convenient
 * to do so _AFTER_ the lock of the related has been released, to avoid workers being awaken immediately finding
 * the gate still owned by the thread who is awaking them. This class serves as indirect step to retrieve
 * the list of workers to awake after releasing a gate's lock:
 *
 * WakeList w;
 * gate->wake_next(w);
 * gate->unlock();
 * w.wake();
 */
class WakeList {
private:
    friend class Gate;
    std::vector<std::promise<void>*> m_list_workers; // the list of workers to wake up

public:
    WakeList() { /* nop */ };

    void operator()(){
        for(auto w : m_list_workers) w->set_value();
        m_list_workers.clear();
    }
};

} // namespace
