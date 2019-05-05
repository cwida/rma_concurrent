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

#include "parallel.hpp"

namespace data_structures {

// definition
bool global_parallel_scan_enabled = true;

// the callbacks are by default nops
ParallelCallbacks::~ParallelCallbacks() {}
void ParallelCallbacks::on_init_main(int num_threads){ };
void ParallelCallbacks::on_init_worker(int worker_id){ };
void ParallelCallbacks::on_destroy_worker(int worker_id){ };
void ParallelCallbacks::on_complete(){ };
void ParallelCallbacks::on_destroy_main(){ };

} // namespace data_structures
