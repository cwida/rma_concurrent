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

#ifndef DATA_STRUCTURES_PARALLEL_HPP_
#define DATA_STRUCTURES_PARALLEL_HPP_

namespace data_structures {

/**
 * Global variable to stop a thread performing scans. At the start of an experiment it is set to true, and set to false at the end.
 */
extern bool global_parallel_scan_enabled;

/**
 * An optional interface that can be implemented by an Index data structure. It provides a mechanism
 * to set hooks at the start and at the end of an experiment.
 */
struct ParallelCallbacks {
    /**
     * 1- This callback is invoked before the start of the experiment by the main thread. It defines
     * the number of worker threads used in the experiment
     */
    virtual void on_init_main(int num_threads);

    /**
     * 2- This callback is invoked once by each of the worker threads, before the actual computation of
     * the worker thread begins. The argument worker_id is a sequential ID for the worker, starting from
     * 0 (inclusive) up to the number of threads used in the experiment (exclusive).
     */
    virtual void on_init_worker(int worker_id);

    /**
     * 3- This callback is invoked once by each of the worker threads, after the whole computation has
     * been performed.
     */
    virtual void on_destroy_worker(int worker_id);

    /**
     * 4- This callback is invoked at the end of the computation by the main thread, once all the worker
     * threads have been executed.
     */
    virtual void on_destroy_main();

    /**
     * 5- This callback is executed when the driver expects all updates to be completed. Some implementations
     * are asynchronous, the callback ensures that all updates have been merged into the data structure.
     */
    virtual void on_complete();

    /**
     * NOP Destructor
     */
    virtual ~ParallelCallbacks();
};

} // namespace data_structures

#endif /* DATA_STRUCTURES_PARALLEL_HPP_ */
