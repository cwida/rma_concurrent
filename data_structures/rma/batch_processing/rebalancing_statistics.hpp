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

#include <chrono>
#include <ostream>
#include <vector>

namespace data_structures::rma::batch_processing {

/**
 * Enable the wrapped statement only in profiling mode (-DPROFILING)
 */
#if defined(PROFILING)
#define IF_PROFILING(what) what;
#else
#define IF_PROFILING(what) ;
#endif

/**
 * Statistics attached to a single rebalancing task
 */
struct RebalancingStatistics {
    std::chrono::time_point<std::chrono::steady_clock> m_time_init; // the wall-clock time this task was created

    enum class Type { REBALANCE, UPSIZE, DOWNSIZE };
    Type m_type = Type::REBALANCE; // the type of rebalancing, whether this is a resize
    int64_t m_window_length = 0; // the total number of segments rebalanced or, in case of resize, of the new sparse array

    int64_t m_master_wallclock_time = 0; // in microsecs, the wall clock time to execute the complete task
    int64_t m_master_search_time = 0; // in microsecs, the amount of time spent in the Master to compute the window to rebalance
    int64_t m_master_num_resumes = 0; // number of invocations to rebal_resume() for this task
    int64_t m_master_num_tasks_merged = 0; // the number of tasks merged in the search phase
    int64_t m_master_launch_time = 0; // in microsecs, the amount of time spent by the Master to launch this task
    int64_t m_master_release_time = 0; // in microsecs, time spent by the Master in the final stage (releasing the locks, waking up the threads)

    int64_t m_worker_total_time = 0; // total time to execute the rebalancing by the workers
    int64_t m_worker_sort_time = 0; // in microsecs, time spent to sort the elts in the bulk loading lists
    int64_t m_worker_make_subtasks_time = 0; // in microsecs, time spent to create the list of subtasks
    int64_t m_worker_num_subtaks = 0; // the number of subtasks created (=0, single queue execution)
    int64_t m_worker_segment_cards = 0; // in microsecs, time spent to update the segment cardinalities
    int64_t m_worker_clear_blkload_queues = 0; // in microsecs, time spent to reset the bulk loading queues
    int64_t m_worker_num_threads = 1; // total number of workers loaded for the task
    std::vector<int64_t> m_worker_task_exec_time; // the execution time of each subtask


    RebalancingStatistics() : m_time_init(std::chrono::steady_clock::now()){ }
};


/**
 * Attach a timer to a variable of RebalancingStatistics using the RAII paradigm
 */
class RebalancingTimer {
    int64_t& m_variable;
    std::chrono::time_point<std::chrono::steady_clock> m_t0; // wall-clock time when the timer started
public:
    RebalancingTimer(int64_t& variable) : m_variable(variable), m_t0(std::chrono::steady_clock::now()) { }
    ~RebalancingTimer() {
        auto d = std::chrono::steady_clock::now() - m_t0;
        m_variable += std::chrono::duration_cast<std::chrono::microseconds>(d).count();
    }
};



/**
 * Statistics associated to a single field of RebalancingStatistics
 */
struct RebalancingFieldStatistics{
    int64_t m_count =0; // number of elements counted
    int64_t m_sum =0; // sum of the times, in microsecs
    int64_t m_sum_sq = 0; // the squared sum of times
    int64_t m_average =0; // average, in microsecs
    int64_t m_min = 0; // minimum, in microsecs
    int64_t m_max = 0; // maximum, in microsecs
    int64_t m_stddev =0; // standard deviation, in microsecs
    int64_t m_median =-1; // median, in microsecs

    RebalancingFieldStatistics() : m_min(std::numeric_limits<int64_t>::max()){ }
};

/**
 * Statistics associated to a single window
 */
struct RebalancingWindowStatistics{
    RebalancingStatistics::Type m_type; // rebalance or resize
    int64_t m_window_length; // the length of the statistics
    int64_t m_count = 0; // the number of entries associated to this window

    RebalancingFieldStatistics m_master_wallclock_time; // in microsecs, the wall clock time to execute the complete task
    RebalancingFieldStatistics m_master_search_time; // in microsecs, the amount of time spent in the Master to compute the window to rebalance
    RebalancingFieldStatistics m_master_num_resumes; // number of invocations to rebal_resume() for this task
    RebalancingFieldStatistics m_master_num_tasks_merged; // the number of tasks merged in the search phase
    RebalancingFieldStatistics m_master_launch_time; // in microsecs, the amount of time spent by the Master to launch this task
    RebalancingFieldStatistics m_master_release_time; // in microsecs, time spent by the Master in the final stage (releasing the locks, waking up the threads)

    RebalancingFieldStatistics m_worker_total_time; // total time to execute the rebalancing by the workers
    RebalancingFieldStatistics m_worker_sort_time; // in microsecs, time spent to sort the elts in the bulk loading lists
    RebalancingFieldStatistics m_worker_make_subtasks_time; // in microsecs, time spent to create the list of subtasks
    RebalancingFieldStatistics m_worker_num_subtaks; // the number of subtasks created (=0, single queue execution)
    RebalancingFieldStatistics m_worker_segment_cards; // in microsecs, time spent to update the segment cardinalities
    RebalancingFieldStatistics m_worker_clear_blkload_queues; // in microsecs, time spent to reset the bulk loading queues
    RebalancingFieldStatistics m_worker_num_threads; // total number of workers loaded for the task
    RebalancingFieldStatistics m_worker_task_exec_time_avg; // the execution time of each subtask
    RebalancingFieldStatistics m_worker_task_exec_time_min; // the execution time of each subtask
    RebalancingFieldStatistics m_worker_task_exec_time_max; // the execution time of each subtask
    RebalancingFieldStatistics m_worker_task_exec_time_median; // the execution time of each subtask

    RebalancingWindowStatistics(RebalancingStatistics::Type type, int64_t window_length) : m_type(type), m_window_length(window_length) { }
};

/**
 * The complete statistics computed for the vector of statistics retrieved
 */
struct RebalancingCompleteStatistics {
    // total amount of search time, resumes, launch time, release time
    RebalancingFieldStatistics m_master_wallclock_time; // in microsecs, total time for the tasks completed
    RebalancingFieldStatistics m_master_search_time; // in microsecs, the amount of time spent in the Master to compute the window to rebalance
    RebalancingFieldStatistics m_master_num_resumes; // number of invocations to rebal_resume() for this task
    RebalancingFieldStatistics m_master_num_tasks_merged; // the number of tasks merged in the search phase
    RebalancingFieldStatistics m_master_launch_time; // in microsecs, the amount of time spent by the Master to launch this task
    RebalancingFieldStatistics m_master_release_time; // in microsecs, time spent by the Master in the final stage (releasing the locks, waking up the threads)

    std::vector<RebalancingWindowStatistics> m_rebalances; // rebalances
    std::vector<RebalancingWindowStatistics> m_upsizes; // increase the capacity
    std::vector<RebalancingWindowStatistics> m_downsizes; // decrease the capacity
};

RebalancingCompleteStatistics get_rebalancing_stastistics(std::vector<RebalancingStatistics>& stats);

std::ostream& operator<<(std::ostream& out, RebalancingStatistics::Type type);
std::ostream& operator<<(std::ostream& out, const RebalancingFieldStatistics& field);
std::ostream& operator<<(std::ostream& out, const RebalancingWindowStatistics& window);
std::ostream& operator<<(std::ostream& out, const RebalancingCompleteStatistics& stats);


} // namespace
