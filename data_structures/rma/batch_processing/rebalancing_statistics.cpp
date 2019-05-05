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

#include "rebalancing_statistics.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <mutex>

#include "common/miscellaneous.hpp" // get_thread_id

using namespace common;
using namespace std;

namespace data_structures::rma::batch_processing {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex _debug_mutex; // PackedMemoryArray.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(_debug_mutex); std::cout << "[RebalancingStatistics::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Compute statistics                                                      *
 *                                                                           *
 *****************************************************************************/
static void add_stat(RebalancingFieldStatistics& field, int64_t value){
    field.m_count++;
    field.m_sum += value;
    field.m_sum_sq += value*value;
    field.m_min = min(value, field.m_min);
    field.m_max = max(value, field.m_max);
}

// I'm not proud of this macro ...
static void compute_avg_stddev(RebalancingFieldStatistics& field){
    if(field.m_count > 0) {
        field.m_average = field.m_sum / field.m_count;
        field.m_stddev = (static_cast<double>(field.m_sum_sq) / field.m_count) - pow(field.m_average, 2.0);
    }
}

#define finalize_stat(field_name) \
    if(index_start < index_end) { \
        auto& field = window.field_name; \
        assert(field.m_count == index_end - index_start && "Invalid count"); \
        std::sort(begin(profiles) + index_start, begin(profiles) + index_end, [](const RebalancingStatistics& stat1, RebalancingStatistics& stat2){ \
            return stat1.field_name < stat2.field_name; \
        }); \
        if(field.m_count % 2 == 1){ \
            field.m_median = profiles[(index_start + index_end) /2].field_name; \
        } else { \
            size_t d1 = (index_start + index_end) /2; \
            size_t d0 = d1 - 1; \
            field.m_median = (profiles[d0].field_name + profiles[d1].field_name) / 2; \
        } \
        compute_avg_stddev(field); \
    }

RebalancingCompleteStatistics get_rebalancing_stastistics(std::vector<RebalancingStatistics>& profiles){
    RebalancingCompleteStatistics result;
    if(profiles.empty()) return result;

    std::sort(begin(profiles), end(profiles), [](const RebalancingStatistics& stat1, RebalancingStatistics& stat2){
        // return true if stat1 < stat2, damn C++ interface

        // sort by type
        int type1 = static_cast<int>(stat1.m_type);
        int type2 = static_cast<int>(stat2.m_type);
        if(type1 < type2)
            return true;
        else if (type1 > type2)
            return false;

        // sort by the window length
        else
            return (stat1.m_window_length < stat2.m_window_length);
    });

    int64_t index_start = 0;
    auto do_compute_statistics = [&result, &profiles, &index_start](RebalancingStatistics::Type type, vector<RebalancingWindowStatistics>& container){
        while(index_start < profiles.size() && profiles[index_start].m_type == type){
            int64_t window_length = profiles[index_start].m_window_length;
            int64_t index_end = index_start; // excl
            RebalancingWindowStatistics window { type, window_length };
            while(index_end < profiles.size() && profiles[index_end].m_window_length == window_length && profiles[index_end].m_type == type){
                window.m_count++;

                // global stats
                add_stat(result.m_master_wallclock_time, profiles[index_end].m_master_wallclock_time);
                add_stat(result.m_master_search_time, profiles[index_end].m_master_search_time);
                add_stat(result.m_master_num_resumes, profiles[index_end].m_master_num_resumes);
                add_stat(result.m_master_num_tasks_merged, profiles[index_end].m_master_num_tasks_merged);
                add_stat(result.m_master_launch_time, profiles[index_end].m_master_launch_time);
                add_stat(result.m_master_release_time, profiles[index_end].m_master_release_time);

                // window stats
                add_stat(window.m_master_wallclock_time, profiles[index_end].m_master_wallclock_time);
                add_stat(window.m_master_search_time, profiles[index_end].m_master_search_time);
                add_stat(window.m_master_num_resumes, profiles[index_end].m_master_num_resumes);
                add_stat(window.m_master_num_tasks_merged, profiles[index_end].m_master_num_tasks_merged);
                add_stat(window.m_master_launch_time, profiles[index_end].m_master_launch_time);
                add_stat(window.m_master_release_time, profiles[index_end].m_master_release_time);
                add_stat(window.m_worker_total_time, profiles[index_end].m_worker_total_time);
                add_stat(window.m_worker_sort_time, profiles[index_end].m_worker_sort_time);
                add_stat(window.m_worker_make_subtasks_time, profiles[index_end].m_worker_make_subtasks_time);
                add_stat(window.m_worker_num_subtaks, profiles[index_end].m_worker_num_subtaks);
                add_stat(window.m_worker_segment_cards, profiles[index_end].m_worker_segment_cards);
                add_stat(window.m_worker_clear_blkload_queues, profiles[index_end].m_worker_clear_blkload_queues);
                add_stat(window.m_worker_num_threads, profiles[index_end].m_worker_num_threads);

                int64_t worker_exec_time_min = std::numeric_limits<int64_t>::max();
                int64_t worker_exec_time_max = std::numeric_limits<int64_t>::min();
                int64_t worker_exec_time_sum = 0;

                const size_t worker_task_exec_time_sz = profiles[index_end].m_worker_task_exec_time.size();
                for(auto time : profiles[index_end].m_worker_task_exec_time){
                    worker_exec_time_sum += time;
                    worker_exec_time_min = min(time, worker_exec_time_min);
                    worker_exec_time_max = max(time, worker_exec_time_max);
                }
                add_stat(window.m_worker_task_exec_time_min, worker_exec_time_min);
                add_stat(window.m_worker_task_exec_time_max, worker_exec_time_max);
                add_stat(window.m_worker_task_exec_time_avg, worker_exec_time_sum / worker_task_exec_time_sz);

                // median
                std::sort(begin(profiles[index_end].m_worker_task_exec_time), end(profiles[index_end].m_worker_task_exec_time));
                if(worker_task_exec_time_sz % 2 == 1){
                    add_stat(window.m_worker_task_exec_time_median, profiles[index_end].m_worker_task_exec_time[worker_task_exec_time_sz/2]);
                } else {
                    size_t d1 = worker_task_exec_time_sz /2;
                    size_t d0 = worker_task_exec_time_sz - 1;
                    add_stat(window.m_worker_task_exec_time_median, (profiles[index_end].m_worker_task_exec_time[d0] + profiles[index_end].m_worker_task_exec_time[d1]) /2);
                }


                index_end++;
            }

            finalize_stat(m_master_wallclock_time);
            finalize_stat(m_master_search_time);
            finalize_stat(m_master_num_resumes);
            finalize_stat(m_master_num_tasks_merged);
            finalize_stat(m_master_release_time);
            finalize_stat(m_worker_total_time);
            finalize_stat(m_worker_sort_time);
            finalize_stat(m_worker_make_subtasks_time);
            finalize_stat(m_worker_num_subtaks);
            finalize_stat(m_worker_segment_cards);
            finalize_stat(m_worker_clear_blkload_queues);
            finalize_stat(m_worker_num_threads);
            compute_avg_stddev(window.m_worker_task_exec_time_min);
            compute_avg_stddev(window.m_worker_task_exec_time_max);
            compute_avg_stddev(window.m_worker_task_exec_time_avg);
            compute_avg_stddev(window.m_worker_task_exec_time_median);

            container.push_back(window);

            index_start = index_end; // next iteration
        }
    };

    // fire!
    do_compute_statistics(RebalancingStatistics::Type::REBALANCE, result.m_rebalances);
    do_compute_statistics(RebalancingStatistics::Type::UPSIZE, result.m_upsizes);
    do_compute_statistics(RebalancingStatistics::Type::DOWNSIZE, result.m_downsizes);


    // global stats
    compute_avg_stddev(result.m_master_wallclock_time);
    compute_avg_stddev(result.m_master_search_time);
    compute_avg_stddev(result.m_master_num_resumes);
    compute_avg_stddev(result.m_master_num_tasks_merged);
    compute_avg_stddev(result.m_master_launch_time);
    compute_avg_stddev(result.m_master_release_time);

    return result;
}


/*****************************************************************************
 *                                                                           *
 *   Output                                                                  *
 *                                                                           *
 *****************************************************************************/
ostream& operator<<(std::ostream& out, RebalancingStatistics::Type type){
    switch(type){
    case RebalancingStatistics::Type::REBALANCE: out << "REBALANCE"; break;
    case RebalancingStatistics::Type::UPSIZE: out << "UPSIZE"; break;
    case RebalancingStatistics::Type::DOWNSIZE: out << "DOWNSIZE"; break;
    }
    return out;
}

ostream& operator<<(ostream& out, const RebalancingFieldStatistics& field) {
    out << "{count: " << field.m_count << ", avg: " << field.m_average << ", median: " << field.m_median << ", min: " <<
            field.m_min << ", max: " << field.m_max << ", std dev: " << field.m_stddev << "}";
    return out;
}

ostream& operator<<(ostream& out, const RebalancingWindowStatistics& window){
    out << window.m_type << " [" << window.m_window_length << "], count: " << window.m_count << "\n";
    out << "    (master) wall clock time: " << window.m_master_wallclock_time << " microsecs\n";
    out << "    (master) search time: " << window.m_master_search_time << " microsecs\n";
    out << "    (master) invocations to rebal_resume(): " << window.m_master_num_resumes << "\n";
    out << "    (master) tasks merged in rebal_resume(): " << window.m_master_num_tasks_merged << "\n";
    out << "    (master) launch time: " << window.m_master_launch_time << " microsecs\n";
    out << "    (master) post processing time: " << window.m_master_release_time << " microsecs\n";
    out << "    (worker) coordinator, execution time (wall clock): " << window.m_worker_total_time << " microsecs\n";
    out << "    (worker) bulk loading, sorting time: " << window.m_worker_sort_time << " microsecs\n";
    out << "    (worker) creating the subtasks: " << window.m_worker_make_subtasks_time << " microsecs\n";
    out << "    (worker) number of subtasks created: " << window.m_worker_num_subtaks << "\n";
    out << "    (worker) total number of threads: " << window.m_worker_num_threads << "\n";
    out << "    (worker) average execution time per subtask: " << window.m_worker_task_exec_time_avg << " microsecs\n";
    out << "    (worker) minimum execution time per subtask: " << window.m_worker_task_exec_time_min << " microsecs\n";
    out << "    (worker) maximum execution time per subtask: " << window.m_worker_task_exec_time_max << " microsecs\n";
    out << "    (worker) median execution time per subtask: " << window.m_worker_task_exec_time_median << " microsecs\n";
    out << "    (worker) update segment cardinalities: " << window.m_worker_segment_cards << " microsecs\n";
    out << "    (worker) bulk loading, clearing queues: " << window.m_worker_clear_blkload_queues << " microsecs\n";
    return out;
}

ostream& operator<<(ostream& out, const RebalancingCompleteStatistics& stats){
    out << "--- Rebalancing statistics ---\n";
    out << "-> Total wall clock time of completed tasks: " << stats.m_master_wallclock_time << " microsecs\n";
    out << "-> Master, cumulative search time: " << stats.m_master_search_time << " microsecs\n";
    out << "-> Master, cumulative number of invocations to rebal_resume(): " << stats.m_master_num_resumes << "\n";
    out << "-> Master, cumulative number of merged tasks: " << stats.m_master_num_tasks_merged << "\n";
    out << "-> Master, cumulative launch time: " << stats.m_master_launch_time << " microsecs\n";
    out << "-> Master, cumulative post processing time: : " << stats.m_master_release_time << " microsecs\n";
    for(auto& window : stats.m_rebalances) out << window;
    for(auto& window : stats.m_upsizes) out << window;
    for(auto& window : stats.m_downsizes) out << window;
    return out;
}

} // namespace
