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

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "common/miscellaneous.hpp"
#include "distributions/random_permutation.hpp"
#include "rma/baseline/packed_memory_array.hpp"
#include "driver.hpp"
#include "parallel.hpp"

using namespace data_structures::rma::baseline;
using namespace std;

TEST_CASE("minimal"){
    data_structures::initialise();
    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 2 };
}


TEST_CASE("initialise"){
    data_structures::initialise();

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 2 };
    pma.register_thread(0);
    REQUIRE(pma.empty());
    REQUIRE(pma.size() == 0);
    REQUIRE(pma.find(10) == -1);

    pma.insert(10, 100);
    REQUIRE(!pma.empty());
    REQUIRE(pma.size() == 1);
    REQUIRE(pma.find(5) == -1);
    REQUIRE(pma.find(10) == 100);
    REQUIRE(pma.find(15) == -1);

    REQUIRE(pma.remove(5) == -1); // the key doesn't exit
    REQUIRE(pma.size() == 1);
    REQUIRE(!pma.empty());

    REQUIRE(pma.remove(10) == 100);
    REQUIRE(pma.size() == 0);
    REQUIRE(pma.empty());

    pma.unregister_thread();
}


TEST_CASE("single_thread_local_rebal"){
    data_structures::initialise();

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 4 };
    pma.register_thread(0);
    REQUIRE(pma.empty());

    constexpr int64_t sz = 400;
    for(int64_t i = 1; i <= sz; i++){
        pma.insert(i, i *10);
        REQUIRE(pma.size() == i);
        for(int64_t j = 1; j <= i; j++){
            REQUIRE(pma.find(j) == j *10);
        }
    }
    REQUIRE(pma.size() == sz);

    for(int64_t i = 1; i <= sz; i++){
        REQUIRE(pma.remove(i) == i * 10);
        REQUIRE(pma.size() == sz - i);
    }

    pma.unregister_thread();
}



TEST_CASE("single_thread_global_rebal"){
    data_structures::initialise();

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 8 };
    pma.register_thread(0);
    REQUIRE(pma.empty());

    constexpr int64_t sz = 100000;
    for(int64_t i = 1; i <= sz; i++){
        pma.insert(i, i *10);
        REQUIRE(pma.size() == i);

    }

//    pma.dump();

    for(int64_t i = 1; i <= sz; i++){
        REQUIRE(pma.find(i) == i * 10);
    }

    for(int64_t i = 1; i <= sz; i++){
//        pma.dump();
        REQUIRE(pma.remove(i) == i * 10);
        REQUIRE(pma.size() == sz - i);
    }

    pma.unregister_thread();
}

TEST_CASE("multi_thread_local_rebal"){
    data_structures::initialise();
    constexpr int num_threads = 8;
    constexpr size_t num_elts = 1024;

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 4 };
    pma.set_max_number_workers(num_threads);

    distributions::RandomPermutationParallel sampler{ num_elts, /* seed */ 7 };
    int threads_started = 0;
    condition_variable _cvar;
    mutex _mutex;

    vector<thread> threads;
    int64_t num_keys_per_thread = num_elts / num_threads;
    int64_t num_keys_leftover = num_elts % num_threads;
    int64_t start_position = 0;
    for(int i = 0; i < num_threads; i++){
        int64_t num_keys_to_insert = num_keys_per_thread + (i < num_keys_leftover);

        threads.emplace_back([&](int64_t pos_start, int64_t num_keys){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // insert the keys
            int64_t pos_end = pos_start + num_keys;
            for(int64_t pos = pos_start; pos < pos_end; pos++){
                int64_t key = sampler.get_raw_key(pos) +1;
                pma.insert(key, key * 10);
            }

            // done
            pma.unregister_thread();
        }, start_position, num_keys_to_insert);

        start_position += num_keys_to_insert;
    }
    for(auto& t : threads) t.join(); // Zzz

    pma.set_max_number_workers(1);
    pma.register_thread(0);

    REQUIRE(pma.size() == num_elts);
    for(size_t i = 1; i <= num_elts; i++){
        REQUIRE(pma.find(i) == i * 10);
    }

    pma.unregister_thread();
}


TEST_CASE("multi_thread_global_rebal"){
    data_structures::initialise();
    constexpr int num_threads = 8;
    constexpr size_t num_elts = 1000000;

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 2, /* segments per lock */ 4 };
    pma.set_max_number_workers(num_threads);

    distributions::RandomPermutationParallel sampler{ num_elts, /* seed */ 7 };
    int threads_started = 0;
    condition_variable _cvar;
    mutex _mutex;

    vector<thread> threads;
    const int64_t num_keys_per_thread = num_elts / num_threads;
    const int64_t num_keys_leftover = num_elts % num_threads;
    int64_t start_position = 0;
    for(int i = 0; i < num_threads; i++){
        int64_t num_keys_to_insert = num_keys_per_thread + (i < num_keys_leftover);

        threads.emplace_back([&](int64_t pos_start, int64_t num_keys){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // insert the keys
            int64_t pos_end = pos_start + num_keys;
            for(int64_t pos = pos_start; pos < pos_end; pos++){
                int64_t key = sampler.get_raw_key(pos) +1;
                pma.insert(key, key * 10);
            }

            // done
            pma.unregister_thread();
        }, start_position, num_keys_to_insert);

        start_position += num_keys_to_insert;
    }
    for(auto& t : threads) t.join(); // Zzz

    pma.set_max_number_workers(1);
    pma.register_thread(0);

    REQUIRE(pma.size() == num_elts);
    for(size_t i = 1; i <= num_elts; i++){
        REQUIRE(pma.find(i) == i * 10);
    }

    pma.unregister_thread();

    // remove the items, one by one
    pma.set_max_number_workers(num_threads);
    start_position = 0; threads_started = 0;
    threads.resize(0);
    for(int i = 0; i < num_threads; i++){
        int64_t num_keys_to_remove = num_keys_per_thread + (i < num_keys_leftover);

        threads.emplace_back([&](int64_t pos_start, int64_t num_keys){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // insert the keys
            int64_t pos_end = pos_start + num_keys;
            for(int64_t pos = pos_start; pos < pos_end; pos++){
                int64_t key = sampler.get_raw_key(pos) +1;
                auto value = pma.remove(key);
                REQUIRE(value == key * 10);
            }

            // done
            pma.unregister_thread();
        }, start_position, num_keys_to_remove);

        start_position += num_keys_to_remove;
    }
    for(auto& t : threads) t.join(); // Zzz


    REQUIRE(pma.size() == 0);
    REQUIRE(pma.empty());
}


TEST_CASE("sum_sequential"){
    data_structures::initialise();
    using Implementation = PackedMemoryArray;

    shared_ptr<Implementation> implementation{ new Implementation{ /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 8, /* segments per lock */ 2 } };
    implementation->register_thread(0);

//    size_t sz = 64;
//    for(size_t i = 1; i <= sz; i++){
//        implementation->insert(i, i * 10);
//    }

    // a permutation of the numbers between 1 and 1033
    int64_t sample[] = {543, 805, 74, 79, 250, 685, 580, 447, 86, 116, 299, 122, 1028, 769,
            976, 702, 126, 353, 381, 888, 374, 822, 77, 139, 991, 986, 407, 259,
            905, 183, 98, 286, 15, 360, 242, 924, 331, 919, 175, 33, 3, 435, 506,
            372, 516, 815, 594, 748, 852, 860, 659, 990, 310, 1004, 497, 345,
            614, 303, 526, 632, 394, 401, 972, 964, 671, 49, 933, 9, 679, 903,
            662, 863, 899, 209, 645, 365, 975, 755, 841, 366, 747, 461, 923, 699,
            980, 796, 438, 1019, 636, 112, 697, 655, 240, 158, 935, 878, 994,
            408, 1030, 517, 129, 724, 551, 498, 600, 673, 604, 456, 695, 224,
            376, 17, 648, 323, 823, 713, 117, 450, 589, 23, 694, 913, 134, 267,
            609, 762, 814, 12, 11, 227, 618, 81, 16, 235, 615, 654, 95, 1023,
            579, 606, 334, 807, 458, 828, 352, 206, 371, 111, 775, 464, 746, 165,
            586, 857, 812, 793, 94, 43, 889, 170, 71, 383, 1015, 477, 448, 953,
            308, 395, 593, 318, 432, 29, 239, 205, 123, 521, 522, 55, 154, 361,
            612, 959, 504, 880, 869, 625, 251, 667, 216, 797, 798, 476, 453, 825,
            624, 405, 851, 128, 194, 375, 133, 813, 722, 977, 399, 363, 145, 682,
            119, 473, 930, 562, 764, 967, 234, 678, 338, 605, 215, 868, 367, 786,
            90, 38, 162, 136, 558, 496, 248, 84, 463, 581, 651, 75, 290, 411,
            354, 417, 602, 737, 311, 195, 966, 391, 518, 767, 93, 57, 564, 416,
            356, 350, 220, 811, 948, 4, 916, 835, 849, 243, 177, 288, 474, 954,
            277, 268, 6, 35, 137, 1003, 125, 293, 779, 816, 565, 629, 337, 887,
            494, 182, 124, 788, 283, 621, 834, 444, 479, 539, 54, 931, 818, 327,
            21, 771, 336, 428, 58, 40, 475, 409, 776, 355, 932, 709, 845, 89,
            359, 893, 885, 507, 595, 1020, 120, 820, 657, 821, 870, 388, 683,
            908, 140, 324, 985, 901, 840, 696, 396, 961, 672, 965, 530, 951, 442,
            50, 937, 853, 1, 457, 426, 304, 871, 263, 343, 576, 731, 315, 1021,
            873, 368, 941, 511, 617, 791, 262, 78, 377, 664, 829, 830, 460, 649,
            751, 768, 468, 691, 92, 386, 992, 258, 317, 616, 537, 484, 877, 152,
            45, 270, 236, 275, 431, 47, 499, 859, 803, 726, 445, 525, 218, 725,
            599, 100, 141, 989, 106, 918, 715, 533, 400, 563, 710, 910, 443, 690,
            217, 341, 228, 712, 890, 626, 592, 495, 25, 1001, 446, 906, 166, 393,
            650, 244, 720, 349, 153, 552, 1002, 392, 513, 64, 862, 781, 684, 716,
            284, 281, 601, 385, 173, 635, 997, 900, 210, 634, 200, 437, 429, 570,
            414, 280, 316, 757, 264, 883, 1018, 707, 157, 717, 557, 515, 766,
            742, 603, 692, 1009, 677, 178, 266, 760, 864, 466, 109, 455, 652,
            898, 981, 736, 837, 936, 85, 572, 993, 127, 911, 333, 184, 675, 528,
            674, 307, 510, 362, 826, 824, 150, 151, 488, 598, 465, 289, 608, 643,
            312, 1005, 167, 232, 896, 199, 172, 330, 642, 1031, 514, 665, 87,
            246, 817, 238, 97, 378, 640, 568, 193, 204, 138, 744, 535, 287, 469,
            656, 291, 357, 915, 7, 756, 783, 66, 879, 960, 348, 255, 529, 31,
            221, 547, 189, 44, 384, 571, 962, 810, 459, 963, 83, 110, 14, 329,
            1006, 418, 790, 597, 619, 1007, 279, 800, 186, 104, 256, 5, 53, 269,
            56, 647, 872, 855, 774, 523, 897, 895, 440, 838, 831, 987, 508, 926,
            984, 27, 582, 276, 26, 765, 114, 633, 542, 519, 588, 861, 301, 858,
            390, 761, 847, 943, 978, 403, 2, 76, 135, 1013, 24, 82, 561, 693,
            921, 721, 425, 728, 653, 548, 912, 503, 105, 427, 321, 502, 758, 549,
            666, 196, 88, 52, 819, 41, 143, 292, 983, 934, 836, 480, 688, 223,
            265, 101, 389, 198, 213, 591, 844, 118, 947, 300, 611, 806, 638, 566,
            550, 708, 839, 380, 260, 909, 369, 146, 569, 532, 644, 161, 925, 340,
            107, 231, 754, 785, 956, 646, 792, 433, 103, 322, 610, 387, 18, 866,
            65, 10, 876, 802, 491, 1032, 296, 854, 434, 735, 843, 833, 531, 113,
            740, 749, 714, 658, 698, 147, 623, 59, 99, 168, 319, 1024, 174, 298,
            160, 573, 902, 988, 917, 554, 534, 320, 778, 946, 422, 130, 730, 48,
            1014, 732, 939, 622, 982, 734, 470, 998, 211, 607, 430, 711, 254,
            784, 449, 185, 285, 28, 505, 574, 197, 297, 567, 342, 22, 544, 187,
            132, 865, 486, 979, 920, 1026, 108, 809, 230, 436, 782, 439, 326,
            344, 192, 536, 1017, 306, 750, 102, 538, 875, 493, 703, 886, 180,
            928, 927, 670, 804, 729, 957, 904, 585, 745, 358, 272, 179, 527, 949,
            524, 273, 481, 958, 639, 164, 867, 881, 313, 181, 364, 63, 462, 1011,
            892, 191, 1012, 471, 950, 91, 328, 441, 67, 739, 247, 973, 596, 669,
            613, 741, 641, 73, 482, 995, 19, 970, 590, 555, 808, 346, 660, 148,
            294, 397, 155, 706, 668, 794, 752, 188, 974, 131, 1033, 229, 556,
            339, 631, 249, 62, 546, 219, 309, 34, 1025, 509, 208, 176, 743, 545,
            225, 676, 424, 121, 489, 347, 413, 332, 237, 302, 780, 795, 938, 472,
            850, 575, 553, 305, 214, 421, 907, 1027, 914, 929, 689, 630, 60, 351,
            1008, 945, 370, 222, 500, 955, 540, 20, 763, 190, 520, 212, 8, 1010,
            490, 587, 884, 325, 13, 252, 382, 874, 39, 968, 687, 163, 492, 856,
            373, 202, 637, 80, 952, 415, 680, 801, 169, 1029, 753, 583, 940, 46,
            922, 423, 70, 770, 335, 30, 999, 282, 541, 245, 1016, 142, 487, 257,
            419, 261, 404, 36, 68, 37, 944, 271, 274, 559, 759, 894, 467, 772,
            584, 96, 777, 485, 560, 512, 233, 406, 149, 718, 483, 799, 115, 686,
            705, 451, 842, 882, 156, 1000, 848, 846, 454, 207, 295, 51, 478, 32,
            663, 891, 628, 420, 72, 789, 701, 203, 727, 996, 241, 410, 971, 620,
            69, 452, 501, 661, 226, 827, 719, 201, 773, 159, 704, 942, 171, 738,
            398, 577, 42, 61, 723, 379, 700, 402, 253, 278, 832, 412, 578, 314,
            681, 969, 144, 1022, 787, 627, 733};
    int64_t sz = sizeof(sample) / sizeof(sample[0]); // 1033
    for(size_t i = 0; i < sz; i++){
        implementation->insert(sample[i], sample[i] * 10);
    }

    implementation->build();
    REQUIRE(implementation->size() == sz);

//    implementation->dump();

    for(size_t i = 0; i <= sz + 1; i++){
        for(size_t j = i; j <= sz + 2; j++){
            auto sum = implementation->sum(i, j);
//            cout << "RANGE [" << i << ", " << j << "] result: " << sum << endl;

            if(j <= 0 || i > sz){
                REQUIRE(sum.m_num_elements == 0);
                REQUIRE(sum.m_sum_keys == 0);
                REQUIRE(sum.m_sum_values == 0);
            } else {
                int64_t vmin = std::max<int64_t>(1, i);
                int64_t vmax = std::min<int64_t>(sz, j);

                REQUIRE(sum.m_first_key == vmin);
                REQUIRE(sum.m_last_key == vmax);
                REQUIRE(sum.m_num_elements == (vmax - vmin +1));
                auto expected_sum = /* sum of the first vmax numbers */ (vmax * (vmax +1) /2) - /* sum of the first vmin -1 numbers */ ((vmin -1) * vmin /2);
                REQUIRE(sum.m_sum_keys == expected_sum);
                REQUIRE(sum.m_sum_values == expected_sum * 10);
            }
        }
    }

    implementation->unregister_thread();
}


TEST_CASE("sum_parallel"){
    data_structures::initialise();
    constexpr int num_update_threads = 8;
    constexpr int num_scan_threads = 8;
    constexpr int num_threads = num_update_threads + num_scan_threads;
    constexpr size_t num_elts = 10000000;

    PackedMemoryArray pma { /* block size */ 17, /* segment size */ 32, /* pages per extent */ 1, /* worker threads */ 8, /* segments per lock */ 2 };
    pma.set_max_number_workers(num_threads);

    distributions::RandomPermutationParallel sampler{ num_elts, /* seed */ 7 };
    int threads_started = 0;
    condition_variable _cvar;
    mutex _mutex;

    vector<thread*> threads;
    const int64_t num_keys_per_thread = num_elts / num_update_threads;
    const int64_t num_keys_leftover = num_elts % num_update_threads;
    int64_t start_position = 0;
    ::data_structures::global_parallel_scan_enabled = true;
    for(int i = 0; i < num_update_threads; i++){
        int64_t num_keys_to_insert = num_keys_per_thread + (i < num_keys_leftover);

        threads.push_back( new thread([&](int64_t pos_start, int64_t num_keys){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // insert the keys
            int64_t pos_end = pos_start + num_keys;
            for(int64_t pos = pos_start; pos < pos_end; pos++){
                int64_t key = sampler.get_raw_key(pos) +1;
                pma.insert(key, key * 10);
            }

            // done
            pma.unregister_thread();
        }, start_position, num_keys_to_insert) );

        start_position += num_keys_to_insert;
    }
    for(int i = 0; i < num_scan_threads; i++){
        threads.push_back( new thread( [&](){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // scan the array
            while(::data_structures::global_parallel_scan_enabled){
                pma.sum(0, numeric_limits<int64_t>::max());
            }

            // done
            pma.unregister_thread();
        }) );
    }
    assert(threads.size() == num_update_threads + num_scan_threads);
    for(int i = 0; i < num_update_threads; i++){
        threads[i]->join();
        delete threads[i];
        threads[i] = nullptr;
    }
    ::data_structures::global_parallel_scan_enabled = false;
    for(int i = num_update_threads; i < num_scan_threads; i++){
        threads[i]->join();
        delete threads[i];
        threads[i] = nullptr;
    }

    pma.set_max_number_workers(1);
    pma.register_thread(0);

    REQUIRE(pma.size() == num_elts);
    for(size_t i = 1; i <= num_elts; i++){
        REQUIRE(pma.find(i) == i * 10);
    }

    pma.unregister_thread();

    // remove the items, one by one
    pma.set_max_number_workers(num_threads);
    start_position = 0; threads_started = 0;
    threads.resize(0);
    ::data_structures::global_parallel_scan_enabled = true;
    for(int i = 0; i < num_update_threads; i++){
        int64_t num_keys_to_remove = num_keys_per_thread + (i < num_keys_leftover);

        threads.push_back(new thread([&](int64_t pos_start, int64_t num_keys){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // insert the keys
            int64_t pos_end = pos_start + num_keys;
            for(int64_t pos = pos_start; pos < pos_end; pos++){
                int64_t key = sampler.get_raw_key(pos) +1;
                auto value = pma.remove(key);
                REQUIRE(value == key * 10);
            }

            // done
            pma.unregister_thread();
        }, start_position, num_keys_to_remove));

        start_position += num_keys_to_remove;
    }
    for(int i = 0; i < num_scan_threads; i++){
        threads.push_back( new thread( [&](){
            int worker_id = -1;

            { // wait for all threads to start
                unique_lock<mutex> lock(_mutex);
                worker_id = threads_started;
                pma.register_thread(worker_id);
                threads_started++;
                _cvar.notify_all();
                if(threads_started < num_threads) { _cvar.wait(lock, [&](){ return threads_started == num_threads; }); }
            }

            // scan the array
            while(::data_structures::global_parallel_scan_enabled){
                pma.sum(0, numeric_limits<int64_t>::max());
            }

            // done
            pma.unregister_thread();
        }) );
    }
    assert(threads.size() == num_update_threads + num_scan_threads);
    for(int i = 0; i < num_update_threads; i++){
        threads[i]->join();
        delete threads[i];
        threads[i] = nullptr;
    }
    ::data_structures::global_parallel_scan_enabled = false;
    for(int i = num_update_threads; i < num_scan_threads; i++){
        threads[i]->join();
        delete threads[i];
        threads[i] = nullptr;
    }
    common::barrier();

    REQUIRE(pma.size() == 0);
    REQUIRE(pma.empty());
}
