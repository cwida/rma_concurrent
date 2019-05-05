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

#include <iostream>
#include <memory>
#include <utility>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "rma/common/static_index.hpp"

using namespace data_structures::rma::common;
using namespace std;

TEST_CASE("only_root"){
    StaticIndex index(/* node size */ 4, /* number of keys */ 3);
    index.set_separator_key(0, 10);
    index.set_separator_key(1, 20);
    index.set_separator_key(2, 30);

    REQUIRE(index.get_separator_key(0) == 10);
    REQUIRE(index.get_separator_key(1) == 20);
    REQUIRE(index.get_separator_key(2) == 30);

    REQUIRE(index.find(5) == 0);
    REQUIRE(index.find(10) == 0);
    REQUIRE(index.find(15) == 0);
    REQUIRE(index.find(20) == 1);
    REQUIRE(index.find(25) == 1);
    REQUIRE(index.find(30) == 2);
    REQUIRE(index.find(35) == 2);

    REQUIRE(index.find_first(5) == 0);
    REQUIRE(index.find_first(10) == 0);
    REQUIRE(index.find_first(15) == 0);
    REQUIRE(index.find_first(20) == 0);
    REQUIRE(index.find_first(25) == 1);
    REQUIRE(index.find_first(30) == 1);
    REQUIRE(index.find_first(35) == 2);

    REQUIRE(index.find_last(5) == 0);
    REQUIRE(index.find_last(10) == 0);
    REQUIRE(index.find_last(15) == 0);
    REQUIRE(index.find_last(20) == 1);
    REQUIRE(index.find_last(25) == 1);
    REQUIRE(index.find_last(30) == 2);
    REQUIRE(index.find_last(35) == 2);


    index.dump();
}

TEST_CASE("height2"){
    StaticIndex index(/* node size */ 4, /* number of keys */ 7);
    for(int i = 0; i < 7; i++){ index.set_separator_key(i, (i+1) * 10); } // 10, 20, 30, 40, 50, 60, 70

    // check
    for(int i = 0; i < 7; i++) { REQUIRE(index.get_separator_key(i) == (i+1) *10); }

    REQUIRE(index.find(5) == 0);
    for(int64_t key = 10; key <= 75; key+=5){
        auto segment_id = index.find(key);
        REQUIRE(segment_id == (key / 10) -1);
    }

    for(int64_t key = 5; key <= 20; key += 5){ REQUIRE(index.find_first(key) == 0); }
    for(int64_t key = 25; key <= 75; key += 5){
        int64_t expected_segment = ((key -1) / 10) -1;
        REQUIRE(index.find_first(key) == expected_segment);
    }
    for(int64_t key = 75; key >= 5; key -= 5){
        int64_t expected_segment = max<int64_t>(( key - 10 ) / 10, 0);
        REQUIRE(index.find_last(key) == expected_segment);
    }


    index.dump();
}

TEST_CASE("full_tree"){
    constexpr size_t num_keys = 64;

    StaticIndex index(/* node size */ 4, /* number of keys */ num_keys);
    for(int i = 0; i < num_keys; i++){
        index.set_separator_key(i, (i+1) * 10);
        for(int j = 0; j <= i; j++) {
            REQUIRE(index.get_separator_key(j) == (j+1) * 10);
        }
    } // 10, 20, 30, 40, 50, 60, 70, etc.

    // check
    for(int i = 0; i < num_keys; i++){
        REQUIRE(index.find((i+1) * 10 -1) == max(i -1, 0));
        REQUIRE(index.find((i+1) * 10) == i);
        REQUIRE(index.find((i+1) * 10 +1) == i);
    }


    index.dump();
}

TEST_CASE("height5"){
    constexpr size_t num_keys = 366;

    StaticIndex index(/* node size */ 4, /* number of keys */ num_keys);
    for(int i = 0; i < num_keys; i++){
        index.set_separator_key(i, (i+1) * 10);
        for(int j = 0; j <= i; j++) {
            REQUIRE(index.get_separator_key(j) == (j+1) * 10);
        }
    } // 10, 20, 30, 40, 50, 60, 70, etc.

    // check
    for(int i = 0; i < num_keys; i++){
        REQUIRE(index.find((i+1) * 10 -1) == max(i -1, 0));
        REQUIRE(index.find((i+1) * 10) == i);
        REQUIRE(index.find((i+1) * 10 +1) == i);
    }
}

TEST_CASE("height6"){
    constexpr size_t num_keys = 4000;

    StaticIndex index(/* node size */ 5, /* number of keys */ num_keys);
    REQUIRE(index.height() == 6);
    for(int i = 0; i < num_keys; i++){
        index.set_separator_key(i, (i+1) * 10);
    } // 10, 20, 30, 40, 50, 60, 70, etc.

    for(int i = 0; i < num_keys; i++) {
        REQUIRE(index.get_separator_key(i) == (i+1) * 10);
    }

    // check
    for(int i = 0; i < num_keys; i++){
        REQUIRE(index.find((i+1) * 10 -1) == max(i -1, 0));
        REQUIRE(index.find((i+1) * 10) == i);
        REQUIRE(index.find((i+1) * 10 +1) == i);
    }
}
