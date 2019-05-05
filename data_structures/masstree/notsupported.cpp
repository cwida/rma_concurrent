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

/*
 * NOTE: This file is only compiled iff the support for masstree is not enabled.
 * To enable the data structure configure the project with the flag --with-masstree
 */

#if !defined(HAVE_MASSTREE)

#include "parallel.hpp"
#include "sequential.hpp"
#include "common/errorhandling.hpp"

#define ERROR_NOT_ENABLED RAISE_EXCEPTION(common::Exception, "Masstree implementation not enabled. Reconfigure the project as ./configure --with-masstree");

namespace data_structures::masstree {

/*****************************************************************************
 *                                                                           *
 *   Sequential                                                              *
 *                                                                           *
 *****************************************************************************/

Sequential::Sequential() : m_masstree(nullptr), m_thread(nullptr), m_cardinality(0) {
    ERROR_NOT_ENABLED
}

Sequential::~Sequential(){ }

void Sequential::insert(int64_t key, int64_t value){
    ERROR_NOT_ENABLED
}

int64_t Sequential::find(int64_t key) const{
    ERROR_NOT_ENABLED
}

int64_t Sequential::remove(int64_t key){
    ERROR_NOT_ENABLED
}

std::size_t Sequential::size() const{
    ERROR_NOT_ENABLED
}

bool Sequential::empty() const {
    ERROR_NOT_ENABLED
}

::data_structures::Interface::SumResult Sequential::sum(int64_t min, int64_t max) const {
    ERROR_NOT_ENABLED
}

std::unique_ptr<data_structures::Iterator> Sequential::iterator() const {
    ERROR_NOT_ENABLED
}

void Sequential::dump() const {
    ERROR_NOT_ENABLED
}


/*****************************************************************************
 *                                                                           *
 *   Parallel                                                                *
 *                                                                           *
 *****************************************************************************/


Parallel::Parallel() : m_masstree(nullptr), m_cardinality(0) {
    ERROR_NOT_ENABLED
}

Parallel::~Parallel(){ }

void Parallel::on_init_main(int num_threads) {
    ERROR_NOT_ENABLED
}

void Parallel::on_init_worker(int worker_id) {
    ERROR_NOT_ENABLED
}

void Parallel::on_destroy_worker(int worker_id) {
    ERROR_NOT_ENABLED
}

void Parallel::on_destroy_main(){
    ERROR_NOT_ENABLED
}

void Parallel::insert(int64_t key, int64_t value){
    ERROR_NOT_ENABLED
}

int64_t Parallel::find(int64_t key) const{
    ERROR_NOT_ENABLED
}

int64_t Parallel::remove(int64_t key){
    ERROR_NOT_ENABLED
}

std::size_t Parallel::size() const{
    ERROR_NOT_ENABLED
}

bool Parallel::empty() const {
    ERROR_NOT_ENABLED
}

::data_structures::Interface::SumResult Parallel::sum(int64_t min, int64_t max) const {
    ERROR_NOT_ENABLED
}

std::unique_ptr<Iterator> Parallel::iterator() const {
    ERROR_NOT_ENABLED
}

void Parallel::dump() const {
    ERROR_NOT_ENABLED
}

} // namespace data_structures::masstree

#endif
