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

#include <cinttypes>
#include "third-party/masstree/kvthread.hh"

/**
 * Global variables required by the Masstree implementation to alter & sync its behaviour
 */
extern kvepoch_t global_log_epoch;
extern volatile mrcu_epoch_type globalepoch; // global epoch, updated by main thread regularly
extern volatile uint64_t active_epoch; // the oldest epoch one of the worker threads is still in
extern bool recovering;
extern kvtimestamp_t initial_timestamp;

