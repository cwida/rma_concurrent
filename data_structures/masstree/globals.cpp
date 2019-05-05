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

#include "third-party/masstree/kvthread.hh"

kvepoch_t global_log_epoch;
volatile mrcu_epoch_type globalepoch = 1; // global epoch, updated by the main thread regularly
volatile uint64_t active_epoch = 1; // min active epoch, updated by the main thread regularly
volatile bool recovering = false; // so don't add log entries, and free old value immediately
kvtimestamp_t initial_timestamp; // unclear at the moment
