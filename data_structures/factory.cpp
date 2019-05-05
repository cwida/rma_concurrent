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

#include "factory.hpp"

#include <algorithm>
#include <string>

#include "driver.hpp"
#include "interface.hpp"
#include "common/console_arguments.hpp"
#include "experiments/interface.hpp"

using namespace data_structures::factory_details;
using namespace std;

namespace data_structures {

Factory Factory::singleton;
Factory& factory(){ return Factory::singleton; }

Factory::Factory(){ }
Factory::~Factory() { }

unique_ptr<data_structures::Interface> Factory::make_algorithm_generic(const string& name){
    auto it = std::find_if(begin(m_data_structures), end(m_data_structures), [&name](decltype(m_data_structures[0])& impl){
        return impl->name() == name;
    });
    if (it == end(m_data_structures)){
        RAISE_EXCEPTION(common::Exception, "Implementation not found: " << name);
    }

    auto cast = dynamic_cast<FactoryImpl*>((*it).get());
    if(cast != nullptr){
        return cast->make();
    } else {
        RAISE_EXCEPTION(common::Exception, "The implementation " << name << " cannot be initialised for the given configuration");
    }
}

unique_ptr<data_structures::Interface> Factory::make_algorithm(const string& name){
    return make_algorithm_generic(name);
}

//unique_ptr<InterfaceNR> Factory::make_multi(const string& name){
//    return make_algorithm_generic<InterfaceNR>(name);
//}

std::unique_ptr<experiments::Interface> Factory::make_experiment_generic(const string& name, shared_ptr<data_structures::Interface> data_structure){
    auto it = std::find_if(begin(m_experiments), end(m_experiments), [&name](decltype(m_experiments[0])& impl){
        return impl->name() == name;
    });
    if (it == end(m_experiments)){
        RAISE_EXCEPTION(common::Exception, "Experiment not found: " << name);
    }

    auto cast = dynamic_cast<FactoryImpl*>((*it).get());
    if(cast != nullptr){
        return cast->make(data_structure);
    } else {
        RAISE_EXCEPTION(common::Exception, "The experiment " << name << " cannot be initialised for the given data structure");
    }
}

unique_ptr<experiments::Interface> Factory::make_experiment(const string& name, shared_ptr<data_structures::Interface> data_structure){
    return make_experiment_generic(name, data_structure);
}

//unique_ptr<Experiment> Factory::make_experiment(const string& name, shared_ptr<InterfaceNR> pma){
//    return make_experiment_generic(name, pma);
//}

namespace factory_details {
ItemDescription::ItemDescription(const string& name, const string& description, const char* source, int line) :
        m_name(name), m_description(description), m_source(source), m_line(line){ }
ItemDescription::~ItemDescription(){ };
const string& ItemDescription::name() const{ return m_name; }
const string& ItemDescription::description() const{ return m_description; }
const char* ItemDescription::source() const { return m_source; }
int ItemDescription::line() const{ return m_line; }
void ItemDescription::set_display(bool value){ m_display_in_help = value; }
bool ItemDescription::is_display() const { return m_display_in_help; }

} // namespace factory_details

} // namespace pma

