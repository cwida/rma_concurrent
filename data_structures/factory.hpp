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

#ifndef DATA_STRUCTURES_FACTORY_HPP
#define DATA_STRUCTURES_FACTORY_HPP

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/errorhandling.hpp"

namespace experiments { class Interface; } // forward decl.

namespace data_structures {

#define REGISTER_DATA_STRUCTURE(name, description, callable) ::data_structures::factory().register_data_structure(name, description, callable, __FILE__, __LINE__)
#define REGISTER_EXPERIMENT(name, description, callable) ::data_structures::factory().register_experiment(name, description, callable, __FILE__, __LINE__)

// forward decl.
class Factory;
class Interface;

Factory& factory();

namespace factory_details {

    class ItemDescription {
    protected:
        std::string m_name;
        std::string m_description;
        const char* m_source;
        int m_line;
        bool m_display_in_help = true; // whether to show this item in the help screen

        ItemDescription(const std::string& name, const std::string& description, const char* source, int line);

    public:
        const std::string& name() const;

        const std::string& description() const;

        const char* source() const;

        int line() const;

        void set_display(bool value);

        bool is_display() const;

        virtual ~ItemDescription();
    };

    class FactoryImpl : public ItemDescription {
    protected:
        FactoryImpl(const std::string& name, const std::string& description, const char* source, int line) :
            ItemDescription(name, description, source, line) { }

    public:
        virtual std::unique_ptr<data_structures::Interface> make() { RAISE_EXCEPTION(common::Exception, "Not implemented"); }
        virtual std::unique_ptr<experiments::Interface> make(std::shared_ptr<data_structures::Interface>){ RAISE_EXCEPTION(common::Exception, "Not implemented"); };
    };

    template<typename Callable>
    class AlgorithmFactoryImpl : public FactoryImpl {
    protected:
        Callable m_callable;

    public:
        AlgorithmFactoryImpl(const std::string& name, const std::string& description, Callable callable, const char* source, int line) :
            FactoryImpl(name, description, source, line), m_callable(callable) {}

        std::unique_ptr<data_structures::Interface> make() override {
            return std::invoke(m_callable);
        }
    };

    template <typename Callable>
    class ExperimentFactoryImpl : public FactoryImpl {
    protected:
        Callable m_callable;

    public:
        ExperimentFactoryImpl(const std::string& name, const std::string& description, Callable callable, const char* source, int line) :
            FactoryImpl(name, description, source, line), m_callable(callable) { }

        std::unique_ptr<experiments::Interface> make(std::shared_ptr<data_structures::Interface> interface) override {
            return std::invoke(m_callable, interface);
        }
    };
} // namespace factory_details


class Factory {
    friend Factory& factory();
    static Factory singleton;

    using Impl = ::data_structures::factory_details::ItemDescription;

    std::vector<std::unique_ptr<Impl>> m_data_structures;
    std::vector<std::unique_ptr<Impl>> m_experiments;

    std::unique_ptr<data_structures::Interface> make_algorithm_generic(const std::string& name);

    std::unique_ptr<experiments::Interface> make_experiment_generic(const std::string& name, std::shared_ptr<Interface> interface);

    Factory();
public:
    ~Factory();

    const auto& algorithms() const { return m_data_structures; }

    const auto& experiments() const { return m_experiments; }

    template<typename Callable>
    void register_data_structure(const std::string& name, const std::string& description, Callable callable, const char* source, int line) {
        auto it = std::find_if(begin(m_data_structures), end(m_data_structures), [name](const std::unique_ptr<Impl>& impl){
            return impl->name() == name;
        });
        if(it != end(m_data_structures)){
            auto& r = *it;
            RAISE_EXCEPTION(common::Exception, "The data structure '" << name << "' has already been registered from: " << r->source() << ":" << r->line() << ". Attempting to register it again from: " << source << ":" << line);
        }

        m_data_structures.push_back(std::unique_ptr<Impl>{ new data_structures::factory_details::AlgorithmFactoryImpl<Callable>(name, description, callable, source, line)});
    }


    template<typename Callable>
    void register_experiment(const std::string& name, const std::string& description, Callable callable, const char* source, int line) {
        auto it = std::find_if(begin(m_experiments), end(m_experiments), [name](const std::unique_ptr<Impl>& impl){
            return impl->name() == name;
        });
        if(it != end(m_experiments)){
            auto& r = *it;
            RAISE_EXCEPTION(common::Exception, "The experiment '" << name << "' has already been registered from: " << r->source() << ":" << r->line() << ". Attempting to register it again from: " << source << ":" << line);
        }

        m_experiments.push_back(std::unique_ptr<Impl>{
            new data_structures::factory_details::ExperimentFactoryImpl<Callable>(name, description, callable, source, line)});
    }

    std::unique_ptr<data_structures::Interface> make_algorithm(const std::string& name);

    std::unique_ptr<experiments::Interface> make_experiment(const std::string& name, std::shared_ptr<Interface> pma);
};

} // namespace data_structures

#endif /* DATA_STRUCTURES_FACTORY_HPP */
