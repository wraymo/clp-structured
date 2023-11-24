#include "Profiler.hpp"

#include <iostream>

#include "spdlog/spdlog.h"

namespace clp_structured {
std::array<Profiler, static_cast<int>(ProfilingStage::Total) + 1> ProfilerManager::m_profilers;
#ifdef ENABLE_PROFILING
void Profiler::start() {
    m_start = std::chrono::high_resolution_clock::now();
}

void Profiler::stop() {
    m_stop = std::chrono::high_resolution_clock::now();
    m_elapsed_time += m_stop - m_start;
}

double Profiler::get_elapsed_time() const {
    return m_elapsed_time.count();
}

void ProfilerManager::start(ProfilingStage profiling_stage) {
    m_profilers[static_cast<int>(profiling_stage)].start();
}

void ProfilerManager::stop(ProfilingStage profiling_stage) {
    m_profilers[static_cast<int>(profiling_stage)].stop();
}

double ProfilerManager::get_elapsed_time(ProfilingStage profiling_stage) {
    return m_profilers[static_cast<int>(profiling_stage)].get_elapsed_time();
}

void ProfilerManager::print_elapsed_time() {
    for (int i = 0; i < static_cast<int>(ProfilingStage::Total) + 1; ++i) {
        spdlog::info(
                "ProfilerManager::ProfilingStage::{}: {} s",
                i,
                m_profilers[i].get_elapsed_time()
        );
    }
}

#else
void Profiler::start() {}

void Profiler::stop() {}

double Profiler::get_elapsed_time() const {
    return 0.0;
}

void ProfilerManager::start(ProfilingStage profiling_stage) {}

void ProfilerManager::stop(ProfilingStage profiling_stage) {}

double ProfilerManager::get_elapsed_time(ProfilingStage profiling_stage) {
    return 0.0;
}

void ProfilerManager::print_elapsed_time() {}

#endif
}  // namespace clp_structured
