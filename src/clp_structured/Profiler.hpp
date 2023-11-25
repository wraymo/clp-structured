#ifndef CLP_STRUCTURED_PROFILER_HPP
#define CLP_STRUCTURED_PROFILER_HPP

#include <array>
#include <chrono>

namespace clp_structured {

enum class ProfilingStage : int {
    ReadSchemaTree,
    ReadSchemaMap,
    TransformQuery,
    ReadVariableDictionary,
    ReadLogTypeDictionary,
    ReadEncodedTable,
    SearchVariableDictionary,
    SearchLogTypeDictionary,
    ScanAndFilter,
    MarshalResults,
    Total
};

class Profiler {
public:
    // Constructor
    Profiler() = default;

    // Destructor
    ~Profiler() = default;

    /**
     * Starts the timer
     */
    inline void start();

    /**
     * Stops the timer
     */
    inline void stop();

    /**
     * @return The elapsed time in milliseconds
     */
    inline double get_elapsed_time() const;

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> m_start;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_stop;
    std::chrono::duration<double> m_elapsed_time{};
};

class ProfilerManager {
public:
    // Constructor
    ProfilerManager() = default;

    // Destructor
    ~ProfilerManager() = default;

    /**
     * Starts the timer for the given profiling stage
     * @param stage
     */
    static void start(ProfilingStage stage);

    /**
     * Stops the timer for the given profiling stage
     * @param stage
     */
    static void stop(ProfilingStage stage);

    /**
     * @return The elapsed time in seconds for the given profiling stage
     */
    static double get_elapsed_time(ProfilingStage stage);

    /**
     * Print the elapsed time in seconds for all profiling stages
     */
    static void print_elapsed_time();

private:
    static std::array<Profiler, static_cast<int>(ProfilingStage::Total) + 1> m_profilers;
};
}  // namespace clp_structured

#endif  // CLP_STRUCTURED_PROFILER_HPP
