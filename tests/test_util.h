#pragma once

// Minimal shared harness for the three queue test binaries.
//
// Deliberately not a test framework.  GoogleTest or Catch2 would put a build
// dependency on a header-only project whose entire premise is that it has
// none — `g++ -Iinclude` and nothing else has to keep working.  This is the
// same hand-rolled counter/macro pattern each test file previously carried
// its own copy of, extracted so the three copies cannot drift.
//
// Each test binary is a single translation unit, so the counters below are
// per-binary state rather than anything shared between them.

#include <atomic>
#include <cstdio>

// Atomic because threaded tests call CHECK from worker threads.
inline std::atomic<int> tests_passed{0};
inline std::atomic<int> tests_failed{0};

// Records a pass or a failure.  A failing check reports the source location
// and keeps going, so one broken invariant does not mask the rest of the run.
#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            std::fprintf(stderr, "  FAIL: %s  (%s:%d)\n", msg, __FILE__,   \
                         __LINE__);                                         \
            tests_failed.fetch_add(1);                                      \
        } else {                                                            \
            tests_passed.fetch_add(1);                                      \
        }                                                                   \
    } while (0)

/// Prints the pass/fail summary and returns the process exit code — 0 if
/// every check passed, 1 otherwise.  Intended as `return test_summary();`.
inline int test_summary() {
    std::printf("\n═══ Results: %d passed, %d failed ═══\n",
                tests_passed.load(), tests_failed.load());
    return tests_failed.load() > 0 ? 1 : 0;
}
