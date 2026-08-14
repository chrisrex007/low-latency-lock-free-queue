/// bench/benchmark.cpp — head-to-head queue benchmark.
///
/// Compares:
///   spsc_lockfree / mutex_spsc  — 1 producer, 1 consumer (SPSC is 1P1C by
///                                 definition; the mutex queue is its control)
///   mpmc_lockfree / mutex_mpmc  — swept across a producer/consumer matrix
///
/// Sweeps message sizes (8B, 64B, 256B), runs 3 iterations per configuration
/// and reports the median.
///
/// Emits CSV to stdout:
///   queue_type,msg_bytes,producers,consumers,ops,pinned,median_mops,
///   iter1_mops,iter2_mops,iter3_mops
///
/// Usage:
///   ./benchmark [--ops N] [--configs 1x1,4x4,16x16] [--help]
/// Environment:
///   BENCH_PIN=1   pin benchmark threads to cores (Linux only, off by default)

#include "spsc_queue.h"

#include "mutex_queue.h"

#include "mpmc_queue.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

// ─── Payload types ──────────────────────────────────────────────────────────
// Fixed-size structs with a sequence number for correctness verification.

template <std::size_t N>
struct Payload {
    std::uint64_t seq;
    char pad[N - sizeof(std::uint64_t)];

    Payload() : seq(0) { std::memset(pad, 0, sizeof(pad)); }
    explicit Payload(std::uint64_t s) : seq(s) { std::memset(pad, 0, sizeof(pad)); }
};

// 8-byte payload (just the sequence number, no pad)
template <>
struct Payload<8> {
    std::uint64_t seq;

    Payload() : seq(0) {}
    explicit Payload(std::uint64_t s) : seq(s) {}
};

static_assert(sizeof(Payload<8>)    == 8,    "Payload<8> must be 8 bytes");
static_assert(sizeof(Payload<64>)   == 64,   "Payload<64> must be 64 bytes");
static_assert(sizeof(Payload<256>)  == 256,  "Payload<256> must be 256 bytes");
static_assert(sizeof(Payload<1024>) == 1024, "Payload<1024> must be 1024 bytes");

// ─── Runtime configuration ──────────────────────────────────────────────────

struct ThreadConfig {
    int producers;
    int consumers;
};

static bool g_pin = false;   // set from BENCH_PIN

static unsigned hw_threads() {
    const unsigned n = std::thread::hardware_concurrency();
    return n ? n : 2;        // hardware_concurrency() may legitimately return 0
}

/// Pin the calling thread to a core, if pinning is enabled.
///
/// Off by default so the standard run stays portable and reflects ordinary
/// scheduler behaviour; enable it via BENCH_PIN=1 to cut run-to-run variance
/// when producing publishable numbers.
///
/// Called from *inside* each worker as its first action rather than from the
/// spawning thread: std::thread starts running on construction, so pinning
/// from outside would leave a window in which the thread is already doing
/// timed work on whatever core the scheduler happened to pick.
static void pin_current_thread(int core_id) {
#ifdef __linux__
    if (!g_pin) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(core_id) % hw_threads(), &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)core_id;
#endif
}

/// Default thread-count matrix, derived from the machine rather than hardcoded.
///
/// A fixed list would either under-exercise a large server or oversubscribe a
/// small dev box.  Three fixed asymmetric configs probe the producer- and
/// consumer-bound cases; the symmetric configs then double until both sides
/// together would exceed the available hardware threads.
static std::vector<ThreadConfig> default_configs() {
    const unsigned n = hw_threads();
    std::vector<ThreadConfig> cfgs = {{1, 1}, {2, 1}, {1, 2}};
    for (unsigned p = 2; 2 * p <= n; p *= 2) {
        cfgs.push_back({static_cast<int>(p), static_cast<int>(p)});
    }
    return cfgs;
}

static std::string config_label(const ThreadConfig& c) {
    return std::to_string(c.producers) + "P" + std::to_string(c.consumers) + "C";
}

// ─── Timing helpers ─────────────────────────────────────────────────────────

static double elapsed_ms(std::chrono::high_resolution_clock::time_point start,
                         std::chrono::high_resolution_clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

static double to_mops(int ops, double ms) {
    return (ops / (ms / 1000.0)) / 1e6;
}

// ─── SPSC benchmark (1P1C only) ────────────────────────────────────────────
// Deliberately NOT routed through the generic harness below: SPSCQueue exposes
// the same method names but is only correct with exactly one thread per side,
// so instantiating it there would compile and then be silently racy.

template <std::size_t MsgBytes>
double bench_spsc(int ops) {
    using P = Payload<MsgBytes>;
    SPSCQueue<P, 4096> q;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        pin_current_thread(0);
        for (int i = 0; i < ops; ++i) {
            q.push(P(static_cast<std::uint64_t>(i)));
        }
    });

    std::uint64_t sum = 0;
    std::thread consumer([&] {
        pin_current_thread(1);
        for (int i = 0; i < ops; ++i) {
            sum += q.pop().seq;
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();

    // Verify correctness
    std::uint64_t expected = static_cast<std::uint64_t>(ops - 1) * ops / 2;
    if (sum != expected) {
        std::fprintf(stderr, "SPSC<%zu> checksum mismatch!\n", MsgBytes);
    }

    return to_mops(ops, elapsed_ms(start, end));
}

// ─── Mutex benchmark (SPSC mode: 1P/1C) ────────────────────────────────────

template <std::size_t MsgBytes>
double bench_mutex_spsc(int ops) {
    using P = Payload<MsgBytes>;
    MutexQueue<P, 4096> q;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        pin_current_thread(0);
        for (int i = 0; i < ops; ++i) {
            q.push(P(static_cast<std::uint64_t>(i)));
        }
    });

    std::uint64_t sum = 0;
    std::thread consumer([&] {
        pin_current_thread(1);
        for (int i = 0; i < ops; ++i) {
            sum += q.pop().seq;
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();

    std::uint64_t expected = static_cast<std::uint64_t>(ops - 1) * ops / 2;
    if (sum != expected) {
        std::fprintf(stderr, "Mutex-SPSC<%zu> checksum mismatch!\n", MsgBytes);
    }

    return to_mops(ops, elapsed_ms(start, end));
}

// ─── Generic multi-producer / multi-consumer harness ────────────────────────
// Used for MPMCQueue and MutexQueue only — both are safe with any number of
// threads on either side.
//
// Total `ops` is held constant across every thread-count configuration, which
// is what makes a throughput-vs-thread-count comparison meaningful.  Work is
// partitioned statically up front (no shared atomic cursor), so the only
// contention measured is contention on the queue itself.

template <typename Queue, std::size_t MsgBytes>
double bench_mp_mc(int ops, int num_producers, int num_consumers) {
    using P = Payload<MsgBytes>;
    Queue q;

    // Split `ops` as evenly as possible; the first `remainder` threads take one
    // extra item so the totals still sum to exactly `ops`.
    auto split = [](int total, int n) {
        std::vector<int> counts(static_cast<std::size_t>(n), total / n);
        for (int i = 0; i < total % n; ++i) ++counts[static_cast<std::size_t>(i)];
        return counts;
    };
    const std::vector<int> prod_counts = split(ops, num_producers);
    const std::vector<int> cons_counts = split(ops, num_consumers);

    // Prefix sums give each producer a disjoint id range covering [0, ops).
    std::vector<int> prod_offsets(static_cast<std::size_t>(num_producers), 0);
    for (int i = 1; i < num_producers; ++i) {
        prod_offsets[static_cast<std::size_t>(i)] =
            prod_offsets[static_cast<std::size_t>(i - 1)] +
            prod_counts[static_cast<std::size_t>(i - 1)];
    }

    std::atomic<std::uint64_t> total_sum{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> producers;
    producers.reserve(static_cast<std::size_t>(num_producers));
    for (int p = 0; p < num_producers; ++p) {
        producers.emplace_back([&, p] {
            pin_current_thread(p);
            const int base  = prod_offsets[static_cast<std::size_t>(p)];
            const int count = prod_counts[static_cast<std::size_t>(p)];
            for (int i = 0; i < count; ++i) {
                q.push(P(static_cast<std::uint64_t>(base + i)));
            }
        });
    }

    std::vector<std::thread> consumers;
    consumers.reserve(static_cast<std::size_t>(num_consumers));
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&, c] {
            pin_current_thread(num_producers + c);
            // Each consumer pops a pre-assigned count.  Because the producer
            // and consumer counts both sum to exactly `ops`, no consumer can
            // starve — and skipping drain detection keeps the timed loop as
            // tight as the 1P1C benchmarks above.
            const int count = cons_counts[static_cast<std::size_t>(c)];
            std::uint64_t local = 0;
            for (int i = 0; i < count; ++i) {
                local += q.pop().seq;
            }
            // Publish once: a shared atomic inside the loop would measure
            // contention on the counter rather than on the queue.
            total_sum.fetch_add(local, std::memory_order_relaxed);
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto end = std::chrono::high_resolution_clock::now();

    const std::uint64_t expected =
        static_cast<std::uint64_t>(ops - 1) * static_cast<std::uint64_t>(ops) / 2;
    if (total_sum.load() != expected) {
        std::fprintf(stderr, "MPMC<%zu> %dP%dC checksum mismatch!\n",
                     MsgBytes, num_producers, num_consumers);
    }

    return to_mops(ops, elapsed_ms(start, end));
}

// Note on ring sizes: MPMCQueue<P,4096> holds exactly 4096 slots, while the
// existing SPSCQueue<P,4096> actually holds 8191 (its reserved slot pushes
// next_pow2(4097) to 8192, minus one).  With ops far larger than either bound
// steady-state throughput is dominated by cache and atomic behaviour rather
// than queue depth, so this does not materially skew the comparison — but it
// is called out rather than left as a silent discrepancy.

template <std::size_t MsgBytes>
double bench_mpmc(int ops, int p, int c) {
    return bench_mp_mc<MPMCQueue<Payload<MsgBytes>, 4096>, MsgBytes>(ops, p, c);
}

template <std::size_t MsgBytes>
double bench_mutex_mpmc(int ops, int p, int c) {
    return bench_mp_mc<MutexQueue<Payload<MsgBytes>, 4096>, MsgBytes>(ops, p, c);
}

// ─── Run a benchmark 3 times, return median ─────────────────────────────────

struct BenchResult {
    double median;
    double iters[3];
};

template <typename Fn>
BenchResult run_3x(Fn&& fn) {
    BenchResult r;
    for (int i = 0; i < 3; ++i) {
        r.iters[i] = fn();
    }
    std::array<double, 3> sorted = {r.iters[0], r.iters[1], r.iters[2]};
    std::sort(sorted.begin(), sorted.end());
    r.median = sorted[1];
    return r;
}

// ─── CSV emitter ────────────────────────────────────────────────────────────

static void emit_csv_row(const char* queue_type, std::size_t msg_bytes,
                         int producers, int consumers, int ops,
                         const BenchResult& r) {
    std::printf("%s,%zu,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f\n",
                queue_type, msg_bytes, producers, consumers, ops,
                g_pin ? 1 : 0,
                r.median, r.iters[0], r.iters[1], r.iters[2]);
    std::fflush(stdout);  // flush immediately — stdout is fully buffered when redirected
}

// ─── Dispatch across message sizes ──────────────────────────────────────────

template <std::size_t MsgBytes>
void run_all_for_size(int ops, const std::vector<ThreadConfig>& configs) {
    // SPSC — 1P1C by definition
    std::fprintf(stderr, "  SPSC       <%4zuB> 1P1C ...\n", MsgBytes);
    auto r = run_3x([&] { return bench_spsc<MsgBytes>(ops); });
    emit_csv_row("spsc_lockfree", MsgBytes, 1, 1, ops, r);

    // Mutex SPSC — the original 1P1C control
    std::fprintf(stderr, "  Mutex-SPSC <%4zuB> 1P1C ...\n", MsgBytes);
    r = run_3x([&] { return bench_mutex_spsc<MsgBytes>(ops); });
    emit_csv_row("mutex_spsc", MsgBytes, 1, 1, ops, r);

    // MPMC lock-free vs mutex, across the thread-count matrix
    for (const auto& cfg : configs) {
        std::fprintf(stderr, "  MPMC       <%4zuB> %s ...\n",
                     MsgBytes, config_label(cfg).c_str());
        r = run_3x([&] {
            return bench_mpmc<MsgBytes>(ops, cfg.producers, cfg.consumers);
        });
        emit_csv_row("mpmc_lockfree", MsgBytes, cfg.producers, cfg.consumers, ops, r);

        std::fprintf(stderr, "  Mutex-MPMC <%4zuB> %s ...\n",
                     MsgBytes, config_label(cfg).c_str());
        r = run_3x([&] {
            return bench_mutex_mpmc<MsgBytes>(ops, cfg.producers, cfg.consumers);
        });
        emit_csv_row("mutex_mpmc", MsgBytes, cfg.producers, cfg.consumers, ops, r);
    }
}

// ─── Argument parsing ───────────────────────────────────────────────────────

static void print_usage(const char* argv0) {
    std::fprintf(stderr,
        "Usage: %s [--ops N] [--configs LIST] [--help]\n"
        "\n"
        "  --ops N         total operations per run (default 2000000)\n"
        "  --configs LIST  comma-separated PxC thread configs, e.g. 1x1,4x4,16x16\n"
        "                  (default: derived from hardware_concurrency())\n"
        "\n"
        "Environment:\n"
        "  BENCH_PIN=1     pin threads to cores (Linux only, off by default)\n"
        "\n"
        "Message sizes are compile-time (8B, 64B, 256B) because the payload is a\n"
        "template parameter, so they are not configurable at runtime.\n"
        "CSV is written to stdout; progress is written to stderr.\n",
        argv0);
}

/// Parse "1x1,4x4,16x16" into thread configs.  Returns false on malformed input.
static bool parse_configs(const char* spec, std::vector<ThreadConfig>& out) {
    std::vector<ThreadConfig> parsed;
    const std::string s(spec);
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok =
            s.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        if (tok.empty()) return false;

        const std::size_t x = tok.find('x');
        if (x == std::string::npos) return false;

        const int p = std::atoi(tok.substr(0, x).c_str());
        const int c = std::atoi(tok.substr(x + 1).c_str());
        if (p <= 0 || c <= 0) return false;
        parsed.push_back({p, c});

        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    if (parsed.empty()) return false;
    out = parsed;
    return true;
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    int ops = 2'000'000;
    std::vector<ThreadConfig> configs = default_configs();

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (std::strcmp(argv[i], "--ops") == 0 && i + 1 < argc) {
            ops = std::atoi(argv[++i]);
            if (ops <= 0) {
                std::fprintf(stderr, "error: --ops must be positive\n");
                return 1;
            }
        } else if (std::strcmp(argv[i], "--configs") == 0 && i + 1 < argc) {
            if (!parse_configs(argv[++i], configs)) {
                std::fprintf(stderr, "error: malformed --configs (expected e.g. 1x1,4x4)\n");
                return 1;
            }
        } else {
            std::fprintf(stderr, "error: unrecognised argument '%s'\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    const char* pin_env = std::getenv("BENCH_PIN");
    g_pin = pin_env && std::strcmp(pin_env, "0") != 0;

    std::fprintf(stderr, "hardware_concurrency=%u  ops=%d  pinned=%s\n",
                 hw_threads(), ops, g_pin ? "yes" : "no");
    std::fprintf(stderr, "configs:");
    for (const auto& c : configs) std::fprintf(stderr, " %s", config_label(c).c_str());
    std::fprintf(stderr, "\n");

    // CSV header
    std::printf("queue_type,msg_bytes,producers,consumers,ops,pinned,"
                "median_mops,iter1_mops,iter2_mops,iter3_mops\n");
    std::fflush(stdout);

    std::fprintf(stderr, "\n═══ Benchmark: 8B messages ═══\n");
    run_all_for_size<8>(ops, configs);

    std::fprintf(stderr, "\n═══ Benchmark: 64B messages ═══\n");
    run_all_for_size<64>(ops, configs);

    std::fprintf(stderr, "\n═══ Benchmark: 256B messages ═══\n");
    run_all_for_size<256>(ops, configs);

    std::fprintf(stderr, "\n═══ Benchmark complete. CSV written to stdout. ═══\n");
    return 0;
}
