/// bench/benchmark.cpp — standalone head-to-head benchmark: SPSC lock-free vs mutex.
///
/// Sweeps message sizes (8B, 64B, 256B).
/// Runs 3 iterations per configuration, reports median.
/// Emits CSV to stdout:
///   queue_type,msg_bytes,consumers,ops,median_mops,iter1_mops,iter2_mops,iter3_mops

#include "spsc_queue.h"

#include "mutex_queue.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

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

// ─── Timing helper ──────────────────────────────────────────────────────────

static double elapsed_ms(std::chrono::high_resolution_clock::time_point start,
                         std::chrono::high_resolution_clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

static double to_mops(int ops, double ms) {
    return (ops / (ms / 1000.0)) / 1e6;
}

// ─── SPSC benchmark ────────────────────────────────────────────────────────

template <std::size_t MsgBytes>
double bench_spsc(int ops) {
    using P = Payload<MsgBytes>;
    SPSCQueue<P, 4096> q;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < ops; ++i) {
            q.push(P(static_cast<std::uint64_t>(i)));
        }
    });

    std::uint64_t sum = 0;
    std::thread consumer([&] {
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
        for (int i = 0; i < ops; ++i) {
            q.push(P(static_cast<std::uint64_t>(i)));
        }
    });

    std::uint64_t sum = 0;
    std::thread consumer([&] {
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
                         int consumers, int ops, const BenchResult& r) {
    std::printf("%s,%zu,%d,%d,%.2f,%.2f,%.2f,%.2f\n",
                queue_type, msg_bytes, consumers, ops,
                r.median, r.iters[0], r.iters[1], r.iters[2]);
    std::fflush(stdout);  // flush immediately — stdout is fully buffered when redirected
}

// ─── Dispatch across message sizes ──────────────────────────────────────────

template <std::size_t MsgBytes>
void run_all_for_size(int ops) {
    // SPSC — always 1 consumer
    std::fprintf(stderr, "  SPSC <%4zuB> 1C ...\n", MsgBytes);
    auto r = run_3x([&] { return bench_spsc<MsgBytes>(ops); });
    emit_csv_row("spsc_lockfree", MsgBytes, 1, ops, r);

    // Mutex SPSC — 1 consumer
    std::fprintf(stderr, "  Mutex-SPSC <%4zuB> 1C ...\n", MsgBytes);
    r = run_3x([&] { return bench_mutex_spsc<MsgBytes>(ops); });
    emit_csv_row("mutex_spsc", MsgBytes, 1, ops, r);
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main() {
    constexpr int OPS = 2'000'000;

    // CSV header
    std::printf("queue_type,msg_bytes,consumers,ops,median_mops,iter1_mops,iter2_mops,iter3_mops\n");
    std::fflush(stdout);

    std::fprintf(stderr, "\n═══ Benchmark: 8B messages ═══\n");
    run_all_for_size<8>(OPS);

    std::fprintf(stderr, "\n═══ Benchmark: 64B messages ═══\n");
    run_all_for_size<64>(OPS);

    std::fprintf(stderr, "\n═══ Benchmark: 256B messages ═══\n");
    run_all_for_size<256>(OPS);

    std::fprintf(stderr, "\n═══ Benchmark complete. CSV written to stdout. ═══\n");
    return 0;
}
