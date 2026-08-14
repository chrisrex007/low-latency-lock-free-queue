#include "mpmc_queue.h"
#include "test_util.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

// ─── Tunables ───────────────────────────────────────────────────────────────
// Overridable so sanitizer builds can run a much smaller workload — TSan slows
// execution by roughly 10-20x, which makes the full-size stress test
// impractical.  See the `tsan` target in the Makefile.

#ifndef STRESS_ITEMS_PER_PRODUCER
#define STRESS_ITEMS_PER_PRODUCER 100000
#endif

#ifndef BENCH_OPS
#define BENCH_OPS 10000000
#endif


// ─── Tests ──────────────────────────────────────────────────────────────────

void test_basic_push_pop() {
    std::printf("[test] basic push/pop ... ");
    MPMCQueue<int, 4> q;

    CHECK(q.try_push(10), "push 10 should succeed");
    CHECK(q.try_push(20), "push 20 should succeed");
    CHECK(q.try_push(30), "push 30 should succeed");

    int a = q.pop();
    int b = q.pop();
    CHECK(a == 10, "first pop should be 10");
    CHECK(b == 20, "second pop should be 20");

    int c = q.pop();
    CHECK(c == 30, "third pop should be 30");
    std::printf("done\n");
}

void test_try_push_when_full() {
    std::printf("[test] try_push when full ... ");
    MPMCQueue<int, 2> q;

    // Unlike SPSCQueue, no slot is reserved to distinguish full from empty —
    // the per-slot sequence numbers do that — so Capacity=2 gives exactly 2
    // usable slots rather than SPSCQueue's 3.
    const std::size_t cap = q.capacity();
    std::printf("(usable capacity=%zu) ", cap);
    CHECK(cap == 2, "Capacity 2 should give exactly 2 usable slots");

    for (std::size_t i = 0; i < cap; ++i) {
        CHECK(q.try_push(static_cast<int>(i)), "push within capacity should succeed");
    }
    CHECK(!q.try_push(999), "push beyond capacity should fail");

    // Pop one, then push should succeed again.
    q.pop();
    CHECK(q.try_push(888), "push after pop should succeed");
    std::printf("done\n");
}

void test_try_pop_when_empty() {
    std::printf("[test] try_pop when empty ... ");
    MPMCQueue<int, 4> q;

    auto result = q.try_pop();
    CHECK(!result.has_value(), "try_pop on empty queue should return nullopt");

    q.push(42);
    result = q.try_pop();
    CHECK(result.has_value() && *result == 42, "try_pop should return 42");
    std::printf("done\n");
}

void test_move_semantics() {
    std::printf("[test] move semantics ... ");
    MPMCQueue<std::string, 4> q;

    std::string s = "hello, world";
    q.push(std::move(s));
    // s is in a valid-but-unspecified state after move

    std::string out = q.pop();
    CHECK(out == "hello, world", "popped string should match original");
    std::printf("done\n");
}

void test_single_producer_fifo_ordering() {
    std::printf("[test] single-producer FIFO ordering ... ");
    MPMCQueue<int, 128> q;

    // Deliberately named "single-producer": FIFO holds here only because one
    // thread issues every push in program order.  With concurrent producers,
    // MPMCQueue guarantees no global ordering — tickets are handed out by CAS,
    // so two producers racing on head_ can be interleaved arbitrarily.  The
    // invariants that DO hold under concurrency are completeness and
    // uniqueness, which test_multi_producer_multi_consumer checks instead.
    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        q.push(i);
    }
    bool in_order = true;
    for (int i = 0; i < N; ++i) {
        if (q.pop() != i) {
            in_order = false;
            break;
        }
    }
    CHECK(in_order, "elements from a single producer should come out in FIFO order");
    std::printf("done\n");
}

void test_wrap_around() {
    std::printf("[test] wrap-around ... ");
    // Small queue to force many laps around the ring, exercising the
    // sequence-number arithmetic (slot freed as pos + kBufSize).
    MPMCQueue<int, 4> q;

    constexpr int rounds = 1000;
    bool correct = true;
    for (int r = 0; r < rounds; ++r) {
        q.push(r);
        int v = q.pop();
        if (v != r) {
            correct = false;
            break;
        }
    }
    CHECK(correct, "push/pop should survive many wrap-arounds");
    std::printf("done\n");
}

void test_size_approx() {
    std::printf("[test] size_approx ... ");
    MPMCQueue<int, 8> q;

    CHECK(q.size_approx() == 0, "empty queue size should be 0");
    q.push(1);
    q.push(2);
    q.push(3);
    CHECK(q.size_approx() == 3, "size should be 3 after 3 pushes");
    q.pop();
    CHECK(q.size_approx() == 2, "size should be 2 after 1 pop");
    std::printf("done\n");
}

void test_capacity_rounding() {
    std::printf("[test] capacity rounding ... ");
    // No wasted slot here, so capacity is simply next_pow2(Capacity).
    // Capacity=1 → 1
    constexpr auto cap1 = MPMCQueue<int, 1>::capacity();
    CHECK(cap1 == 1, "Capacity 1 should give 1 usable slot");
    // Capacity=5 → next_pow2(5) = 8
    constexpr auto cap5 = MPMCQueue<int, 5>::capacity();
    CHECK(cap5 == 8, "Capacity 5 should round up to 8 usable slots");
    // Capacity=8 → already a power of 2, stays 8.  Note SPSCQueue<int,8>
    // reports 15 for the same argument because its +1 reservation tips it
    // over the next power-of-2 boundary.
    constexpr auto cap8 = MPMCQueue<int, 8>::capacity();
    CHECK(cap8 == 8, "Capacity 8 should stay at 8 usable slots");
    // Capacity=9 → next_pow2(9) = 16
    constexpr auto cap9 = MPMCQueue<int, 9>::capacity();
    CHECK(cap9 == 16, "Capacity 9 should round up to 16 usable slots");
    std::printf("done\n");
}

void test_threaded_single_producer_single_consumer() {
    std::printf("[test] threaded 1P1C correctness ... ");
    constexpr int N = 200'000;
    MPMCQueue<int, 1024> q;

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            q.push(i);
        }
    });

    std::vector<int> received;
    received.reserve(N);

    std::thread consumer([&] {
        for (int i = 0; i < N; ++i) {
            received.push_back(q.pop());
        }
    });

    producer.join();
    consumer.join();

    CHECK(static_cast<int>(received.size()) == N, "should receive all items");
    // Order is still strict here: with one thread per side there is no CAS
    // contention, so tickets are consumed in the order they were issued.
    bool in_order = true;
    for (int i = 0; i < N; ++i) {
        if (received[i] != i) {
            in_order = false;
            break;
        }
    }
    CHECK(in_order, "1P1C items should arrive in order");
    std::printf("done\n");
}

void test_multi_producer_multi_consumer() {
    std::printf("[test] MPMC threaded (completeness + uniqueness) ... ");
    constexpr int num_producers      = 4;
    constexpr int num_consumers      = 4;
    constexpr int items_per_producer = STRESS_ITEMS_PER_PRODUCER;
    constexpr int total              = num_producers * items_per_producer;

    // Capacity deliberately far smaller than the total item count so that
    // producers genuinely block on a full queue and consumers genuinely
    // observe an empty one — exercising both the diff<0 paths.
    MPMCQueue<int, 256> q;
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    // seen[v] counts how many times value v was delivered.  Every value must
    // be delivered exactly once; this catches loss, duplication, and payload
    // corruption, which a plain produced/consumed counter comparison cannot.
    std::vector<std::atomic<int>> seen(total);

    std::vector<std::thread> producers;
    for (int p = 0; p < num_producers; ++p) {
        producers.emplace_back([&, p] {
            // Unique ids across producers: a bijection onto [0, total).
            const int base = p * items_per_producer;
            for (int i = 0; i < items_per_producer; ++i) {
                q.push(base + i);
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> consumers;
    std::atomic<bool> done{false};
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&] {
            for (;;) {
                auto val = q.try_pop();
                if (val.has_value()) {
                    seen[static_cast<std::size_t>(*val)].fetch_add(
                        1, std::memory_order_relaxed);
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else if (done.load(std::memory_order_acquire)) {
                    // Drain what the producers left behind.  Unlike the
                    // equivalent loop in test_mutex_queue.cpp, this records
                    // each drained value — discarding them here would
                    // silently under-count and defeat the check below.
                    while (auto remaining = q.try_pop()) {
                        seen[static_cast<std::size_t>(*remaining)].fetch_add(
                            1, std::memory_order_relaxed);
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    }
                    break;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& t : producers) t.join();
    done.store(true, std::memory_order_release);
    for (auto& t : consumers) t.join();

    CHECK(produced.load() == total, "all items should be produced");
    CHECK(consumed.load() == total, "all items should be consumed");

    int missing = 0, duplicated = 0;
    for (int i = 0; i < total; ++i) {
        const int n = seen[i].load(std::memory_order_relaxed);
        if (n == 0)      ++missing;
        else if (n > 1)  ++duplicated;
    }
    if (missing || duplicated) {
        std::fprintf(stderr, "  (missing=%d duplicated=%d of %d)\n",
                     missing, duplicated, total);
    }
    CHECK(missing == 0, "no item should be lost");
    CHECK(duplicated == 0, "no item should be delivered more than once");
    std::printf("done\n");
}

// ─── Throughput benchmark ───────────────────────────────────────────────────

void bench_mpmc_throughput() {
    std::printf("\n[bench] MPMC throughput (lock-free, 4P4C) ...\n");
    constexpr int num_producers = 4;
    constexpr int num_consumers = 4;
    constexpr int N             = BENCH_OPS;
    constexpr int per_producer  = N / num_producers;
    constexpr int per_consumer  = N / num_consumers;
    // Keep the checksum exact by benchmarking only the evenly divisible part.
    constexpr int ops           = per_producer * num_producers;

    MPMCQueue<std::uint64_t, 4096> q;
    std::atomic<std::uint64_t> total_sum{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> producers;
    for (int p = 0; p < num_producers; ++p) {
        producers.emplace_back([&, p] {
            const std::uint64_t base =
                static_cast<std::uint64_t>(p) * per_producer;
            for (int i = 0; i < per_producer; ++i) {
                q.push(base + static_cast<std::uint64_t>(i));
            }
        });
    }

    std::vector<std::thread> consumers;
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&] {
            // Accumulate locally and publish once — a shared atomic in the hot
            // loop would measure contention on the counter, not on the queue.
            std::uint64_t local = 0;
            for (int i = 0; i < per_consumer; ++i) {
                local += q.pop();
            }
            total_sum.fetch_add(local, std::memory_order_relaxed);
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    double ops_per_sec = ops / (elapsed_ms / 1000.0);

    std::printf("  %d ops in %.2f ms  →  %.2f Mops/s\n", ops, elapsed_ms,
                ops_per_sec / 1e6);

    // Verify correctness: sum of 0..ops-1
    std::uint64_t expected =
        static_cast<std::uint64_t>(ops - 1) * static_cast<std::uint64_t>(ops) / 2;
    CHECK(total_sum.load() == expected, "sum should match expected total");
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main() {
    std::printf("═══ MPMCQueue Tests ═══\n\n");

    test_basic_push_pop();
    test_try_push_when_full();
    test_try_pop_when_empty();
    test_move_semantics();
    test_single_producer_fifo_ordering();
    test_wrap_around();
    test_size_approx();
    test_capacity_rounding();
    test_threaded_single_producer_single_consumer();
    test_multi_producer_multi_consumer();

    bench_mpmc_throughput();

    return test_summary();
}
