#include "spsc_queue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

// ─── Test helpers ───────────────────────────────────────────────────────────

static std::atomic<int> tests_passed{0};
static std::atomic<int> tests_failed{0};

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

// ─── Tests ──────────────────────────────────────────────────────────────────

void test_basic_push_pop() {
    std::printf("[test] basic push/pop ... ");
    SPSCQueue<int, 4> q;

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
    SPSCQueue<int, 2> q;

    // capacity() returns kBufSize-1; for Capacity=2, kBufSize=4, so
    // usable capacity is 3 (next_pow2(2+1)=4, minus 1 wasted slot).
    const std::size_t cap = q.capacity();
    std::printf("(usable capacity=%zu) ", cap);

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
    SPSCQueue<int, 4> q;

    auto result = q.try_pop();
    CHECK(!result.has_value(), "try_pop on empty queue should return nullopt");

    q.push(42);
    result = q.try_pop();
    CHECK(result.has_value() && *result == 42, "try_pop should return 42");
    std::printf("done\n");
}

void test_fifo_ordering() {
    std::printf("[test] FIFO ordering ... ");
    SPSCQueue<int, 128> q;

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
    CHECK(in_order, "elements should come out in FIFO order");
    std::printf("done\n");
}

void test_wrap_around() {
    std::printf("[test] wrap-around ... ");
    // Small queue to force multiple wrap-arounds.
    SPSCQueue<int, 4> q;

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
    SPSCQueue<int, 8> q;

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
    // Capacity=5 → need 6 slots → next_pow2(6)=8 → usable = 7
    constexpr auto cap5 = SPSCQueue<int, 5>::capacity();
    CHECK(cap5 == 7, "Capacity 5 should round to 7 usable slots");
    // Capacity=8 → need 9 slots → next_pow2(9)=16 → usable = 15
    constexpr auto cap8 = SPSCQueue<int, 8>::capacity();
    CHECK(cap8 == 15, "Capacity 8 should round to 15 usable slots");
    // Capacity=1 → need 2 slots → next_pow2(2)=2 → usable = 1
    constexpr auto cap1 = SPSCQueue<int, 1>::capacity();
    CHECK(cap1 == 1, "Capacity 1 should give 1 usable slot");
    std::printf("done\n");
}

void test_threaded_spsc() {
    std::printf("[test] threaded SPSC correctness ... ");
    constexpr int N = 1'000'000;
    SPSCQueue<int, 1024> q;

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
    bool in_order = true;
    for (int i = 0; i < N; ++i) {
        if (received[i] != i) {
            in_order = false;
            break;
        }
    }
    CHECK(in_order, "SPSC items should arrive in order");
    std::printf("done\n");
}

// ─── Throughput benchmark ───────────────────────────────────────────────────

void bench_spsc_throughput() {
    std::printf("\n[bench] SPSC throughput (lock-free) ...\n");
    constexpr int N = 10'000'000;
    SPSCQueue<std::uint64_t, 1024> q;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            q.push(static_cast<std::uint64_t>(i));
        }
    });

    std::uint64_t sum = 0;
    std::thread consumer([&] {
        for (int i = 0; i < N; ++i) {
            sum += q.pop();
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    double ops_per_sec = N / (elapsed_ms / 1000.0);

    std::printf("  %d ops in %.2f ms  →  %.2f Mops/s\n", N, elapsed_ms,
                ops_per_sec / 1e6);

    // Verify correctness: sum of 0..N-1
    std::uint64_t expected = static_cast<std::uint64_t>(N - 1) * N / 2;
    CHECK(sum == expected, "sum should match expected total");
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main() {
    std::printf("═══ SPSCQueue Tests ═══\n\n");

    test_basic_push_pop();
    test_try_push_when_full();
    test_try_pop_when_empty();
    test_fifo_ordering();
    test_wrap_around();
    test_size_approx();
    test_capacity_rounding();
    test_threaded_spsc();

    bench_spsc_throughput();

    std::printf("\n═══ Results: %d passed, %d failed ═══\n",
                tests_passed.load(), tests_failed.load());

    return tests_failed.load() > 0 ? 1 : 0;
}
