#include "mutex_queue.h"

#include <atomic>
#include <string>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
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
    MutexQueue<int, 4> q;

    q.push(10);
    q.push(20);
    q.push(30);
    CHECK(q.size() == 3, "size should be 3 after 3 pushes");

    int a = q.pop();
    int b = q.pop();
    CHECK(a == 10, "first pop should be 10");
    CHECK(b == 20, "second pop should be 20");
    CHECK(q.size() == 1, "size should be 1 after 2 pops");

    int c = q.pop();
    CHECK(c == 30, "third pop should be 30");
    CHECK(q.empty(), "queue should be empty");
    std::printf("done\n");
}

void test_try_push_when_full() {
    std::printf("[test] try_push when full ... ");
    MutexQueue<int, 2> q;

    CHECK(q.try_push(1) == true,  "first try_push should succeed");
    CHECK(q.try_push(2) == true,  "second try_push should succeed");
    CHECK(q.try_push(3) == false, "third try_push should fail (full)");
    CHECK(q.size() == 2, "size should remain 2");
    std::printf("done\n");
}

void test_try_pop_when_empty() {
    std::printf("[test] try_pop when empty ... ");
    MutexQueue<int, 4> q;

    auto result = q.try_pop();
    CHECK(!result.has_value(), "try_pop on empty queue should return nullopt");

    q.push(42);
    result = q.try_pop();
    CHECK(result.has_value() && *result == 42, "try_pop should return 42");
    std::printf("done\n");
}

void test_move_semantics() {
    std::printf("[test] move semantics ... ");
    MutexQueue<std::string, 4> q;

    std::string s = "hello, world";
    q.push(std::move(s));
    // s is in a valid-but-unspecified state after move

    std::string out = q.pop();
    CHECK(out == "hello, world", "popped string should match original");
    std::printf("done\n");
}

void test_fifo_ordering() {
    std::printf("[test] FIFO ordering ... ");
    MutexQueue<int, 128> q;

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

void test_single_producer_single_consumer() {
    std::printf("[test] SPSC threaded ... ");
    constexpr int N = 100'000;
    MutexQueue<int, 256> q;

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

void test_multi_producer_multi_consumer() {
    std::printf("[test] MPMC threaded ... ");
    constexpr int num_producers = 4;
    constexpr int num_consumers = 4;
    constexpr int items_per_producer = 25'000;
    constexpr int total = num_producers * items_per_producer;

    MutexQueue<int, 256> q;
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    // Producers
    std::vector<std::thread> producers;
    for (int p = 0; p < num_producers; ++p) {
        producers.emplace_back([&, p] {
            for (int i = 0; i < items_per_producer; ++i) {
                q.push(p * items_per_producer + i);
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Consumers
    std::vector<std::thread> consumers;
    std::atomic<bool> done{false};
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&] {
            while (true) {
                auto val = q.try_pop();
                if (val.has_value()) {
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else if (done.load(std::memory_order_acquire)) {
                    // Drain any remaining items
                    while (auto remaining = q.try_pop()) {
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
    std::printf("done\n");
}

void test_blocking_push_backpressure() {
    std::printf("[test] blocking push backpressure ... ");
    MutexQueue<int, 4> q;

    // Fill the queue
    for (int i = 0; i < 4; ++i) q.push(i);

    std::atomic<bool> push_started{false};
    std::atomic<bool> push_completed{false};

    // This push should block because the queue is full
    std::thread producer([&] {
        push_started.store(true, std::memory_order_release);
        q.push(99);  // Will block until consumer pops
        push_completed.store(true, std::memory_order_release);
    });

    // Wait for the producer thread to start
    while (!push_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    // Give it a moment to actually block on the push
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK(!push_completed.load(std::memory_order_acquire),
          "push should still be blocked");

    // Pop one to unblock the producer
    q.pop();
    producer.join();

    CHECK(push_completed.load(std::memory_order_acquire),
          "push should have completed after pop");
    std::printf("done\n");
}

// ─── Quick throughput measurement ───────────────────────────────────────────

void bench_spsc_throughput() {
    std::printf("\n[bench] SPSC throughput (mutex baseline) ...\n");
    constexpr int N = 10'000'000;
    MutexQueue<std::uint64_t, 1024> q;

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
    std::printf("═══ MutexQueue Tests ═══\n\n");

    test_basic_push_pop();
    test_try_push_when_full();
    test_try_pop_when_empty();
    test_move_semantics();
    test_fifo_ordering();
    test_single_producer_single_consumer();
    test_multi_producer_multi_consumer();
    test_blocking_push_backpressure();

    bench_spsc_throughput();

    std::printf("\n═══ Results: %d passed, %d failed ═══\n",
                tests_passed.load(), tests_failed.load());

    return tests_failed.load() > 0 ? 1 : 0;
}
