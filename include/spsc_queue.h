#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <type_traits>

/// Lock-free single-producer / single-consumer bounded queue.
///
/// Uses a power-of-2 ring buffer with two cache-line-separated atomics
/// (head_ for the producer, tail_ for the consumer). No CAS — only
/// acquire/release loads and stores.
///
/// Template parameters:
///   T        — element type (must be nothrow move-constructible)
///   Capacity — number of usable slots. Rounded up to the next power of 2.
///              One slot is always reserved to distinguish full from empty,
///              so the internal buffer size is the rounded capacity + 1 (then
///              rounded up again to a power of 2).
template <typename T, std::size_t Capacity = 1024>
class SPSCQueue {
    static_assert(Capacity > 0, "Capacity must be greater than zero");
    static_assert(std::is_nothrow_move_constructible_v<T> ||
                      std::is_nothrow_copy_constructible_v<T>,
                  "T must be nothrow move- or copy-constructible");

    // ── Internal capacity: next power-of-2 >= Capacity + 1 ──────────────
    // We need Capacity usable slots.  One slot is wasted to tell full from
    // empty, so the buffer must hold at least Capacity+1 entries.  Round
    // that up to a power of two so we can mask instead of modulo.
    static constexpr std::size_t next_pow2(std::size_t v) {
        --v;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        return v + 1;
    }

    static constexpr std::size_t kBufSize = next_pow2(Capacity + 1);
    static constexpr std::size_t kMask    = kBufSize - 1;

    // Cache-line size — 64 bytes on x86_64 and most aarch64.
    static constexpr std::size_t kCacheLine = 64;

public:
    SPSCQueue() : head_(0), tail_(0) {
        // Placement-new is not needed; the array of T is default-initialized
        // only if T is trivially default-constructible.  We write before read,
        // so uninitialised slots are never observed.
    }

    ~SPSCQueue() {
        // Drain any remaining elements to run destructors.
        while (try_pop().has_value()) {}
    }

    // Non-copyable, non-movable.
    SPSCQueue(const SPSCQueue&)            = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&)                 = delete;
    SPSCQueue& operator=(SPSCQueue&&)      = delete;

    // ── Producer API (call from exactly ONE thread) ─────────────────────

    /// Non-blocking push.  Returns true on success, false if the queue is full.
    bool try_push(const T& item) {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t next = (h + 1) & kMask;

        // Full when next head would collide with current tail.
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        slots_[h] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    /// Non-blocking push (move overload).
    bool try_push(T&& item) {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t next = (h + 1) & kMask;

        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        slots_[h] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }

    /// Spinning push.  Blocks until space is available.
    void push(const T& item) {
        while (!try_push(item)) {
            // spin
        }
    }

    /// Spinning push (move overload).
    void push(T&& item) {
        while (!try_push(std::move(item))) {
            // spin
        }
    }

    // ── Consumer API (call from exactly ONE thread) ─────────────────────

    /// Non-blocking pop.  Returns std::nullopt if the queue is empty.
    std::optional<T> try_pop() {
        const std::size_t t = tail_.load(std::memory_order_relaxed);

        // Empty when tail == head.
        if (t == head_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        T item = std::move(slots_[t]);
        tail_.store((t + 1) & kMask, std::memory_order_release);
        return item;
    }

    /// Spinning pop.  Blocks until an item is available.
    T pop() {
        while (true) {
            auto item = try_pop();
            if (item.has_value()) {
                return std::move(*item);
            }
            // spin
        }
    }

    // ── Observers ───────────────────────────────────────────────────────

    /// Approximate size (may be stale; useful for diagnostics only).
    std::size_t size_approx() const {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        return (h - t + kBufSize) & kMask;
    }

    /// Returns the usable capacity (number of elements that can be stored).
    static constexpr std::size_t capacity() { return kBufSize - 1; }

private:
    // ── Data layout: head and tail on separate cache lines ──────────────
    alignas(kCacheLine) std::atomic<std::size_t> head_;
    alignas(kCacheLine) std::atomic<std::size_t> tail_;

    // Slot array — separate from head/tail to avoid false sharing.
    alignas(kCacheLine) T slots_[kBufSize];
};
