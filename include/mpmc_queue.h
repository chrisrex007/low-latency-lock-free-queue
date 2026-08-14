#pragma once

#include "spin_hint.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>      // std::move, std::forward

/// Lock-free bounded multi-producer / multi-consumer queue.
///
/// Implements Dmitry Vyukov's bounded MPMC ring buffer.  Each slot carries its
/// own sequence number which acts as a per-slot state machine; producers and
/// consumers claim a slot with a single CAS on a shared index and then publish
/// via a release store on that slot's sequence number.
///
/// Key properties:
///   - The CAS only ever touches the index, never the payload.  There is no
///     ABA problem because the sequence numbers are monotonically increasing
///     ticket values, not reused pointers.
///   - Exactly one CAS per successful push or pop.
///   - **Lock-free, not wait-free.**  System-wide progress is guaranteed (some
///     thread always makes progress), but an individual thread's CAS can lose
///     the race repeatedly, so per-thread progress is not bounded.
///
/// Unlike SPSCQueue, every operation here is safe to call from any number of
/// concurrent threads.
///
/// Template parameters:
///   T        — element type (must be nothrow move- or copy-constructible)
///   Capacity — number of usable slots.  Rounded up to the next power of 2.
///              Unlike SPSCQueue, NO slot is reserved — see the note on
///              capacity() below.
template <typename T, std::size_t Capacity = 1024>
class MPMCQueue {
    static_assert(Capacity > 0, "Capacity must be greater than zero");
    static_assert(std::is_nothrow_move_constructible_v<T> ||
                      std::is_nothrow_copy_constructible_v<T>,
                  "T must be nothrow move- or copy-constructible");

    // ── Internal capacity: next power-of-2 >= Capacity ──────────────────
    // Note the absence of a "+1" here, which is the one real divergence from
    // SPSCQueue.  SPSCQueue tells full from empty purely by comparing its two
    // indices, so it must waste one slot to break the tie.  This queue encodes
    // each slot's state in its own sequence number instead, so every slot in
    // the buffer is usable.
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

    static constexpr std::size_t kBufSize = next_pow2(Capacity);
    static constexpr std::size_t kMask    = kBufSize - 1;

    // Cache-line size — 64 bytes on x86_64 and most aarch64.
    static constexpr std::size_t kCacheLine = 64;

    /// One ring slot.  `sequence_` is the synchronisation point; `data_` is
    /// only ever touched by the thread that has claimed the slot.
    ///
    /// Deliberately NOT padded to a full cache line individually.  Doing so
    /// would bloat the buffer up to 8x for small payloads (an 8-byte T would
    /// occupy 64 bytes plus its sequence number), and Vyukov's reference
    /// design does not pad either.  The array as a whole is cache-line aligned,
    /// matching the approach SPSCQueue already takes.  Per-slot padding is a
    /// legitimate micro-optimisation to revisit if slot-level false sharing
    /// ever shows up in a profile.
    struct Slot {
        std::atomic<std::size_t> sequence_;
        T                        data_;
    };

public:
    MPMCQueue() : head_(0), tail_(0) {
        // Seed each slot's sequence with its own index.  A producer at ticket
        // `pos` requires sequence == pos, so initially only slot 0 is writable
        // by the producer holding ticket 0, slot 1 by ticket 1, and so on.
        //
        // Relaxed is sufficient: construction happens-before any concurrent
        // access to the queue, the same assumption SPSCQueue and MutexQueue
        // already make.
        for (std::size_t i = 0; i < kBufSize; ++i) {
            slots_[i].sequence_.store(i, std::memory_order_relaxed);
        }
    }

    ~MPMCQueue() {
        // Drain any remaining elements to run destructors.
        while (try_pop().has_value()) {}
    }

    // Non-copyable, non-movable.
    MPMCQueue(const MPMCQueue&)            = delete;
    MPMCQueue& operator=(const MPMCQueue&) = delete;
    MPMCQueue(MPMCQueue&&)                 = delete;
    MPMCQueue& operator=(MPMCQueue&&)      = delete;

    // ── Producer API (safe from ANY number of threads) ──────────────────

    /// Non-blocking push.  Returns true on success, false if the queue is full.
    /// The claim protocol lives in try_push_impl() below.
    bool try_push(const T& item) { return try_push_impl(item); }

    /// Non-blocking push (move overload).
    bool try_push(T&& item) { return try_push_impl(std::move(item)); }

    /// Spinning push.  Blocks until space is available.
    void push(const T& item) {
        while (!try_push(item)) {
            spin_pause();
        }
    }

    /// Spinning push (move overload).
    void push(T&& item) {
        while (!try_push(std::move(item))) {
            spin_pause();
        }
    }

    // ── Consumer API (safe from ANY number of threads) ──────────────────

    /// Non-blocking pop.  Returns std::nullopt if the queue is empty.
    std::optional<T> try_pop() {
        Slot* slot;
        std::size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            slot = &slots_[pos & kMask];
            const std::size_t seq = slot->sequence_.load(std::memory_order_acquire);
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                // A producer has published into this slot and it is our turn.
                if (tail_.compare_exchange_weak(pos, pos + 1,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // Producer has not published here yet: the queue is empty.
                return std::nullopt;
            } else {
                // Another consumer claimed this ticket before us; re-read.
                pos = tail_.load(std::memory_order_relaxed);
            }
        }

        // Read the payload BEFORE freeing the slot — once the sequence store
        // below lands, a producer may immediately overwrite data_.
        std::optional<T> result(std::move(slot->data_));

        // Hand the slot to the producer that will hold ticket pos + kBufSize,
        // i.e. the same buffer index one lap further around the ring.
        slot->sequence_.store(pos + kBufSize, std::memory_order_release);
        return result;
    }

    /// Spinning pop.  Blocks until an item is available.
    T pop() {
        for (;;) {
            auto item = try_pop();
            if (item.has_value()) {
                return std::move(*item);
            }
            spin_pause();
        }
    }

    // ── Observers ───────────────────────────────────────────────────────

    /// Approximate size (may be stale; useful for diagnostics only).
    ///
    /// head_ and tail_ are read non-atomically with respect to each other, so
    /// this is a best-effort snapshot that can even read as slightly negative
    /// under concurrency — hence the clamp.
    std::size_t size_approx() const {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        return (h > t) ? (h - t) : 0;
    }

    /// Returns the usable capacity — the full buffer, since no slot is
    /// reserved.  Contrast with SPSCQueue::capacity(), which rounds
    /// Capacity + 1 up to a power of two and then subtracts the one slot it
    /// wastes to disambiguate full from empty.  For the same template
    /// argument the two therefore differ, e.g. for Capacity = 4096:
    ///     MPMCQueue<T, 4096>::capacity() == 4096
    ///     SPSCQueue<T, 4096>::capacity() == 8191
    static constexpr std::size_t capacity() { return kBufSize; }

private:
    /// Shared implementation of both try_push overloads.
    ///
    /// The copy and move versions differ in exactly one statement — how the
    /// payload lands in the claimed slot — so the CAS protocol is written
    /// once and the overloads forward into it.  That matters more here than
    /// it would in ordinary code: two hand-maintained copies of a lock-free
    /// claim loop invite a memory-ordering fix being applied to one and not
    /// the other, and the resulting race would be intermittent enough that
    /// no single test run could be trusted to expose it.
    ///
    /// U is deduced as `const T&` or `T`, so std::forward selects copy- or
    /// move-assignment into the slot with no extra temporary.
    template <typename U>
    bool try_push_impl(U&& item) {
        Slot* slot;
        std::size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            slot = &slots_[pos & kMask];
            const std::size_t seq = slot->sequence_.load(std::memory_order_acquire);
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                // Slot is free and it is our turn — try to claim ticket `pos`.
                if (head_.compare_exchange_weak(pos, pos + 1,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                    break;
                }
                // CAS failed: it already refreshed `pos` with the current head.
            } else if (diff < 0) {
                // The slot still holds an element a consumer has not taken yet,
                // meaning we have lapped the ring: the queue is full.
                return false;
            } else {
                // Another producer claimed this ticket before us; re-read.
                pos = head_.load(std::memory_order_relaxed);
            }
        }

        slot->data_ = std::forward<U>(item);
        // Release: publishes the payload write above to the consumer that
        // acquires this same sequence number.
        slot->sequence_.store(pos + 1, std::memory_order_release);
        return true;
    }

    // ── Data layout ─────────────────────────────────────────────────────
    // head_ and tail_ are each contended by every producer / every consumer
    // respectively, so they must not share a cache line with one another or
    // with the slot array.
    //
    // Note: unlike SPSCQueue, `slots_` is not merely written by one side —
    // the sequence numbers are read-modify-written from both sides, so the
    // separation matters even more here.
    alignas(kCacheLine) std::atomic<std::size_t> head_;   // producers' ticket counter
    alignas(kCacheLine) std::atomic<std::size_t> tail_;   // consumers' ticket counter

    // The slot array is default-initialised, which means T must be default
    // constructible.  This is a latent requirement rather than a static_assert
    // purely for parity with SPSCQueue, which has the same implicit constraint.
    // Like SPSCQueue, the hot path assigns into slots rather than constructing
    // in place; slots are written before they are ever read, so the
    // default-constructed values are never observed.
    alignas(kCacheLine) Slot slots_[kBufSize];
};
