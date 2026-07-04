#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>

/// Thread-safe bounded queue using std::mutex + std::condition_variable.
/// Serves as the baseline (control group) for benchmarking against
/// lock-free SPSC and SPMC implementations.
///
/// Template parameters:
///   T        — element type (must be movable)
///   Capacity — maximum number of elements the queue can hold
template <typename T, std::size_t Capacity = 1024>
class MutexQueue {
public:
    static_assert(Capacity > 0, "Capacity must be greater than zero");

    MutexQueue() = default;

    // Non-copyable, non-movable — the mutex and cv are not movable.
    MutexQueue(const MutexQueue&) = delete;
    MutexQueue& operator=(const MutexQueue&) = delete;
    MutexQueue(MutexQueue&&) = delete;
    MutexQueue& operator=(MutexQueue&&) = delete;

    /// Blocking push. Waits until space is available, then enqueues the item.
    void push(const T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return queue_.size() < Capacity; });
        queue_.push(item);
        not_empty_.notify_one();
    }

    /// Blocking push (move overload).
    void push(T&& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return queue_.size() < Capacity; });
        queue_.push(std::move(item));
        not_empty_.notify_one();
    }

    /// Non-blocking push. Returns true if the item was enqueued, false if full.
    bool try_push(const T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= Capacity) {
            return false;
        }
        queue_.push(item);
        not_empty_.notify_one();
        return true;
    }

    /// Non-blocking push (move overload).
    bool try_push(T&& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= Capacity) {
            return false;
        }
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    /// Blocking pop. Waits until an item is available, then dequeues it.
    T pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return !queue_.empty(); });
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    /// Non-blocking pop. Returns std::nullopt if the queue is empty.
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    /// Returns the current number of elements (snapshot — may be stale).
    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    /// Returns true if the queue is empty (snapshot — may be stale).
    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    /// Returns the fixed capacity of the queue.
    static constexpr std::size_t capacity() { return Capacity; }

private:
    mutable std::mutex      mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::queue<T>           queue_;
};
