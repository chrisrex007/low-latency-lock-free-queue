CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -Iinclude -pthread

# Benchmark needs _GNU_SOURCE for cpu_set_t / pthread_setaffinity_np (pinning).
BENCHFLAGS = $(CXXFLAGS) -D_GNU_SOURCE

# Sanitizer builds: -O1 keeps them tolerably fast while staying debuggable.
# The stress workloads are scaled down because TSan costs roughly 10-20x.
TSANFLAGS = -std=c++17 -Wall -Wextra -O1 -g -fsanitize=thread -Iinclude -pthread
ASANFLAGS = -std=c++17 -Wall -Wextra -O1 -g -fsanitize=address,undefined -Iinclude -pthread
SMALL     = -DSTRESS_ITEMS_PER_PRODUCER=2000 -DBENCH_OPS=40000 -DTHREADED_N=20000

# TSan is incompatible with the high-entropy ASLR used by recent kernels and
# aborts with "FATAL: ThreadSanitizer: unexpected memory mapping". Running
# under `setarch -R` disables ASLR for the child and sidesteps it.
SETARCH := $(shell command -v setarch >/dev/null 2>&1 && echo setarch $(shell uname -m) -R)

# halt_on_error + a distinct exitcode make a detected race fail the build
# loudly, instead of printing a warning that scrolls past unnoticed.
TSAN_OPTIONS_ENV = TSAN_OPTIONS="halt_on_error=1 exitcode=66"

TESTS      = test_spsc_queue test_mutex_queue test_mpmc_queue
TSAN_BINS  = test_spsc_tsan test_mutex_tsan test_mpmc_tsan
ASAN_BINS  = test_spsc_asan test_mutex_asan test_mpmc_asan

HEADERS_SPSC  = include/spsc_queue.h include/spin_hint.h
HEADERS_MUTEX = include/mutex_queue.h
HEADERS_MPMC  = include/mpmc_queue.h include/spin_hint.h

# Shared test harness (CHECK macro + pass/fail counters), included by all three
# test files but NOT by the benchmark — kept out of HEADERS_* so a change here
# does not force a benchmark rebuild.
TEST_UTIL     = tests/test_util.h

.PHONY: all run run-bench plot tsan sanitize clean clean-results

all: $(TESTS)

test_spsc_queue: tests/test_spsc_queue.cpp $(HEADERS_SPSC) $(TEST_UTIL)
	$(CXX) $(CXXFLAGS) -o $@ $<

test_mutex_queue: tests/test_mutex_queue.cpp $(HEADERS_MUTEX) $(TEST_UTIL)
	$(CXX) $(CXXFLAGS) -o $@ $<

test_mpmc_queue: tests/test_mpmc_queue.cpp $(HEADERS_MPMC) $(TEST_UTIL)
	$(CXX) $(CXXFLAGS) -o $@ $<

run: $(TESTS)
	@echo "Running tests..." > output.log
	@./test_spsc_queue >> output.log 2>&1 || true
	@./test_mutex_queue >> output.log 2>&1 || true
	@./test_mpmc_queue >> output.log 2>&1 || true
	@cat output.log
	@echo "All tests executed. Results in output.log"

# ─── ThreadSanitizer ────────────────────────────────────────────────────────
# The highest-value correctness check for the lock-free queues: a clean TSan
# run is real evidence the acquire/release pairings are right, in a way that a
# passing stress test alone is not.
tsan:
	$(CXX) $(TSANFLAGS) $(SMALL) -o test_spsc_tsan  tests/test_spsc_queue.cpp
	$(CXX) $(TSANFLAGS) $(SMALL) -o test_mutex_tsan tests/test_mutex_queue.cpp
	$(CXX) $(TSANFLAGS) $(SMALL) -o test_mpmc_tsan  tests/test_mpmc_queue.cpp
	@echo "── TSan: SPSCQueue ──"  && $(TSAN_OPTIONS_ENV) $(SETARCH) ./test_spsc_tsan
	@echo "── TSan: MutexQueue ──" && $(TSAN_OPTIONS_ENV) $(SETARCH) ./test_mutex_tsan
	@echo "── TSan: MPMCQueue ──"  && $(TSAN_OPTIONS_ENV) $(SETARCH) ./test_mpmc_tsan
	@echo "TSan run complete — clean (a detected race would have failed this target)."

sanitize:
	$(CXX) $(ASANFLAGS) $(SMALL) -o test_spsc_asan  tests/test_spsc_queue.cpp
	$(CXX) $(ASANFLAGS) $(SMALL) -o test_mutex_asan tests/test_mutex_queue.cpp
	$(CXX) $(ASANFLAGS) $(SMALL) -o test_mpmc_asan  tests/test_mpmc_queue.cpp
	@./test_spsc_asan && ./test_mutex_asan && ./test_mpmc_asan
	@echo "ASan/UBSan run complete."

# ─── Benchmark ──────────────────────────────────────────────────────────────

benchmark: bench/benchmark.cpp $(HEADERS_SPSC) $(HEADERS_MUTEX) $(HEADERS_MPMC)
	$(CXX) $(BENCHFLAGS) -o benchmark bench/benchmark.cpp

# Pass extra args through, e.g.  make run-bench BENCH_ARGS="--ops 500000"
# Set BENCH_PIN=1 in the environment to pin threads to cores.
run-bench: benchmark
	./benchmark $(BENCH_ARGS) > bench_results.csv
	@echo "Benchmark complete. Results in bench_results.csv"

plot:
	python3 bench/plot.py

# Removes build artifacts only. bench_results.csv and the charts are committed
# results, not build output — deleting them here would silently stage
# deletions of tracked files and break the README's embedded images.
clean:
	rm -f $(TESTS) $(TSAN_BINS) $(ASAN_BINS) benchmark output.log

# Explicitly discard the committed benchmark results and charts.
clean-results:
	rm -f bench_results.csv throughput_vs_msgsize.png throughput_vs_threads.png
