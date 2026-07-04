CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -Iinclude -pthread

TESTS = test_spsc_queue test_mutex_queue

.PHONY: all run run-bench plot clean

all: $(TESTS)

test_spsc_queue: tests/test_spsc_queue.cpp include/spsc_queue.h
	$(CXX) $(CXXFLAGS) -o $@ $<



test_mutex_queue: tests/test_mutex_queue.cpp include/mutex_queue.h
	$(CXX) $(CXXFLAGS) -o $@ $<

run: $(TESTS)
	@echo "Running tests..." > output.log
	@./test_spsc_queue >> output.log 2>&1 || true

	@./test_mutex_queue >> output.log 2>&1 || true
	@cat output.log
	@echo "All tests executed. Results in output.log"

benchmark: bench/benchmark.cpp include/spsc_queue.h include/mutex_queue.h
	$(CXX) $(CXXFLAGS) -o benchmark bench/benchmark.cpp

run-bench: benchmark
	./benchmark > bench_results.csv
	@echo "Benchmark complete. Results in bench_results.csv"

plot:
	python3 bench/plot.py

clean:
	rm -f $(TESTS) benchmark output.log bench_results.csv \
	      throughput_vs_msgsize.png
