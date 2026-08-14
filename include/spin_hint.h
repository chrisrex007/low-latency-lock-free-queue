#pragma once

/// Portable CPU spin hint for busy-wait loops.
///
/// Shared by SPSCQueue and MPMCQueue so both spin the same way — otherwise
/// benchmark comparisons between them measure the spin strategy as much as
/// the queue algorithm.
///
/// Why this matters on the hot path:
///   - On x86, PAUSE tells the CPU this is a spin-wait loop.  It de-pipelines
///     the loop (avoiding a memory-order-violation stall when the wait finally
///     ends) and, critically on SMT cores, yields execution resources to the
///     sibling hyperthread instead of hogging them doing nothing.
///   - On AArch64, YIELD is the equivalent hint.
///
/// This is a *hint*, not a syscall — it does not deschedule the thread.  A
/// spinning thread still owns its core, which is the intended behaviour for
/// low-latency work where a context switch costs far more than the spin.
static inline void spin_pause() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    // Unknown architecture — spin without a hint.
#endif
}
