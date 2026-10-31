# distributed-token-bucket-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/distributed-token-bucket-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/distributed-token-bucket-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/distributed-token-bucket-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket-engine?style=flat)](https://github.com/sohamshewani/distributed-token-bucket-engine)

## Executive Summary & Problem Space

`distributed-token-bucket-engine` is an ultra-low-latency, enterprise-grade distributed rate limiting engine written in Java, engineered to protect hyperscale infrastructure from traffic anomalies, cascading failures, and distributed denial-of-service (DDoS) vectors. 

At multi-tenant cloud scales (10M+ RPS), conventional rate-limiting architectures suffer from severe structural bottlenecks:
1. **Garbage Collection (GC) Pauses:** Frequent allocation of ephemeral rate-limiting evaluation objects triggers Stop-the-World (STW) events, violating strict SLA p99.9 latency constraints.
2. **Lock Contention:** Centralized synchronization over shared rate limit buckets creates severe thread contention, degrading throughput under high-concurrency environments.
3. **Clock Drift in Distributed Nodes:** Asynchronous microservices executing across disparate physical hardware suffer from multi-millisecond NTP clock skew, leading to unfair token allocation bursts and rate-limit leakage.
4. **Memory Fragmentation:** Native and heap-managed buffers unaligned with CPU cache lines induce costly CPU cache misses and False Sharing.

This engine solves these challenges by combining a zero-allocation off-heap memory arena allocator, lock-free bucket sharding via CAS (Compare-And-Swap) primitives, a ring-buffer sliding window mechanism, and an active NTP clock skew compensation layer.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------------------+
 |                        DistributedRateLimiterEngine (Driver)                      |
 +-----------------------------------------------------------------------------------+
           |                                                       |
           v                                                       v
 +-----------------------------------+   +-------------------------------------------+
 |      RateLimitRequest (DTO)       |   |         ClockSkewCompensator              |
 |  - TenantID, Cost, Timestamp      |   |  - Monotonic Time Base & Drift Correction |
 +-----------------------------------+   +-------------------------------------------+
           |                                                       |
           +---------------------------+---------------------------+
                                       |
                                       v
 +-----------------------------------------------------------------------------------+
 |                             LockFreeBucketShard (Core)                            |
 |  - CAS-based token updates                                                        |
 |  - Thread-local affinity and striped sharding to prevent cache contention         |
 +-----------------------------------------------------------------------------------+
           |                                                       |
           v                                                       v
 +-----------------------------------+   +-------------------------------------------+
 |    SlidingWindowRingBuffer        |   |           NativeArenaAllocator            |
 |  - Lock-free circular ring vector |   |  - Unsafe direct memory / zero-GC impact  |
 |  - Temporal sub-window aggregation|   |  - Cache-line aligned layout (64-byte)    |
 +-----------------------------------+   +-------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

The engine enforces strict synchronization boundaries and deterministic memory management:
* **Lock-Free Sharding (`LockFreeBucketShard.java`):** Hot paths completely eschew `synchronized` blocks and heavy mutexes in favor of hardware-level Atomic CAS instructions (`VarHandle` and `AtomicLongFieldUpdater`). Shards are striped across CPU cores to isolate thread domains and eliminate false sharing.
* **Zero-Allocation Data Paths (`NativeArenaAllocator.java`):** Direct memory allocation via `sun.misc.Unsafe` bypasses the JVM heap entirely for high-frequency tracking structures. All native blocks align strictly to 64-byte cache line boundaries.
* **Sliding Window Buffering (`SlidingWindowRingBuffer.java`):** Fixed-size circular arrays maintain historical counters without dynamic resizing overhead or garbage collection pressure.
* **Skew Synchronization (`ClockSkewCompensator.java`):** Continuously tracks vector clocks and applies monotonic adjustment factors to maintain precision across distributed cluster nodes.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Time Complexity (Big-O) | Space Complexity | Estimated Latency (p99) | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Token Evaluation** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | $< 120\text{ns}$ | Lock-free check and atomic decrement via `LockFreeBucketShard`. |
| **Ring Window Slide** | $\mathcal{O}(1)$ | $\mathcal{O}(N)$ | $< 250\text{ns}$ | Circular buffer index advancement and window aggregate update. |
| **Memory Allocation**| $\mathcal{O}(1)$ | $\mathcal{O}(M)$ | $< 50\text{ns}$ | Arena-backed block reservation with zero heap footprint. |
| **Clock Correction** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | $< 15\text{ns}$ | Monotonic time normalization using skew drift coefficient. |

---

## Security Architecture & Threat Model

The engine operates under a Zero-Trust security paradigm for high-throughput ingestion gateways:
* **Input Boundary Sanitization (`RateLimitRequest.java`):** Strict bounds-checking on tenant identifiers, request weights, and timestamps mitigates integer overflow, negative-cost injection, and temporal replay vectors.
* **Memory Corruption Protections:** Direct memory bounds checking within `NativeArenaAllocator` prevents out-of-bounds pointer arithmetic and use-after-free vulnerabilities.
* **DoS Mitigation:** Rejects malformed metadata payloads at the boundary before incurring synchronization or allocation costs.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
* Java Development Kit (JDK) 17+ (Temurin or OpenJDK distribution recommended)
* Apache Maven 3.8.x+
* Python 3.9+ (with `pytest` and `numpy` for invariant and benchmark validation scripts)

### 1. Compile and Package
```bash
mvn clean package -DskipTests
```

### 2. Execute Verification Invariants
```bash
python3 tests/test_invariants.py
```

### 3. Run Performance Benchmarks
```bash
python3 benchmarks/bench_engine.py