package com.engine.ratelimit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/* =========================================================================
   PRODUCTION-GRADE DISTRIBUTED TOKEN BUCKET ENGINE SOURCE
   ========================================================================= */

interface TimeProvider {
    long currentTimeMillis();
    Instant now();
}

class SystemTimeProvider implements TimeProvider {
    @Override public long currentTimeMillis() { return System.currentTimeMillis(); }
    @Override public Instant now() { return Instant.now(); }
}

class ManualTimeProvider implements TimeProvider {
    private final AtomicLong currentTime;
    public ManualTimeProvider(long initialMillis) { this.currentTime = new AtomicLong(initialMillis); }
    public void advance(long millis) { this.currentTime.addAndGet(millis); }
    public void set(long millis) { this.currentTime.set(millis); }
    @Override public long currentTimeMillis() { return currentTime.get(); }
    @Override public Instant now() { return Instant.ofEpochMilli(currentTime.get()); }
}

class RateLimitConfig {
    private final long capacity;
    private final long refillTokens;
    private final long refillPeriodMillis;
    private final double tokensPerMillisecond;

    public RateLimitConfig(long capacity, long refillTokens, Duration refillPeriod) {
        if (capacity <= 0) throw new IllegalArgumentException("Capacity must be > 0");
        if (refillTokens <= 0) throw new IllegalArgumentException("Refill tokens must be > 0");
        if (refillPeriod == null || refillPeriod.toMillis() <= 0) throw new IllegalArgumentException("Refill period must be > 0");
        this.capacity = capacity;
        this.refillTokens = refillTokens;
        this.refillPeriodMillis = refillPeriod.toMillis();
        this.tokensPerMillisecond = (double) refillTokens / (double) refillPeriodMillis;
    }

    public long getCapacity() { return capacity; }
    public long getRefillTokens() { return refillTokens; }
    public long getRefillPeriodMillis() { return refillPeriodMillis; }
    public double getTokensPerMillisecond() { return tokensPerMillisecond; }
}

interface DistributedLock {
    boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;
    void unlock();
}

interface BucketStateStore {
    BucketState get(String key);
    void put(String key, BucketState state);
    BucketState getAndLock(String key, long leaseTimeMillis);
    void atomicUpdate(String key, BucketState expectedState, BucketState newState);
}

class BucketState {
    private final long tokens;
    private final long lastRefillTimestamp;
    private final long version;

    public BucketState(long tokens, long lastRefillTimestamp, long version) {
        this.tokens = tokens;
        this.lastRefillTimestamp = lastRefillTimestamp;
        this.version = version;
    }

    public long getTokens() { return tokens; }
    public long getLastRefillTimestamp() { return lastRefillTimestamp; }
    public long getVersion() { return version; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BucketState)) return false;
        BucketState that = (BucketState) o;
        return tokens == that.tokens && lastRefillTimestamp == that.lastRefillTimestamp && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokens, lastRefillTimestamp, version);
    }
}

class ConcurrentMapBucketStore implements BucketStateStore {
    private final ConcurrentHashMap<String, BucketState> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> locks = new ConcurrentHashMap<>();

    @Override
    public BucketState get(String key) {
        return store.get(key);
    }

    @Override
    public void put(String key, BucketState state) {
        store.put(key, state);
    }

    @Override
    public BucketState getAndLock(String key, long leaseTimeMillis) {
        return store.get(key);
    }

    @Override
    public void atomicUpdate(String key, BucketState expectedState, BucketState newState) {
        if (expectedState == null) {
            BucketState prev = store.putIfAbsent(key, newState);
            if (prev != null) {
                throw new IllegalStateException("CAS failed: expected null, found state");
            }
        } else {
            boolean success = store.replace(key, expectedState, newState);
            if (!success) {
                throw new IllegalStateException("CAS failed: state modified concurrently");
            }
        }
    }
}

class RateLimitResult {
    private final boolean allowed;
    private final long remainingTokens;
    private final long waitForRefillMillis;
    private final long resetAfterMillis;

    public RateLimitResult(boolean allowed, long remainingTokens, long waitForRefillMillis, long resetAfterMillis) {
        this.allowed = allowed;
        this.remainingTokens = remainingTokens;
        this.waitForRefillMillis = waitForRefillMillis;
        this.resetAfterMillis = resetAfterMillis;
    }

    public boolean isAllowed() { return allowed; }
    public long getRemainingTokens() { return remainingTokens; }
    public long getWaitForRefillMillis() { return waitForRefillMillis; }
    public long getResetAfterMillis() { return resetAfterMillis; }
}

class DistributedTokenBucketEngine {
    private final RateLimitConfig config;
    private final BucketStateStore store;
    private final TimeProvider timeProvider;
    private final int maxCasRetries;

    public DistributedTokenBucketEngine(RateLimitConfig config, BucketStateStore store, TimeProvider timeProvider, int maxCasRetries) {
        this.config = config;
        this.store = store;
        this.timeProvider = timeProvider;
        this.maxCasRetries = maxCasRetries;
    }

    public RateLimitResult tryConsume(String key, long requestedTokens) {
        if (requestedTokens <= 0) {
            throw new IllegalArgumentException("Requested tokens must be > 0");
        }
        if (requestedTokens > config.getCapacity()) {
            return new RateLimitResult(false, 0, -1, 0);
        }

        long now = timeProvider.currentTimeMillis();

        for (int attempt = 0; attempt < maxCasRetries; attempt++) {
            BucketState currentState = store.get(key);
            BucketState calculatedState;
            long currentTokens;

            if (currentState == null) {
                long tokensAfterRefill = Math.max(0, config.getCapacity() - requestedTokens);
                boolean allowed = tokensAfterRefill >= 0 && currentStateTokensValid(config.getCapacity(), requestedTokens);
                // Wait, if initial, tokens = capacity. If requested <= capacity, allowed = true.
                long actualTokens = config.getCapacity();
                long tokensToSave = actualTokens - requestedTokens;
                calculatedState = new BucketState(tokensToSave, now, 1L);
                
                try {
                    store.atomicUpdate(key, null, calculatedState);
                    long waitForRefill = tokensToSave >= 0 ? 0 : calculateWaitTime(Math.abs(tokensToSave));
                    long resetAfter = calculateResetTime(tokensToSave);
                    return new RateLimitResult(true, Math.max(0, tokensToSave), waitForRefill, resetAfter);
                } catch (IllegalStateException e) {
                    continue; // CAS conflict, retry
                }
            } else {
                long timePassed = now - currentState.getLastRefillTimestamp();
                long generatedTokens = 0;
                if (timePassed > 0) {
                    generatedTokens = (long) (timePassed * config.getTokensPerMillisecond());
                }

                long tokensWithRefill = Math.min(config.getCapacity(), currentState.getTokens() + generatedTokens);
                long effectiveLastRefill = timePassed > 0 ? currentState.getLastRefillTimestamp() + (long)(generatedTokens / config.getTokensPerMillisecond()) : currentState.getLastRefillTimestamp();
                
                // If no tokens generated, keep timestamp to avoid time drift loss
                if (generatedTokens == 0) {
                    effectiveLastRefill = currentState.getLastRefillTimestamp();
                }

                if (tokensWithRefill >= requestedTokens) {
                    long remaining = tokensWithRefill - requestedTokens;
                    BucketState newState = new BucketState(remaining, effectiveLastRefill, currentState.getVersion() + 1);
                    try {
                        store.atomicUpdate(key, currentState, newState);
                        return new RateLimitResult(true, remaining, 0, calculateResetTime(remaining));
                    } catch (IllegalStateException e) {
                        continue;
                    }
                } else {
                    long deficit = requestedTokens - tokensWithRefill;
                    long waitMillis = (long) Math.ceil(deficit / config.getTokensPerMillisecond());
                    return new RateLimitResult(false, tokensWithRefill, waitMillis, calculateResetTime(tokensWithRefill));
                }
            }
        }
        throw new RuntimeException("Failed to acquire rate limit after " + maxCasRetries + " CAS attempts due to high contention");
    }

    private boolean currentStateTokensValid(long capacity, long requested) {
        return requested <= capacity;
    }

    private long calculateWaitTime(long missingTokens) {
        return (long) Math.ceil(missingTokens / config.getTokensPerMillisecond());
    }

    private long calculateResetTime(long currentTokens) {
        long missing = config.getCapacity() - currentTokens;
        if (missing <= 0) return 0;
        return (long) Math.ceil(missing / config.getTokensPerMillisecond());
    }
}


/* =========================================================================
   EXHAUSTIVE UNIT, INVARIANT, AND CONCURRENT STRESS-TEST SUITE
   ========================================================================= */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DistributedTokenBucketEngineTest {

    private ManualTimeProvider timeProvider;
    private ConcurrentMapBucketStore stateStore;
    private RateLimitConfig standardConfig;
    private DistributedTokenBucketEngine engine;

    @BeforeEach
    void setUp() {
        timeProvider = new ManualTimeProvider(1000L);
        stateStore = new ConcurrentMapBucketStore();
        // Capacity: 10 tokens, Refill: 10 tokens per 1000ms (1 token per 100ms)
        standardConfig = new RateLimitConfig(10, 10, Duration.ofMillis(1000));
        engine = new DistributedTokenBucketEngine(standardConfig, stateStore, timeProvider, 100);
    }

    /* =========================================================
       1. BOUNDARY AND EDGE-CASE ASSERTIONS
       ========================================================= */

    @Test
    @DisplayName("Edge Case: Zero or Negative Token Requests Should Throw Exception")
    void testInvalidRequests() {
        assertThrows(IllegalArgumentException.class, () -> engine.tryConsume("key1", 0));
        assertThrows(IllegalArgumentException.class, () -> engine.tryConsume("key1", -5));
    }

    @Test
    @DisplayName("Edge Case: Requesting More Than Total Capacity Fails Instantly")
    void testRequestExceedsCapacity() {
        RateLimitResult result = engine.tryConsume("key1", 15);
        assertFalse(result.isAllowed());
        assertEquals(0, result.getRemainingTokens());
        assertEquals(-1, result.getWaitForRefillMillis());
    }

    @Test
    @DisplayName("Boundary: Requesting Exactly Total Capacity Succeeds and Empties Bucket")
    void testRequestExactCapacity() {
        RateLimitResult result = engine.tryConsume("key1", 10);
        assertTrue(result.isAllowed());
        assertEquals(0, result.getRemainingTokens());
        assertEquals(0, result.getWaitForRefillMillis());

        // Subsequent request should fail
        RateLimitResult next = engine.tryConsume("key1", 1);
        assertFalse(next.isAllowed());
        assertTrue(next.getWaitForRefillMillis() > 0);
    }

    @Test
    @DisplayName("Boundary: Config Validation Guardrails")
    void testInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new Rate