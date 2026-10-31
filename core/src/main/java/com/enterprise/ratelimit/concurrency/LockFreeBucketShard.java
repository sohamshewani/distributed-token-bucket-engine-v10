package com.enterprise.ratelimit.concurrency;

import sun.misc.Unsafe;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

public final class LockFreeBucketShard implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize Unsafe", e);
        }
    }

    private static final long CACHE_LINE_ALIGNMENT = 64;
    private static final long BUCKET_STRUCT_SIZE = 16; // long tokens (8) + long lastRefillTimestamp (8)
    private static final long FIXED_POINT_SCALE = 1_000_000L;

    private final int capacitySlots;
    private final long windowDurationNanos;
    private final long baseAddress;
    private final long totalAllocationSize;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final long hazardPointerAddress;
    private static final AtomicReferenceFieldUpdater<LockFreeBucketShard, Thread> HAZARD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LockFreeBucketShard.class, Thread.class, "activeHazardThread");
    private volatile Thread activeHazardThread;

    public LockFreeBucketShard(int capacitySlots, long windowDurationNanos) {
        if (capacitySlots <= 0 || (capacitySlots & (capacitySlots - 1)) != 0) {
            throw new IllegalArgumentException("Capacity slots must be a positive power of two.");
        }
        if (windowDurationNanos <= 0) {
            throw new IllegalArgumentException("Window duration must be positive.");
        }

        this.capacitySlots = capacitySlots;
        this.windowDurationNanos = windowDurationNanos;

        // Calculate size with cache line padding
        long rawSize = (long) capacitySlots * BUCKET_STRUCT_SIZE;
        this.totalAllocationSize = rawSize + CACHE_LINE_ALIGNMENT;

        long allocated = UNSAFE.allocateMemory(this.totalAllocationSize);
        // Align to 64 bytes
        long remainder = allocated % CACHE_LINE_ALIGNMENT;
        this.baseAddress = (remainder == 0) ? allocated : (allocated + (CACHE_LINE_ALIGNMENT - remainder));

        // Allocate local hazard pointer slot metadata
        this.hazardPointerAddress = UNSAFE.allocateMemory(8);
        UNSAFE.putLong(this.hazardPointerAddress, 0L);

        // Zero initialize memory arena
        UNSAFE.setMemory(this.baseAddress, rawSize, (byte) 0);
    }

    private long getSlotAddress(int index) {
        return baseAddress + ((long) (index & (capacitySlots - 1)) * BUCKET_STRUCT_SIZE);
    }

    public boolean tryConsume(int slotIndex, long requestedTokens, long currentMonotonicTime, long refillRatePerNano, long maxCapacity, long skewDeltaNanos) {
        ensureOpen();
        acquireHazardPointer(Thread.currentThread());
        try {
            long slotAddr = getSlotAddress(slotIndex);
            long tokensAddr = slotAddr;
            long timestampAddr = slotAddr + 8;

            long adjustedTime = currentMonotonicTime + skewDeltaNanos;
            if (adjustedTime < 0) {
                adjustedTime = currentMonotonicTime;
            }

            while (true) {
                long currentTokens = UNSAFE.getLongVolatile(null, tokensAddr);
                long currentTimestamp = UNSAFE.getLongVolatile(null, timestampAddr);

                long timeElapsed = adjustedTime - currentTimestamp;
                long calculatedTokens = currentTokens;

                if (timeElapsed > 0) {
                    long generated = (long) ((double) timeElapsed * ((double) refillRatePerNano / 1000.0) * FIXED_POINT_SCALE);
                    if (generated > 0) {
                        calculatedTokens = Math.min(maxCapacity * FIXED_POINT_SCALE, currentTokens + generated);
                    }
                }

                long scaledRequested = requestedTokens * FIXED_POINT_SCALE;
                if (calculatedTokens < scaledRequested) {
                    return false;
                }

                long newTokens = calculatedTokens - scaledRequested;

                // Attempt CAS on tokens
                if (UNSAFE.compareAndSwapLong(null, tokensAddr, currentTokens, newTokens)) {
                    // Update timestamp atomically-ish or optimistically
                    UNSAFE.putLongVolatile(null, timestampAddr, adjustedTime);
                    return true;
                }
                // CAS failed, retry loop
            }
        } finally {
            releaseHazardPointer();
        }
    }

    public void rolloverSlot(int slotIndex, long resetTimestamp) {
        ensureOpen();
        acquireHazardPointer(Thread.currentThread());
        try {
            long slotAddr = getSlotAddress(slotIndex);
            long tokensAddr = slotAddr;
            long timestampAddr = slotAddr + 8;

            while (true) {
                long currentTokens = UNSAFE.getLongVolatile(null, tokensAddr);
                long currentTimestamp = UNSAFE.getLongVolatile(null, timestampAddr);

                if (UNSAFE.compareAndSwapLong(null, tokensAddr, currentTokens, 0L)) {
                    UNSAFE.putLongVolatile(null, timestampAddr, resetTimestamp);
                    break;
                }
            }
        } finally {
            releaseHazardPointer();
        }
    }

    private void acquireHazardPointer(Thread thread) {
        HAZARD_UPDATER.set(this, thread);
        UNSAFE.putLongVolatile(null, hazardPointerAddress, thread.getId());
    }

    private void releaseHazardPointer() {
        UNSAFE.putLongVolatile(null, hazardPointerAddress, 0L);
        HAZARD_UPDATER.set(this, null);
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LockFreeBucketShard is destroyed and closed.");
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // Wait for hazard pointers to clear
            while (activeHazardThread != null || UNSAFE.getLongVolatile(null, hazardPointerAddress) != 0) {
                LockSupport.parkNanos(100);
            }

            // Free allocated memory
            long actualAllocatedBase = baseAddress - (baseAddress % CACHE_LINE_ALIGNMENT);
            UNSAFE.freeMemory(actualAllocatedBase);
            UNSAFE.freeMemory(hazardPointerAddress);
        }
    }

    public int getCapacitySlots() {
        return capacitySlots;
    }

    public long getWindowDurationNanos() {
        return windowDurationNanos;
    }
}