package com.enterprise.ratelimit.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import sun.misc.Unsafe;

public final class SlidingWindowRingBuffer implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final long CACHE_LINE_SIZE = 64;
    private static final int COUNTER_SIZE_BYTES = 8;
    
    private final int windowSizeMs;
    private final int bucketIntervalMs;
    private final int capacity;
    private final long totalSizeBytes;
    private final long baseAddress;
    private final AtomicBoolean isClosed;
    private final Cleaner.Cleanable cleanable;

    private static final Cleaner CLEANER = Cleaner.create();

    private static final class State implements Runnable {
        private final long address;
        private final long size;

        private State(long address, long size) {
            this.address = address;
            this.size = size;
        }

        @Override
        public void run() {
            if (address != 0) {
                UNSAFE.freeMemory(address);
            }
        }
    }

    public SlidingWindowRingBuffer(int windowSizeMs, int bucketIntervalMs) {
        if (windowSizeMs <= 0 || bucketIntervalMs <= 0) {
            throw new IllegalArgumentException("Window size and bucket interval must be positive.");
        }
        if (windowSizeMs % bucketIntervalMs != 0) {
            throw new IllegalArgumentException("Window size must be evenly divisible by bucket interval.");
        }
        
        this.windowSizeMs = windowSizeMs;
        this.bucketIntervalMs = bucketIntervalMs;
        this.capacity = windowSizeMs / bucketIntervalMs;
        
        long rawSize = (long) capacity * COUNTER_SIZE_BYTES;
        long remainder = rawSize % CACHE_LINE_SIZE;
        this.totalSizeBytes = remainder == 0 ? rawSize : rawSize + (CACHE_LINE_SIZE - remainder);
        
        this.baseAddress = UNSAFE.allocateMemory(this.totalSizeBytes);
        UNSAFE.setMemory(this.baseAddress, this.totalSizeBytes, (byte) 0);
        
        this.isClosed = new AtomicBoolean(false);
        this.cleanable = CLEANER.register(this, new State(this.baseAddress, this.totalSizeBytes));
    }

    public int getCapacity() {
        return capacity;
    }

    public int getWindowSizeMs() {
        return windowSizeMs;
    }

    public int getBucketIntervalMs() {
        return bucketIntervalMs;
    }

    private long calculateOffset(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for capacity " + capacity);
        }
        return baseAddress + ((long) index * COUNTER_SIZE_BYTES);
    }

    public void increment(int index, long delta) {
        if (isClosed.get()) {
            throw new IllegalStateException("SlidingWindowRingBuffer is closed.");
        }
        long offset = calculateOffset(index);
        long current;
        do {
            current = UNSAFE.getLongVolatile(null, offset);
        } while (!UNSAFE.compareAndSwapLong(null, offset, current, current + delta));
    }

    public long get(int index) {
        if (isClosed.get()) {
            throw new IllegalStateException("SlidingWindowRingBuffer is closed.");
        }
        long offset = calculateOffset(index);
        return UNSAFE.getLongVolatile(null, offset);
    }

    public void set(int index, long value) {
        if (isClosed.get()) {
            throw new IllegalStateException("SlidingWindowRingBuffer is closed.");
        }
        long offset = calculateOffset(index);
        UNSAFE.putLongVolatile(null, offset, value);
    }

    public void clear(int index) {
        set(index, 0L);
    }

    public void reset() {
        if (isClosed.get()) {
            throw new IllegalStateException("SlidingWindowRingBuffer is closed.");
        }
        UNSAFE.setMemory(baseAddress, totalSizeBytes, (byte) 0);
    }

    public long sum() {
        if (isClosed.get()) {
            throw new IllegalStateException("SlidingWindowRingBuffer is closed.");
        }
        long total = 0;
        for (int i = 0; i < capacity; i++) {
            total += get(i);
        }
        return total;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            cleanable.clean();
        }
    }
}