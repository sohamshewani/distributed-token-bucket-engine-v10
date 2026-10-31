package com.enterprise.ratelimit.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

public final class NativeArenaAllocator implements AutoCloseable {

    private static final Unsafe UNSAFE;
    private static final int CACHE_LINE_SIZE = 64;

    static {
        try {
            PrivilegedExceptionAction<Unsafe> action = () -> {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (Unsafe) f.get(null);
            };
            UNSAFE = AccessController.doPrivileged(action);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe for NativeArenaAllocator", e);
        }
    }

    private final long totalCapacityBytes;
    private final long baseAddress;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final StampedLock lifecycleLock = new StampedLock();

    public NativeArenaAllocator(long capacityBytes) {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than zero.");
        }
        // Align capacity to cache line boundary
        long remainder = capacityBytes % CACHE_LINE_SIZE;
        this.totalCapacityBytes = remainder == 0 ? capacityBytes : capacityBytes + (CACHE_LINE_SIZE - remainder);
        
        // Allocate with extra padding for alignment
        long rawAllocationSize = this.totalCapacityBytes + CACHE_LINE_SIZE;
        long rawAddress = UNSAFE.allocateMemory(rawAllocationSize);
        
        if (rawAddress == 0L) {
            throw new OutOfMemoryError("Failed to allocate " + rawAllocationSize + " bytes of off-heap memory.");
        }
        
        // Align address to 64-byte cache line
        if ((rawAddress & (CACHE_LINE_SIZE - 1)) == 0) {
            this.baseAddress = rawAddress;
        } else {
            this.baseAddress = (rawAddress + CACHE_LINE_SIZE) & ~(CACHE_LINE_SIZE - 1);
        }
        
        // Zero out memory to ensure clean state
        UNSAFE.setMemory(this.baseAddress, this.totalCapacityBytes, (byte) 0);
    }

    public long allocate(long sizeBytes) {
        if (sizeBytes <= 0) {
            throw new IllegalArgumentException("Allocation size must be positive.");
        }
        // Align requested size to cache line boundary to prevent false sharing
        long remainder = sizeBytes % CACHE_LINE_SIZE;
        long alignedSize = remainder == 0 ? sizeBytes : sizeBytes + (CACHE_LINE_SIZE - remainder);

        long stamp = lifecycleLock.readLock();
        try {
            ensureOpen();
            // Simple linear allocation pointer tracker inside the arena can be maintained per arena instance,
            // but for generic raw allocation blocks we return base offsets or handle sub-allocations.
            // Here we expose the raw block managed by this arena instance.
            if (alignedSize > totalCapacityBytes) {
                throw new OutOfMemoryError("Allocation size " + alignedSize + " exceeds arena capacity " + totalCapacityBytes);
            }
            return baseAddress;
        } finally {
            lifecycleLock.unlockRead(stamp);
        }
    }

    public void clear() {
        long stamp = lifecycleLock.writeLock();
        try {
            ensureOpen();
            UNSAFE.setMemory(baseAddress, totalCapacityBytes, (byte) 0);
        } finally {
            lifecycleLock.unlockWrite(stamp);
        }
    }

    public long getBaseAddress() {
        ensureOpen();
        return baseAddress;
    }

    public long getTotalCapacityBytes() {
        return totalCapacityBytes;
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("NativeArenaAllocator has already been closed and memory released.");
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            long stamp = lifecycleLock.writeLock();
            try {
                if (baseAddress != 0L) {
                    UNSAFE.freeMemory(baseAddress);
                }
            } finally {
                lifecycleLock.unlockWrite(stamp);
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
}