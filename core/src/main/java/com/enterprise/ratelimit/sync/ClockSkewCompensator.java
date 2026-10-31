package com.enterprise.ratelimit.sync;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class ClockSkewCompensator {

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

    private static final long MAX_OFFSET_NANOS = 5_000_000_000L; // 5 seconds max bound
    private static final long MAX_BACKWARD_JUMP_NANOS = 100_000_000L; // 100ms tolerance

    private final AtomicLong estimatedSkewNanos;
    private final AtomicLong lastSyncTimestampNanos;
    private final AtomicLong lastMonotonicTimestampNanos;
    private final AtomicLong lastPhysicalTimeNanos;
    private final AtomicLong logicalCounter;

    private volatile boolean partitionMode;

    public ClockSkewCompensator() {
        this.estimatedSkewNanos = new AtomicLong(0L);
        long nowNano = System.nanoTime();
        long nowMillis = System.currentTimeMillis();
        this.lastSyncTimestampNanos = new AtomicLong(nowNano);
        this.lastMonotonicTimestampNanos = new AtomicLong(nowNano);
        this.lastPhysicalTimeNanos = new AtomicLong(nowMillis * 1_000_000L);
        this.logicalCounter = new AtomicLong(0L);
        this.partitionMode = false;
    }

    public void updateNtpSample(long clientSendNano, long serverReceiveMillis, long serverTransmitMillis, long clientReceiveNano) {
        long roundTripTime = (clientReceiveNano - clientSendNano);
        if (roundTripTime < 0) {
            return; // Invalid sample due to measurement anomaly
        }
        long serverTimeAtReceiveNanos = serverReceiveMillis * 1_000_000L;
        long serverTimeAtTransmitNanos = serverTransmitMillis * 1_000_000L;
        long estimatedServerNano = serverTimeAtTransmitNanos + (roundTripTime / 2);
        long rawSkew = estimatedServerNano - clientReceiveNano;

        if (Math.abs(rawSkew) > MAX_OFFSET_NANOS) {
            rawSkew = Math.copySign(MAX_OFFSET_NANOS, rawSkew);
        }

        long currentSkew = estimatedSkewNanos.get();
        // Exponential Moving Average smoothing: alpha = 0.2
        long smoothedSkew = (long) (currentSkew * 0.8 + rawSkew * 0.2);
        estimatedSkewNanos.set(smoothedSkew);
        lastSyncTimestampNanos.set(clientReceiveNano);
    }

    public long getCompensatedTimestampNanos() {
        long monotonicNow = System.nanoTime();
        long lastMono = lastMonotonicTimestampNanos.get();

        if (monotonicNow < lastMono) {
            // Monotonic clock regression check. Enforce monotonicity.
            long diff = lastMono - monotonicNow;
            if (diff > MAX_BACKWARD_JUMP_NANOS) {
                // Severe backward clock jump detected. Trigger local fallback isolation.
                this.partitionMode = true;
            }
            monotonicNow = lastMono;
        } else {
            // CAS update for monotonic tracking
            while (!lastMonotonicTimestampNanos.compareAndSet(lastMono, monotonicNow)) {
                lastMono = lastMonotonicTimestampNanos.get();
                if (monotonicNow < lastMono) {
                    monotonicNow = lastMono;
                    break;
                }
            }
        }

        long skew = estimatedSkewNanos.get();
        return monotonicNow + skew;
    }

    public long nextHybridLogicalClockTimestamp() {
        long physicalNow = getCompensatedTimestampNanos();
        long lastPhys = lastPhysicalTimeNanos.get();
        long logical = logicalCounter.get();

        if (physicalNow > lastPhys) {
            if (lastPhysicalTimeNanos.compareAndSet(lastPhys, physicalNow)) {
                logicalCounter.set(0L);
                return (physicalNow << 16);
            }
        }

        // If physical time has not advanced or CAS failed, increment logical component
        long newLogical = logicalCounter.incrementAndGet();
        if (newLogical >= 0xFFFFL) {
            // Logical counter exhaustion within the same nanosecond/tick boundary; spin-wait briefly
            LockSupport.parkNanos(1);
            return nextHybridLogicalClockTimestamp();
        }

        long basePhys = lastPhysicalTimeNanos.get();
        return (basePhys << 16) | (newLogical & 0xFFFFL);
    }

    public void reconcileRemoteHlc(long remoteHlcTimestamp) {
        long remotePhysical = remoteHlcTimestamp >>> 16;
        long remoteLogical = remoteHlcTimestamp & 0xFFFFL;

        long localPhysical = lastPhysicalTimeNanos.get();
        long localLogical = logicalCounter.get();

        long maxPhysical = Math.max(Math.max(localPhysical, remotePhysical), getCompensatedTimestampNanos());

        if (maxPhysical == localPhysical && maxPhysical == remotePhysical) {
            long maxLogical = Math.max(localLogical, remoteLogical) + 1;
            logicalCounter.set(maxLogical);
        } else if (maxPhysical == remotePhysical) {
            lastPhysicalTimeNanos.set(remotePhysical);
            logicalCounter.set(remoteLogical + 1);
        } else {
            lastPhysicalTimeNanos.set(maxPhysical);
            logicalCounter.incrementAndGet();
        }
    }

    public long calculateSkewDeltaFactor(long expectedIntervalNanos, long actualIntervalNanos) {
        if (expectedIntervalNanos <= 0 || actualIntervalNanos <= 0) {
            return 1_000_000L; // Unit scale factor (1.0 in fixed-point 10^6)
        }
        // Fixed point scaling factor with 10^6 precision
        return (expectedIntervalNanos * 1_000_000L) / actualIntervalNanos;
    }

    public boolean isPartitionMode() {
        return partitionMode;
    }

    public void setPartitionMode(boolean partitionMode) {
        this.partitionMode = partitionMode;
    }

    public long getEstimatedSkewNanos() {
        return estimatedSkewNanos.get();
    }
}