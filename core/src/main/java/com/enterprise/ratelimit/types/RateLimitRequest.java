package com.enterprise.ratelimit.types;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents an immutable, high-throughput rate-limiting evaluation request payload 
 * designed for zero-allocation processing within the distributed-token-bucket-engine.
 * 
 * <p>This record-like class encapsulates client identity, cost weights, and hybrid 
 * logical clock metadata required by off-heap sliding-window ring buffers and 
 * lock-free bucket shards.
 */
public final class RateLimitRequest {

    private final long tenantId;
    private final long clientId;
    private final long requestedTokens;
    private final long hlcTimestamp;
    private final int clusterNodeId;
    private final byte priorityClass;

    public RateLimitRequest(
            final long tenantId,
            final long clientId,
            final long requestedTokens,
            final long hlcTimestamp,
            final int clusterNodeId,
            final byte priorityClass) {
        
        if (requestedTokens <= 0L) {
            throw new IllegalArgumentException("Requested tokens must be strictly positive: " + requestedTokens);
        }
        if (hlcTimestamp < 0L) {
            throw new IllegalArgumentException("HLC timestamp cannot be negative: " + hlcTimestamp);
        }
        if (clusterNodeId < 0) {
            throw new IllegalArgumentException("Cluster node ID cannot be negative: " + clusterNodeId);
        }

        this.tenantId = tenantId;
        this.clientId = clientId;
        this.requestedTokens = requestedTokens;
        this.hlcTimestamp = hlcTimestamp;
        this.clusterNodeId = clusterNodeId;
        this.priorityClass = priorityClass;
    }

    /**
     * Returns the globally unique tenant identifier associated with this request quota rule.
     *
     * @return tenant ID
     */
    public long getTenantId() {
        return tenantId;
    }

    /**
     * Returns the specific client, API key, or route identifier subject to rate limiting.
     *
     * @return client ID
     */
    public long getClientId() {
        return clientId;
    }

    /**
     * Returns the number of tokens requested to consume, scaled by the engine's fixed-point factor (10^6).
     *
     * @return requested token count with precision scaling
     */
    public long getRequestedTokens() {
        return requestedTokens;
    }

    /**
     * Returns the Hybrid Logical Clock (HLC) timestamp synchronized across the cluster nodes 
     * for network clock-skew compensation.
     *
     * @return HLC timestamp in nanoseconds/milliseconds composite layout
     */
    public long getHlcTimestamp() {
        return hlcTimestamp;
    }

    /**
     * Returns the originating cluster node identifier that received the edge request.
     *
     * @return cluster node ID
     */
    public int getClusterNodeId() {
        return clusterNodeId;
    }

    /**
     * Returns the traffic priority classification used during local fallback and shedding 
     * when network partitions occur.
     *
     * @return priority class byte (e.g., 0 = critical, 1 = standard, 2 = bulk)
     */
    public byte getPriorityClass() {
        return priorityClass;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RateLimitRequest that = (RateLimitRequest) o;
        return tenantId == that.tenantId &&
                clientId == that.clientId &&
                requestedTokens == that.requestedTokens &&
                hlcTimestamp == that.hlcTimestamp &&
                clusterNodeId == that.clusterNodeId &&
                priorityClass == that.priorityClass;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(tenantId);
        result = 31 * result + Long.hashCode(clientId);
        result = 31 * result + Long.hashCode(requestedTokens);
        result = 31 * result + Long.hashCode(hlcTimestamp);
        result = 31 * result + Integer.hashCode(clusterNodeId);
        result = 31 * result + Byte.hashCode(priorityClass);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RateLimitRequest.class.getSimpleName + "[", "]")
                .add("tenantId=" + tenantId)
                .add("clientId=" + clientId)
                .add("requestedTokens=" + requestedTokens)
                .add("hlcTimestamp=" + hlcTimestamp)
                .add("clusterNodeId=" + clusterNodeId)
                .add("priorityClass=" + priorityClass)
                .toString();
    }
}