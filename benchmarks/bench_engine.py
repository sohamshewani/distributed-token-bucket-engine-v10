import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DistributedTokenBucketMicrobenchmark {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    
    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASUREMENT_ITERATIONS = 200_000;
    private static final int CONCURRENCY = 8;
    
    private static final long CAPACITY = 100_000_000L;
    private static final long REFILL_RATE = 100_000_000L; // tokens per second

    // Lua script implementing a distributed token bucket
    // KEYS[1]: bucket key
    // ARGV[1]: capacity, ARGV[2]: refill_rate, ARGV[3]: requested_tokens, ARGV[4]: current_timestamp
    private static final String TOKEN_BUCKET_SCRIPT = 
        "local key = KEYS[1]\n" +
        "local capacity = tonumber(ARGV[1])\n" +
        "local refill_rate = tonumber(ARGV[2])\n" +
        "local requested = tonumber(ARGV[3])\n" +
        "local now = tonumber(ARGV[4])\n" +
        "\n" +
        "local data = redis.call('hmget', key, 'tokens', 'last_updated')\n" +
        "local tokens = tonumber(data[1])\n" +
        "local last_updated = tonumber(data[2])\n" +
        "\n" +
        "if tokens == nil then\n" +
        "    tokens = capacity\n" +
        "    last_updated = now\n" +
        "else\n" +
        "    local delta = math.max(0, now - last_updated)\n" +
        "    local generated = delta * refill_rate\n" +
        "    tokens = math.min(capacity, tokens + generated)\n" +
        "    last_updated = now\n" +
        "end\n" +
        "\n" +
        "if tokens < requested then\n" +
        "    redis.call('hmset', key, 'tokens', tokens, 'last_updated', last_updated)\n" +
        "    return 0\n" +
        "end\n" +
        "\n" +
        "tokens = tokens - requested\n" +
        "redis.call('hmset', key, 'tokens', tokens, 'last_updated', last_updated)\n" +
        "return 1\n";

    public static void main(String[] args) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(CONCURRENCY * 2);
        poolConfig.setMaxIdle(CONCURRENCY);
        poolConfig.setMinIdle(2);

        try (JedisPool jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
             Jedis jedis = jedisPool.getResource()) {
            
            // Verify connection
            jedis.ping();
            
            String sha = jedis.scriptLoad(TOKEN_BUCKET_SCRIPT);
            String bucketKey = "benchmark:token_bucket:" + UUID.randomUUID();

            // Cleanup
            jedis.del(bucketKey);

            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);

            System.out.println("Starting Warmup (" + WARMUP_ITERATIONS + " ops)...");
            runBenchmarkPhase(jedisPool, sha, bucketKey, WARMUP_ITERATIONS, CONCURRENCY, false);

            System.out.println("Starting Measurement (" + MEASUREMENT_ITERATIONS + " ops)...");
            BenchmarkResult result = runBenchmarkPhase(jedisPool, sha, bucketKey, MEASUREMENT_ITERATIONS, CONCURRENCY, true);

            executor.shutdown();
            jedis.del(bucketKey);

            printResults(result);

        } catch (Exception e) {
            System.err.println("Benchmark failed to run. Ensure Redis is running on " + REDIS_HOST + ":" + REDIS_PORT);
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static BenchmarkResult runBenchmarkPhase(JedisPool pool, String sha, String bucketKey, 
                                                     int totalIterations, int concurrency, boolean recordLatencies) throws Exception {
        int opsPerThread = totalIterations / concurrency;
        CountDownLatch latch = new CountDownLatch(concurrency);
        AtomicLong successCount = new AtomicLong(0);

        List<Long> latencyList = recordLatencies ? Collections.synchronizedList(new ArrayList<>(totalIterations)) : null;

        long startTime = System.nanoTime();

        for (int i = 0; i < concurrency; i++) {
            executorSubmit(pool, sha, bucketKey, opsPerThread, latch, successCount, latencyList, recordLatencies);
        }

        latch.await();
        long endTime = System.nanoTime();
        long totalDurationNanos = endTime - startTime;

        if (recordLatencies) {
            Collections.sort(latencyList);
        }

        return new BenchmarkResult(totalIterations, totalDurationNanos, successCount.get(), latencyList);
    }

    private static void executorSubmit(JedisPool pool, String sha, String bucketKey, int ops, 
                                       CountDownLatch latch, AtomicLong successCount, 
                                       List<Long> latencyList, boolean recordLatencies) {
        new Thread(() -> {
            try (Jedis jedis = pool.getResource()) {
                for (int i = 0; i < ops; i++) {
                    double now = System.currentTimeMillis() / 1000.0;
                    long opStart = System.nanoTime();
                    
                    Object response = jedis.evalsha(sha, 1, bucketKey, 
                            String.valueOf(CAPACITY), 
                            String.valueOf(REFILL_RATE), 
                            "1", 
                            String.valueOf(now));
                    
                    long opEnd = System.nanoTime();

                    if (response != null && ((Long) response) == 1L) {
                        successCount.incrementAndGet();
                    }

                    if (recordLatencies) {
                        latencyList.add(opEnd - opStart);
                    }
                }
            } finally {
                latch.countDown();
            }
        }).start();
    }

    private static void printResults(BenchmarkResult result) {
        double durationSeconds = result.durationNanos / 1_000_000_000.0;
        double opsPerSec = result.totalOps / durationSeconds;

        long p50 = 0;
        long p99 = 0;
        long p999 = 0;

        if (result.latencies != null && !result.latencies.isEmpty()) {
            p50 = result.latencies.get((int) (result.latencies.size() * 0.50));
            p99 = result.latencies.get((int) (result.latencies.size() * 0.99));
            p999 = result.latencies.get((int) (result.latencies.size() * 0.999));
        }

        System.out.println("\n--------------------------------------------------------------------------------");
        System.out.println("                   DISTRIBUTED TOKEN BUCKET BENCHMARK RESULTS                   ");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-25s | %-15s | %-15s\n", "Metric", "Value", "Unit");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-25s | %-15d | %-15s\n", "Concurrency", CONCURRENCY, "threads");
        System.out.printf("%-25s | %-15d | %-15s\n", "Total Operations", result.totalOps, "ops");
        System.out.printf("%-25s | %-15d | %-15s\n", "Successful Operations", result.successOps, "ops");
        System.out.printf("%-25s | %-15.3s | %-15s\n", "Total Duration", durationSeconds, "seconds");
        System.out.printf("%-25s | %-15.2f | %-15s\n", "Throughput", opsPerSec, "ops/sec");
        System.out.printf("%-25s | %-15.2f | %-15s\n", "Latency p50", p50 / 1_000_000.0, "ms");
        System.out.printf("%-25s | %-15.2f | %-15s\n", "Latency p99", p99 / 1_000_000.0, "ms");
        System.out.printf("%-25s | %-15.2f | %-15s\n", "Latency p99.9", p999 / 1_000_000.0, "ms");
        System.out.println("--------------------------------------------------------------------------------\n");
    }

    private static class BenchmarkResult {
        final int totalOps;
        final long durationNanos;
        final long successOps;
        final List<Long> latencies;

        private BenchmarkResult(int totalOps, long durationNanos, long successOps, List<Long> latencies) {
            this.totalOps = totalOps;
            this.durationNanos = durationNanos;
            this.successOps = successOps;
            this.latencies = latencies;
        }
    }
}