package xme.common.kfkprocessor.requestreply.adapters.allowance;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.RemoteBucketBuilder;
import io.github.bucket4j.redis.lettuce.Bucket4jLettuce;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import java.time.Duration;

/** Factory for a Bucket4j + Lettuce backed BudgetCounter (budget units per sliding second). */
public final class Bucket4jBudgetCounters implements AutoCloseable {

    /** Upper bound for connecting and for every command, so an outage surfaces well inside the 5 s pause target. */
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private final RedisClient client;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final BudgetCounter counter;

    private Bucket4jBudgetCounters(RedisClient client, StatefulRedisConnection<byte[], byte[]> connection,
            BudgetCounter counter) {
        this.client = client;
        this.connection = connection;
        this.counter = counter;
    }

    public static Bucket4jBudgetCounters connect(String redisUri, String key, long budgetPerSecond) {
        RedisURI uri = RedisURI.create(redisUri);
        uri.setTimeout(TIMEOUT);
        RedisClient client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(TIMEOUT).build())
                .timeoutOptions(TimeoutOptions.enabled(TIMEOUT))
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build());
        StatefulRedisConnection<byte[], byte[]> connection = null;
        try {
            connection = client.connect(ByteArrayCodec.INSTANCE);
            LettuceBasedProxyManager<byte[]> manager = Bucket4jLettuce.casBasedBuilder(connection)
                    .expirationAfterWrite(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(
                            Duration.ofSeconds(10)))
                    .build();
            BucketConfiguration configuration = BucketConfiguration.builder()
                    .addLimit(Bandwidth.builder()
                            .capacity(burstCapacity(budgetPerSecond))
                            .refillGreedy(budgetPerSecond, Duration.ofSeconds(1))
                            .build())
                    .build();
            RemoteBucketBuilder<byte[]> builder = manager.builder();
            var bucket = builder.build(key.getBytes(UTF_8), () -> configuration);
            BudgetCounter counter = new BudgetCounter() {
                @Override
                public long takeUpTo(long max) {
                    return bucket.tryConsumeAsMuchAsPossible(max);
                }

                @Override
                public void put(long units) {
                    bucket.addTokens(units);
                }
            };
            return new Bucket4jBudgetCounters(client, connection, counter);
        } catch (RuntimeException e) {
            if (connection != null) {
                connection.close();
            }
            client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
            throw e;
        }
    }

    /**
     * Burst capacity: any sliding second admits at most capacity + budget units, so the capacity is capped at 5 %
     * of the budget (QG-1: at most budget x 1.05). Applies to a fresh, state-lost or outage-idle (full) bucket alike.
     * Budgets under 20 cannot express 5 % in whole units and keep the minimum capacity of 1.
     */
    static long burstCapacity(long budgetPerSecond) {
        return Math.max(1, budgetPerSecond / 20);
    }

    public BudgetCounter counter() {
        return counter;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } finally {
            client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
        }
    }
}
