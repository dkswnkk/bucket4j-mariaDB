package com.example.bucket4j.component;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.BucketProxy;
import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.jdbc.SQLProxyConfiguration;
import io.github.bucket4j.mysql.MySQLSelectForUpdateBasedProxyManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
public class APIRateLimiter {
    private static final int CAPACITY = 3;
    private static final int REFILL_AMOUNT = 3;
    private static final Duration REFILL_DURATION = Duration.ofSeconds(5);

    private final MySQLSelectForUpdateBasedProxyManager<Long> proxyManager;
    private final ConcurrentMap<String, BucketProxy> buckets = new ConcurrentHashMap<>();

    public APIRateLimiter(DataSource dataSource) {
        var tableSettings = BucketTableSettings.getDefault();
        var sqlProxyConfiguration = SQLProxyConfiguration.builder()
                .withTableSettings(tableSettings)
                .build(dataSource);
        proxyManager = new MySQLSelectForUpdateBasedProxyManager<>(sqlProxyConfiguration);
    }

    private BucketProxy getOrCreateBucket(String apiKey) {
        return buckets.computeIfAbsent(apiKey, key -> {
            Long bucketId = (long) key.hashCode();
            var bucketConfiguration = createBucketConfiguration();
            return proxyManager.builder().build(bucketId, bucketConfiguration);
        });
    }

    private BucketConfiguration createBucketConfiguration() {
        return BucketConfiguration.builder()
                .addLimit(Bandwidth.builder().capacity(CAPACITY).refillIntervally(REFILL_AMOUNT, REFILL_DURATION).build())
                .build();
    }

    public boolean tryConsume(String apiKey) {
        BucketProxy bucket = getOrCreateBucket(apiKey);
        boolean consumed = bucket.tryConsume(1);
        log.info("API Key: {}, Consumed: {}, Time: {}", apiKey, consumed, LocalDateTime.now());
        return consumed;
    }
}
