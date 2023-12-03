package com.example.bucket4j.service;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.BucketProxy;
import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.jdbc.SQLProxyConfiguration;
import io.github.bucket4j.mysql.MySQLSelectForUpdateBasedProxyManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@Slf4j
public class RateLimitingService {

    // 버킷의 최대 용량을 정의하는 상수
    private static final int CAPACITY = 3;
    // 버킷이 정해진 시간 동안 리필할 수 있는 최대 토큰 수를 정의하는 상수
    private static final int REFILL_AMOUNT = 3;
    // 리필 주기를 정의하는 상수 (여기서는 5초)
    private static final Duration REFILL_DURATION = Duration.ofSeconds(5);

    // Bucket4j 라이브러리를 사용한 MySQL 기반의 프록시 매니저
    private final MySQLSelectForUpdateBasedProxyManager<Long> proxyManager;
    // API 키별로 BucketProxy 객체를 저장하기 위한 ConcurrentHashMap
    private final ConcurrentMap<String, BucketProxy> buckets = new ConcurrentHashMap<>();

    public RateLimitingService(DataSource dataSource) {
        // Bucket4j의 기본 테이블 설정을 가져옴
        var tableSettings = BucketTableSettings.getDefault();
        // SQL 프록시 구성을 위한 빌더를 사용하여 구성 객체 생성
        var sqlProxyConfiguration = SQLProxyConfiguration.builder()
                .withTableSettings(tableSettings)
                .build(dataSource);
        // 구성 객체를 사용하여 MySQLSelectForUpdateBasedProxyManager 초기화
        proxyManager = new MySQLSelectForUpdateBasedProxyManager<>(sqlProxyConfiguration);
    }

    private BucketProxy getOrCreateBucket(String apiKey) {
        // ConcurrentHashMap의 computeIfAbsent 메서드를 사용하여 버킷 생성 또는 검색
        return buckets.computeIfAbsent(apiKey, key -> {
            // API 키의 해시코드를 버킷 ID로 사용
            Long bucketId = (long) key.hashCode();
            // 버킷 구성을 위한 메서드 호출
            var bucketConfiguration = createBucketConfiguration();
            // 프록시 매니저를 사용하여 버킷 생성
            return proxyManager.builder().build(bucketId, bucketConfiguration);
        });
    }

    private BucketConfiguration createBucketConfiguration() {
        // Bandwidth를 설정하여 버킷 구성 정의
        return BucketConfiguration.builder()
                .addLimit(Bandwidth.builder().capacity(CAPACITY).refillIntervally(REFILL_AMOUNT, REFILL_DURATION).build())
                .build();
    }

    public boolean consume(String apiKey) {
        BucketProxy bucket = getOrCreateBucket(apiKey);
        // 버킷에서 토큰 1개를 소비 시도
        boolean consumed = bucket.tryConsume(1);
        log.info("API Key: {}, Consumed: {}, time{}", apiKey, consumed, LocalDateTime.now());
        return consumed;
    }

    public String run() {
        if (consume("test")) {
            return "요청 성공";
        } else {
            return "요청 한도 초과";
        }
    }
}