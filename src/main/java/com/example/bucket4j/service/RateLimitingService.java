package com.example.bucket4j.service;

import com.example.bucket4j.annotation.RateLimit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RateLimitingService {
    @RateLimit(key = "someUniqueKey1")
    public String run1() {
        return "요청 성공";
    }

    @RateLimit(key = "someUniqueKey2")
    public String run2() {
        return "요청 성공";
    }
}