package com.example.bucket4j.aop;

import com.example.bucket4j.annotation.RateLimit;
import com.example.bucket4j.component.APIRateLimiter;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class RateLimitingAspect {

    private final APIRateLimiter apiRateLimiter;

    @Autowired
    public RateLimitingAspect(APIRateLimiter apiRateLimiter) {
        this.apiRateLimiter = apiRateLimiter;
    }

    @Around("@annotation(rateLimit)")
    public Object rateLimit(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        if (apiRateLimiter.tryConsume(rateLimit.key())) {
            return joinPoint.proceed();
        } else {
            throw new RuntimeException("Rate limit exceeded for key: " + rateLimit.key());
        }
    }
}
