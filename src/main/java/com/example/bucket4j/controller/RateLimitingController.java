package com.example.bucket4j.controller;

import com.example.bucket4j.service.RateLimitingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class RateLimitingController {
    @Autowired
    private RateLimitingService rateLimitingService;

    @GetMapping("/test/1")
    public String test1() {
        return rateLimitingService.run1();
    }

    @GetMapping("/test/2")
    public String test2() {
        return rateLimitingService.run2();
    }
}
