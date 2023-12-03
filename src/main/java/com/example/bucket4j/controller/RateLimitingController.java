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

    @GetMapping("/test")
    public String test() {
        return rateLimitingService.run();
    }

}
