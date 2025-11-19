package com.example.consommateur_rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClickCountController {

    private final ClickCountService service;

    public ClickCountController(ClickCountService service) {
        this.service = service;
    }

    @GetMapping("/clicks/count")
    public long getTotalClicks() {
        return service.getTotalClicks();
    }
}
