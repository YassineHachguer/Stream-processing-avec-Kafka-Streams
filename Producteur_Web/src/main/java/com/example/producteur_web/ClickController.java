package com.example.producteur_web;

import com.example.producteur_web.KafkaProducerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClickController {

    private final KafkaProducerService producerService;

    public ClickController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/click")
    public String click(String userId) {
        producerService.sendClick(userId);
        return "Clic envoyé pour " + userId;
    }
}
