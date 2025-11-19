package com.example.consommateur_rest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClickCountService {

    private final ConcurrentHashMap<String, Long> userClickCounts = new ConcurrentHashMap<>();

    @KafkaListener(topics = "click-counts", groupId = "click-consumer")
    public void listen(ConsumerRecord<String, Long> record) {
        userClickCounts.put(record.key(), record.value());
    }

    public long getTotalClicks() {
        return userClickCounts.values().stream().mapToLong(Long::longValue).sum();
    }
}
