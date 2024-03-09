package com.example.kafkaproducerexample.controller;

import com.example.kafkaproducerexample.dto.Customer;
import com.example.kafkaproducerexample.service.KafkaMessagePublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    private final KafkaMessagePublisher publisher;

    public EventController(KafkaMessagePublisher publisher) {
        this.publisher = publisher;
    }

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            for (int i = 0; i <= 100; i++) {
                publisher.sendMessageToTopic(message + " : " + i);
            }
            return ResponseEntity.ok("Message published successfully.");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }

    @PostMapping("/publish")
    public ResponseEntity<?> sendEvents(@RequestBody Customer customer) {
        try {
            publisher.sendEventsToTopic(customer);
            return ResponseEntity.ok("Event published successfully.");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}
