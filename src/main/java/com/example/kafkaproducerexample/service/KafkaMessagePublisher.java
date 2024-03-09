package com.example.kafkaproducerexample.service;

import com.example.kafkaproducerexample.dto.Customer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    private final KafkaTemplate<String, Object> template;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> template) {
        this.template = template;
    }


    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("example-topic2", message);

        future.whenComplete((result,ex)->{
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Customer customer) {
        CompletableFuture<SendResult<String, Object>> future = template.send("example-topic", customer);

        future.whenComplete((result,ex)->{
            if (ex == null) {
                System.out.println("Sent customer=[" + customer.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send customer=[" +
                        customer.toString() + "] due to : " + ex.getMessage());
            }
        });
    }
}
