package com.homework.learnKafka.order_service.contoller;

import com.homework.learnkafka.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class OrderContoller {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @PostMapping("/createOrder")
    public ResponseEntity<String> createOrder(@RequestBody  OrderCreatedEvent event) {
        kafkaTemplate.send("order-created-topic", event);
        return ResponseEntity.ok().body("Order created");

    }
}

