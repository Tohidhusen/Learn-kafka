package com.homework.learnKafka.order_service.contoller;

import com.homework.learnkafka.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaMessageListener {

    @KafkaListener(topics = "order-status-update", groupId = "order-service-group")
    public void listen(OrderCreatedEvent message) {
        log.info("Received message from Kafka: {}", message);
    }
}
