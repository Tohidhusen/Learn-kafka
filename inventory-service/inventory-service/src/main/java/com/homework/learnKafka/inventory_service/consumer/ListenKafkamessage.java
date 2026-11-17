package com.homework.learnKafka.inventory_service.consumer;

import com.homework.learnkafka.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class ListenKafkamessage {




    @KafkaListener(topics = "order-created-topic",groupId = "inventory-service-group")
    public void listen(OrderCreatedEvent message) {

        System.out.println("Message received from Kafka");

        System.out.println("Order ID: " + message.getOrderId());
        System.out.println("Order Status: " + message.getOrderStatus());
        System.out.println("Order Total: " + message.getOrderTotal());
        System.out.println("Quantity: " + message.getQuantity());
    }


}
