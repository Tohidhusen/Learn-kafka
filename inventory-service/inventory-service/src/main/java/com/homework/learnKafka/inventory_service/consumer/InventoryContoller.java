package com.homework.learnKafka.inventory_service.consumer;

import com.homework.learnkafka.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/inventory")
public class InventoryContoller {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @PostMapping("/updateInventory")
    public void updateInventory(@RequestBody OrderCreatedEvent event) {
        // Logic to update inventory based on the order event
        // For example, you can decrease the stock quantity based on the order details
        // After updating the inventory, you can send a message to another Kafka topic if needed
        kafkaTemplate.send("order-status-update", event);
    }
}
