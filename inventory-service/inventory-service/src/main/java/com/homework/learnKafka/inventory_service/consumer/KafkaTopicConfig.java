package com.homework.learnKafka.inventory_service.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopicConfig {

    @Bean
    public NewTopic newTopic() {
        return new NewTopic("order-status-update", 1, (short) 1);
    }
}
