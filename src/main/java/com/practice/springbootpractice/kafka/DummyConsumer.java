package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DummyConsumer {

    private static final Logger log = LoggerFactory.getLogger(DummyConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(DummyMessage message) {
        log.info("Received message: id={} text={} createdAt={}",
                message.getId(), message.getText(), message.getCreatedAt());
    }
}
