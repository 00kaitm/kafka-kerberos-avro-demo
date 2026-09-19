package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class DummyProducer {

    private static final Logger log = LoggerFactory.getLogger(DummyProducer.class);

    private final KafkaTemplate<String, DummyMessage> kafkaTemplate;
    private final String topic;

    public DummyProducer(KafkaTemplate<String, DummyMessage> kafkaTemplate,
                          @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(String text) {
        DummyMessage message = DummyMessage.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setText(text)
                .setCreatedAt(Instant.now())
                .build();

        kafkaTemplate.send(topic, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message to {}", topic, ex);
                    } else {
                        log.info("Sent message to {} partition {} offset {}",
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
