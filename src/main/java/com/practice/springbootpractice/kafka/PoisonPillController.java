package com.practice.springbootpractice.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

/**
 * Publishes the request body to dummy-topic as raw UTF-8 bytes, bypassing Avro, so consumers of
 * the topic hit a record they can't deserialize (a "poison pill"). Every reader of dummy-topic
 * sees it, including the Spark jobs. Only exists when app.kafka.failure-demo.enabled is true.
 */
@RestController
@ConditionalOnProperty(name = "app.kafka.failure-demo.enabled", havingValue = "true")
public class PoisonPillController {

    private final RawBytesProducer rawBytesProducer;
    private final String topic;

    public PoisonPillController(RawBytesProducer rawBytesProducer, @Value("${app.kafka.topic}") String topic) {
        this.rawBytesProducer = rawBytesProducer;
        this.topic = topic;
    }

    @PostMapping("/dummy-topic/poison-pill")
    public String send(@RequestBody String body) throws Exception {
        RecordMetadata sent = rawBytesProducer.sendAndWait(topic, body.getBytes(StandardCharsets.UTF_8));
        return "sent raw bytes to " + sent.topic() + " partition " + sent.partition() + " offset " + sent.offset();
    }
}
