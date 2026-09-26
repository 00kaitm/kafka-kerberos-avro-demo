package com.practice.springbootpractice.kafka;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A producer for values that are already bytes: raw payloads that failed deserialization (so the
 * dead-letter topic gets the exact original bytes) and the poison-pill demo endpoint.
 *
 * Deliberately a wrapper, not a KafkaTemplate bean: Spring Boot only auto-configures its own
 * (Avro) KafkaTemplate when no other KafkaTemplate bean exists, so declaring a second one would
 * silently remove the one DummyProducer uses. This copies Boot's producer config - same brokers,
 * Kerberos and TLS, same acks/idempotence - overriding only the value serializer.
 */
@Component
public class RawBytesProducer {

    private final KafkaTemplate<String, byte[]> template;

    @SuppressWarnings("unchecked")
    public RawBytesProducer(ProducerFactory<?, ?> producerFactory) {
        this.template = new KafkaTemplate<>((ProducerFactory<String, byte[]>) producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    public KafkaTemplate<String, byte[]> template() {
        return template;
    }

    public RecordMetadata sendAndWait(String topic, byte[] value) throws ExecutionException, InterruptedException {
        return template.send(new ProducerRecord<>(topic, value)).get().getRecordMetadata();
    }

    @PreDestroy
    void close() {
        // Closes the producer this template's private copy of the factory created.
        template.destroy();
    }
}
