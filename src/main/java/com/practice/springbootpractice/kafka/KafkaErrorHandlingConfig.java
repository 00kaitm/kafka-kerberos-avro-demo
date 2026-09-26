package com.practice.springbootpractice.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocking retries with capped exponential back off, then the dead-letter topic.
 *
 * Spring Boot picks up a single CommonErrorHandler bean and a ContainerCustomizer bean and applies
 * them to its auto-configured listener container factory, so every @KafkaListener using that
 * factory gets this behaviour without further wiring. See README "Retries and the dead-letter topic".
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> avroTemplate,
                                          RawBytesProducer rawBytesProducer,
                                          @Value("${app.kafka.dlt-topic}") String dltTopic,
                                          @Value("${app.kafka.retry.initial-interval-ms}") long initialIntervalMs,
                                          @Value("${app.kafka.retry.multiplier}") double multiplier,
                                          @Value("${app.kafka.retry.max-interval-ms}") long maxIntervalMs,
                                          @Value("${app.kafka.retry.max-retries}") int maxRetries) {
        // The recoverer picks the first template whose type matches the value it's publishing.
        // Records that failed deserialization arrive with their original raw bytes restored (from
        // the ErrorHandlingDeserializer header), so they need a byte[] serializer; records that
        // failed in the listener carry the deserialized DummyMessage, which needs the Avro one.
        // byte[] goes first because the recoverer also uses the first template for null values.
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, rawBytesProducer.template());
        templates.put(Object.class, avroTemplate);

        // Same partition number as the original record (the default resolver's behaviour), but a
        // topic name from config instead of the default "<topic>-dlt" convention.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // Retry everything except what can't succeed on a second try. DeserializationException
        // (and a few conversion exceptions) are already in Spring's built-in not-retryable list.
        errorHandler.addNotRetryableExceptions(InvalidMessageException.class);
        return errorHandler;
    }

    /** Adds the kafka_deliveryAttempt header (1, 2, 3...) that DummyConsumer reads. Off by default. */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptHeader() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }
}
