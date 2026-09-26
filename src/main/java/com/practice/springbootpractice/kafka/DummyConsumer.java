package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DummyConsumer {

    private static final Logger log = LoggerFactory.getLogger(DummyConsumer.class);

    private final FailureDemo failureDemo;

    public DummyConsumer(FailureDemo failureDemo) {
        this.failureDemo = failureDemo;
    }

    /**
     * Exceptions thrown here go to the DefaultErrorHandler in KafkaErrorHandlingConfig, which
     * retries or dead-letters the record depending on the exception type.
     */
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(DummyMessage message, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int deliveryAttempt) {
        failureDemo.apply(message.getText().toString(), deliveryAttempt);
        log.info("Received message: id={} text={} createdAt={} (delivery attempt {})",
                message.getId(), message.getText(), message.getCreatedAt(), deliveryAttempt);
    }
}
