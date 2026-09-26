package com.practice.springbootpractice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Makes DummyConsumer fail on purpose when a message's text starts with one of the configured
 * markers (app.kafka.failure-demo.*), so each error-handling path can be triggered with a plain
 * POST. A no-op when app.kafka.failure-demo.enabled is false.
 */
@Component
public class FailureDemo {

    private static final Logger log = LoggerFactory.getLogger(FailureDemo.class);

    private final boolean enabled;
    private final String transientMarker;
    private final int transientFailures;
    private final String alwaysMarker;
    private final String invalidMarker;

    public FailureDemo(@Value("${app.kafka.failure-demo.enabled}") boolean enabled,
                       @Value("${app.kafka.failure-demo.transient-marker}") String transientMarker,
                       @Value("${app.kafka.failure-demo.transient-failures}") int transientFailures,
                       @Value("${app.kafka.failure-demo.always-marker}") String alwaysMarker,
                       @Value("${app.kafka.failure-demo.invalid-marker}") String invalidMarker) {
        this.enabled = enabled;
        this.transientMarker = transientMarker;
        this.transientFailures = transientFailures;
        this.alwaysMarker = alwaysMarker;
        this.invalidMarker = invalidMarker;
    }

    /**
     * @param deliveryAttempt 1 on the first delivery of a record, 2 on its first retry, and so on
     *                        (the kafka_deliveryAttempt header).
     */
    public void apply(String text, int deliveryAttempt) {
        if (!enabled) {
            return;
        }
        if (text.startsWith(invalidMarker)) {
            throw new InvalidMessageException("Simulated validation failure for \"" + text + "\"");
        }
        if (text.startsWith(alwaysMarker)) {
            log.warn("Simulating a failure that never recovers: \"{}\", attempt {}", text, deliveryAttempt);
            throw new RetryableProcessingException(
                    "Simulated permanent outage for \"" + text + "\" (attempt " + deliveryAttempt + ")");
        }
        if (text.startsWith(transientMarker) && deliveryAttempt <= transientFailures) {
            log.warn("Simulating a transient failure: \"{}\", attempt {} of {} that fail",
                    text, deliveryAttempt, transientFailures);
            throw new RetryableProcessingException(
                    "Simulated transient failure for \"" + text + "\" (attempt " + deliveryAttempt + ")");
        }
    }
}
