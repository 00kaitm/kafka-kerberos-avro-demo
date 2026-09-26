package com.practice.springbootpractice.kafka;

/**
 * A record that decoded fine but breaks a business rule. Processing it again can never succeed,
 * so KafkaErrorHandlingConfig marks this not-retryable: it goes to the dead-letter topic on the
 * first failure instead of burning through the retries.
 */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }
}
