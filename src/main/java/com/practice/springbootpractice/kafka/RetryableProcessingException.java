package com.practice.springbootpractice.kafka;

/**
 * A failure that might go away if the same record is simply processed again - a downstream
 * service timing out, a lock being held. The error handler retries these with back off before
 * giving up and sending the record to the dead-letter topic.
 */
public class RetryableProcessingException extends RuntimeException {

    public RetryableProcessingException(String message) {
        super(message);
    }
}
