package com.practice.springbootpractice.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Logs everything that lands on the dead-letter topic: where the record came from, why it failed,
 * and what its value was. Reads values as raw bytes, because the DLT holds both Avro records
 * (processing failures) and arbitrary bytes (records that were never valid Avro).
 *
 * Must never throw: it shares the error handler that publishes to the DLT, so a failure here
 * would send the DLT record back onto the DLT.
 */
@Component
public class DeadLetterInspector {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterInspector.class);
    private static final int MAX_PREVIEW_BYTES = 64;
    private static final Pattern LISTENER_WRAPPER_PREFIX = Pattern.compile("^Listener method '.*?' threw exception; ");

    private final KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();

    public DeadLetterInspector(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        avroDeserializer.configure(Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", true), false);
    }

    @KafkaListener(topics = "${app.kafka.dlt-topic}", groupId = "${app.kafka.dlt-group-id}",
            properties = "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer")
    public void inspect(ConsumerRecord<String, byte[]> record) {
        try {
            Headers headers = record.headers();
            log.warn("Dead letter at {}-{}@{}: from {}-{}@{} (group {}), failed with {}: {} | value: {}",
                    record.topic(), record.partition(), record.offset(),
                    string(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                    intValue(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                    longValue(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                    string(headers, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                    rootExceptionClass(headers),
                    exceptionMessage(headers),
                    describeValue(record.topic(), record.value()));
        }
        catch (RuntimeException ex) {
            log.error("Could not describe dead letter at {}-{}@{}", record.topic(), record.partition(), record.offset(), ex);
        }
    }

    /** The cause is the interesting one; the outer exception is usually Spring's ListenerExecutionFailedException. */
    private static String rootExceptionClass(Headers headers) {
        String cause = string(headers, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return cause != null ? cause : string(headers, KafkaHeaders.DLT_EXCEPTION_FQCN);
    }

    /** Drops the "Listener method '...' threw exception; " prefix Spring's wrapper exception adds. */
    private static String exceptionMessage(Headers headers) {
        String message = string(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        return message == null ? null : LISTENER_WRAPPER_PREFIX.matcher(message).replaceFirst("");
    }

    private String describeValue(String topic, byte[] value) {
        if (value == null) {
            return "null";
        }
        try {
            return "Avro " + avroDeserializer.deserialize(topic, value);
        }
        catch (RuntimeException notAvro) {
            byte[] preview = value.length > MAX_PREVIEW_BYTES
                    ? Arrays.copyOf(value, MAX_PREVIEW_BYTES) : value;
            return value.length + " raw bytes, not decodable as Avro: hex " + HexFormat.of().formatHex(preview)
                    + " / text \"" + new String(preview, StandardCharsets.UTF_8) + "\"";
        }
    }

    private static String string(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer intValue(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longValue(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : ByteBuffer.wrap(header.value()).getLong();
    }
}
