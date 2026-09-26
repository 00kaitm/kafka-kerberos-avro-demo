package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Each error-handling path from README "Retries and the dead-letter topic", end to end against an
 * in-process broker: retried-then-succeeded, retries exhausted, not retryable, and a poison pill.
 * Back off is shortened to milliseconds so the retries don't slow the suite down.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=true",
        "app.kafka.retry.initial-interval-ms=20",
        "app.kafka.retry.max-interval-ms=40"
})
@ActiveProfiles("test")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"dummy-topic", "dummy-topic-dlt"})
@DirtiesContext
class DeadLetterIntegrationTest {

    private static final String DLT = "dummy-topic-dlt";
    /** 1 delivery + app.kafka.retry.max-retries (4). */
    private static final int ATTEMPTS_BEFORE_DLT = 5;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private DummyProducer producer;

    @Autowired
    private MockMvc mockMvc;

    @MockitoSpyBean
    private DummyConsumer consumer;

    @Test
    void aTransientFailureSucceedsOnRetry() {
        String text = "fail-transient " + UUID.randomUUID();
        producer.send(text);

        // transient-failures is 2, so attempts 1 and 2 throw and attempt 3 succeeds.
        verify(consumer, timeout(15_000)).listen(withText(text), eq(3));
        verify(consumer, after(500).times(3)).listen(withText(text), anyInt());
    }

    @Test
    void aRecordThatAlwaysFailsIsRetriedThenDeadLettered() {
        String text = "fail-always " + UUID.randomUUID();
        producer.send(text);

        ConsumerRecord<String, byte[]> deadLetter = awaitDeadLetter(r -> valueContains(r, text));

        verify(consumer, timeout(5_000).times(ATTEMPTS_BEFORE_DLT)).listen(withText(text), anyInt());
        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(RetryableProcessingException.class.getName());
        assertThat(header(deadLetter, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo("dummy-topic");
        assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
        // Processing failures keep their Avro encoding: Confluent magic byte 0 first.
        assertThat(deadLetter.value()[0]).isEqualTo((byte) 0);
    }

    @Test
    void aNotRetryableFailureGoesStraightToTheDeadLetterTopic() {
        String text = "fail-invalid " + UUID.randomUUID();
        producer.send(text);

        ConsumerRecord<String, byte[]> deadLetter = awaitDeadLetter(r -> valueContains(r, text));

        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(InvalidMessageException.class.getName());
        verify(consumer, after(500).times(1)).listen(withText(text), anyInt());
    }

    @Test
    void aPoisonPillIsDeadLetteredWithItsOriginalBytes() throws Exception {
        String garbage = "not avro " + UUID.randomUUID();
        mockMvc.perform(post("/dummy-topic/poison-pill").contentType(MediaType.TEXT_PLAIN).content(garbage))
                .andExpect(status().isOk());

        ConsumerRecord<String, byte[]> deadLetter = awaitDeadLetter(r -> valueContains(r, garbage));

        assertThat(deadLetter.value()).isEqualTo(garbage.getBytes(StandardCharsets.UTF_8));
        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo(DeserializationException.class.getName());
        assertThat(header(deadLetter, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo("dummy-topic");
        assertThat(ByteBuffer.wrap(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()).getInt())
                .isZero();
    }

    private static DummyMessage withText(String text) {
        return argThat((DummyMessage m) -> m != null && text.equals(m.getText().toString()));
    }

    private static boolean valueContains(ConsumerRecord<String, byte[]> record, String text) {
        return record.value() != null && new String(record.value(), StandardCharsets.UTF_8).contains(text);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Reads the DLT from the beginning with a throwaway group until a matching record shows up. */
    private ConsumerRecord<String, byte[]> awaitDeadLetter(Predicate<ConsumerRecord<String, byte[]>> matches) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(brokers, "dlt-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, byte[]> dltConsumer = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            dltConsumer.subscribe(List.of(DLT));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, byte[]> record : dltConsumer.poll(Duration.ofMillis(500))) {
                    if (matches.test(record)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No matching record on " + DLT + " within 30s");
    }
}
