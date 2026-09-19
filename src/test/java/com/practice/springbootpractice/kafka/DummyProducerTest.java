package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DummyProducerTest {

    private static final String TOPIC = "dummy-topic";

    @Mock
    private KafkaTemplate<String, DummyMessage> kafkaTemplate;

    private DummyProducer producer;

    @BeforeEach
    void setUp() {
        producer = new DummyProducer(kafkaTemplate, TOPIC);
    }

    @Test
    void sendPublishesAMessageWithTheGivenTextToTheConfiguredTopic() {
        stubSuccessfulSend();
        // Avro's timestamp-millis truncates to milliseconds, so compare at that precision.
        Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        producer.send("hello");

        ArgumentCaptor<DummyMessage> sent = ArgumentCaptor.forClass(DummyMessage.class);
        verify(kafkaTemplate).send(eq(TOPIC), sent.capture());
        DummyMessage message = sent.getValue();
        assertThat(message.getText()).isEqualTo("hello");
        assertThat(UUID.fromString(message.getId().toString())).isNotNull();
        assertThat(message.getCreatedAt()).isBetween(before, Instant.now());
    }

    @Test
    void sendGeneratesAFreshIdForEveryMessage() {
        stubSuccessfulSend();

        producer.send("first");
        producer.send("second");

        ArgumentCaptor<DummyMessage> sent = ArgumentCaptor.forClass(DummyMessage.class);
        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(eq(TOPIC), sent.capture());
        assertThat(sent.getAllValues().get(0).getId())
                .isNotEqualTo(sent.getAllValues().get(1).getId());
    }

    @Test
    void sendDoesNotThrowWhenTheBrokerReportsAFailure() {
        CompletableFuture<SendResult<String, DummyMessage>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(eq(TOPIC), any(DummyMessage.class))).thenReturn(failed);

        assertThatCode(() -> producer.send("hello")).doesNotThrowAnyException();
    }

    // The mocks are fully built before the outer when(...) starts, so Mockito
    // never sees two stubbings in progress at once.
    private void stubSuccessfulSend() {
        RecordMetadata metadata = mock(RecordMetadata.class);
        @SuppressWarnings("unchecked")
        SendResult<String, DummyMessage> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(metadata);
        CompletableFuture<SendResult<String, DummyMessage>> future =
                CompletableFuture.completedFuture(result);

        when(kafkaTemplate.send(eq(TOPIC), any(DummyMessage.class))).thenReturn(future);
    }
}
