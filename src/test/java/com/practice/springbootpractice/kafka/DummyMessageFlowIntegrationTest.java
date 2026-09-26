package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Runs the real producer, Avro serialization and consumer against an in-process
 * Kafka broker. Security is plaintext and the schema registry is a {@code mock://}
 * one, so no Docker, Kerberos or TLS is involved.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // Overrides application-test.yml, which disables listeners for the plain context test.
        "spring.kafka.listener.auto-startup=true"
})
@ActiveProfiles("test")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"dummy-topic", "dummy-topic-dlt"})
@DirtiesContext
class DummyMessageFlowIntegrationTest {

    @Autowired
    private DummyProducer producer;

    @Autowired
    private MockMvc mockMvc;

    @MockitoSpyBean
    private DummyConsumer consumer;

    @Test
    void aMessagePostedToTheEndpointIsReceivedByTheConsumer() throws Exception {
        mockMvc.perform(post("/dummy-topic/messages")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("posted over http"))
                .andExpect(status().isOk())
                .andExpect(content().string("queued"));

        ArgumentCaptor<DummyMessage> received = ArgumentCaptor.forClass(DummyMessage.class);
        verify(consumer, timeout(15_000)).listen(received.capture(), anyInt());
        assertThat(received.getValue().getText().toString()).isEqualTo("posted over http");
    }

    @Test
    void aMessageSentByTheProducerIsReceivedByTheConsumer() {
        producer.send("through the embedded broker");

        ArgumentCaptor<DummyMessage> received = ArgumentCaptor.forClass(DummyMessage.class);
        verify(consumer, timeout(15_000)).listen(received.capture(), anyInt());

        DummyMessage message = received.getValue();
        assertThat(message.getText().toString()).isEqualTo("through the embedded broker");
        assertThat(message.getId()).isNotNull();
        assertThat(message.getCreatedAt()).isNotNull();
    }
}
