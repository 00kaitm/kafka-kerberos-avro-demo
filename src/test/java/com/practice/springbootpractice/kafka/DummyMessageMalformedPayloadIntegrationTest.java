package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Negative cases: records on the topic that are not valid Avro {@code DummyMessage}s.
 * The consumer must not get stuck on them, and a good message sent afterwards must
 * still be delivered.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=true"
})
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"dummy-topic", "dummy-topic-dlt"})
@DirtiesContext
class DummyMessageMalformedPayloadIntegrationTest {

    private static final String TOPIC = "dummy-topic";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @Autowired
    private DummyProducer producer;

    @MockitoSpyBean
    private DummyConsumer consumer;

    @Test
    void aNonAvroRecordDoesNotBlockLaterValidMessages() throws Exception {
        sendRaw("not avro at all".getBytes(StandardCharsets.UTF_8));

        producer.send("valid after garbage");

        verifyConsumerEventuallyReceives("valid after garbage");
    }

    @Test
    void aNullValuedRecordDoesNotBlockLaterValidMessages() throws Exception {
        sendRaw(null);

        producer.send("valid after null");

        verifyConsumerEventuallyReceives("valid after null");
    }

    private void verifyConsumerEventuallyReceives(String text) {
        verify(consumer, timeout(30_000)).listen(argThat(
                (DummyMessage m) -> m != null && text.equals(m.getText().toString())), anyInt());
    }

    /** Publishes with a plain byte[] serializer, bypassing Avro and the schema registry. */
    private void sendRaw(byte[] value) throws Exception {
        Map<String, Object> props = KafkaTestUtils.producerProps(brokers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);
        KafkaTemplate<String, byte[]> rawTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, byte[]>(props));
        try {
            rawTemplate.send(new ProducerRecord<>(TOPIC, "bad-key", value)).get();
        } finally {
            rawTemplate.destroy();
        }
    }
}
