package com.practice.springbootpractice.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the producer reliability settings from application.yml, so a later config change that
 * drops them (and with them the idempotence guarantee) fails a test instead of passing silently.
 */
@SpringBootTest
@ActiveProfiles("test")
class ProducerReliabilityConfigTest {

    @Autowired
    private ProducerFactory<?, ?> producerFactory;

    @Test
    void theProducerIsIdempotentWithAcksAll() {
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        assertThat(config.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(String.valueOf(config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))).isEqualTo("true");
    }
}
