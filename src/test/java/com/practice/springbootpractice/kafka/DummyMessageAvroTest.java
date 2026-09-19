package com.practice.springbootpractice.kafka;

import com.practice.springbootpractice.avro.DummyMessage;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DummyMessageAvroTest {

    @Test
    void messageSurvivesAnAvroSerializeDeserializeRoundTrip() throws IOException {
        DummyMessage original = DummyMessage.newBuilder()
                .setId("6f1c1f0e-0000-4000-8000-000000000001")
                .setText("round trip")
                .setCreatedAt(Instant.ofEpochMilli(1_758_247_576_249L))
                .build();

        DummyMessage decoded = deserialize(serialize(original));

        assertThat(decoded).isEqualTo(original);
        // Avro decodes string fields as Utf8 unless stringType=String is configured.
        assertThat(decoded.getText().toString()).isEqualTo("round trip");
        assertThat(decoded.getCreatedAt()).isEqualTo(Instant.ofEpochMilli(1_758_247_576_249L));
    }

    @Test
    void createdAtIsStoredAtMillisecondPrecision() throws IOException {
        Instant withNanos = Instant.parse("2026-09-19T02:06:16.249123456Z");
        DummyMessage original = DummyMessage.newBuilder()
                .setId("id")
                .setText("precision")
                .setCreatedAt(withNanos)
                .build();

        DummyMessage decoded = deserialize(serialize(original));

        assertThat(decoded.getCreatedAt()).isEqualTo(Instant.parse("2026-09-19T02:06:16.249Z"));
    }

    private static byte[] serialize(DummyMessage message) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(DummyMessage.class).write(message, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static DummyMessage deserialize(byte[] bytes) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return new SpecificDatumReader<>(DummyMessage.class).read(null, decoder);
    }
}
