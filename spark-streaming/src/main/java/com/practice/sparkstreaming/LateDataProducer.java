package com.practice.sparkstreaming;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

/**
 * Publishes one DummyMessage directly to dummy-topic with a caller-chosen createdAt, to
 * demonstrate the watermark in SparkAggregationApp: a message old enough to fall outside it gets
 * silently dropped from the windowed count instead of being aggregated.
 *
 * Builds the Confluent wire format (magic byte + 4-byte schema ID + Avro binary) by hand - the
 * same format KafkaAvroSource strips on the way in - deliberately not
 * io.confluent:kafka-avro-serializer, for the same reason SchemaRegistry uses a plain HTTP call:
 * avoiding a second, differently versioned copy of Jackson on Spark's classpath for a demo tool.
 *
 * Usage (from the repo root): bash spark-streaming/run.sh late-data "<text>" <secondsAgo>
 */
public class LateDataProducer {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LateDataProducer <text> <secondsAgo>");
            System.exit(1);
        }
        String text = args[0];
        long secondsAgo = Long.parseLong(args[1]);

        System.setProperty("java.security.krb5.conf", "docker/kerberos/krb5-client.conf");

        Properties config = ConfigLoader.load();
        String topic = config.getProperty("topic");
        SchemaRegistry.SchemaInfo schemaInfo = SchemaRegistry.fetchLatest(
                config.getProperty("schema.registry.url"), topic + "-value");
        Schema avroSchema = new Schema.Parser().parse(schemaInfo.schema());

        long createdAtMillis = Instant.now().minusSeconds(secondsAgo).toEpochMilli();
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", UUID.randomUUID().toString());
        record.put("text", text);
        record.put("createdAt", createdAtMillis);

        byte[] value = encode(schemaInfo.id(), avroSchema, record);

        Properties producerProps = producerProperties(config);
        // Printed rather than logged: this tool never starts Spark, so Spark's log4j2 defaults are
        // never installed, and log4j2's own fallback hides the Kafka client's INFO-level
        // "ProducerConfig values:" block. See README "Idempotent producer".
        System.out.println(effectiveReliabilitySettings(producerProps));

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }

        System.out.printf("Sent \"%s\" with createdAt=%s (%d seconds ago)%n",
                text, Instant.ofEpochMilli(createdAtMillis), secondsAgo);
    }

    static Properties producerProperties(Properties config) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        for (String key : config.stringPropertyNames()) {
            // kafka.* is shared with Spark's (consumer-side) Kafka source, so only take the keys a
            // producer actually understands - the connection and security settings - and leave
            // consumer-only ones like kafka.isolation.level behind.
            if (key.startsWith("kafka.")) {
                String name = key.substring("kafka.".length());
                if (ProducerConfig.configNames().contains(name)) {
                    producerProps.put(name, config.getProperty(key));
                }
            } else if (key.startsWith("producer.")) {
                producerProps.put(key.substring("producer.".length()), config.getProperty(key));
            }
        }
        return producerProps;
    }

    /**
     * The reliability settings the producer will actually run with, parsed by Kafka's own
     * ProducerConfig exactly as KafkaProducer's constructor does - so "all" shows up resolved to
     * -1, and idempotence shows up false if some other setting conflicted with it.
     */
    static String effectiveReliabilitySettings(Properties producerProps) {
        ProducerConfig effective = new ProducerConfig(producerProps);
        return String.format(
                "Producer config: acks=%s enable.idempotence=%s retries=%d max.in.flight.requests.per.connection=%d delivery.timeout.ms=%d",
                effective.getString(ProducerConfig.ACKS_CONFIG),
                effective.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
                effective.getInt(ProducerConfig.RETRIES_CONFIG),
                effective.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                effective.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    }

    private static byte[] encode(int schemaId, Schema schema, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0); // magic byte
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
