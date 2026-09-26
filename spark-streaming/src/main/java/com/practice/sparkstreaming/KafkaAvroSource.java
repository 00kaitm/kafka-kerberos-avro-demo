package com.practice.sparkstreaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

/**
 * Reads dummy-topic and decodes the Confluent-wire-format Avro DummyMessage records. Shared by
 * every app in this module; each app's own writeStream().start() still re-reads Kafka
 * independently, even when they all call this method on the same SparkSession - sharing this
 * method never means sharing a Kafka consumer or any state between queries.
 */
final class KafkaAvroSource {

    private KafkaAvroSource() {
    }

    static Dataset<Row> readDecoded(SparkSession spark, Properties config, String valueSchema) {
        Dataset<Row> kafkaRecords = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getProperty("bootstrap.servers"))
                .option("subscribe", config.getProperty("topic"))
                .option("startingOffsets", "earliest")
                .options(kafkaSecurityOptions(config))
                .load();

        // Confluent wire format: byte 0 is a magic byte, bytes 1-4 are the writer schema's ID,
        // and the Avro binary payload from_avro understands starts at byte 5. substring's start
        // is 1-based, so this keeps everything from the 6th byte onward.
        Column avroPayload = expr("substring(value, 6, length(value) - 5)");

        return kafkaRecords
                .withColumn("record", from_avro(avroPayload, valueSchema))
                .select(
                        col("record.id").as("id"),
                        col("record.text").as("text"),
                        col("record.createdAt").as("createdAt"),
                        col("topic"),
                        col("partition"),
                        col("offset"),
                        col("timestamp"));
    }

    static Map<String, String> kafkaSecurityOptions(Properties config) {
        Map<String, String> options = new HashMap<>();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("kafka.")) {
                options.put(key, config.getProperty(key));
            }
        }
        return options;
    }
}
