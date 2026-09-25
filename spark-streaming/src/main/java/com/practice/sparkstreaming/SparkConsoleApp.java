package com.practice.sparkstreaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

/**
 * Reads the Avro DummyMessage topic the Spring Boot app produces to, over the same
 * Kerberos/SASL_SSL + TLS secured Kafka listener, and prints the decoded records to the console.
 *
 * Run from the repo root with: mvn -f spark-streaming/pom.xml exec:java
 */
public class SparkConsoleApp {

    public static void main(String[] args) throws Exception {
        // Must be set before any Kafka/Kerberos client class loads, same as
        // SpringBootPracticeApplication.main does for the producer/consumer app.
        System.setProperty("java.security.krb5.conf", "docker/kerberos/krb5-client.conf");

        Properties config = ConfigLoader.load();
        String topic = config.getProperty("topic");
        String schemaRegistryUrl = config.getProperty("schema.registry.url");

        String valueSchema = SchemaRegistry.fetchLatestSchema(schemaRegistryUrl, topic + "-value");

        SparkSession spark = SparkSession.builder()
                .appName("dummy-topic-console")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> kafkaRecords = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getProperty("bootstrap.servers"))
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .options(kafkaSecurityOptions(config))
                .load();

        // Confluent wire format: byte 0 is a magic byte, bytes 1-4 are the writer schema's ID,
        // and the Avro binary payload from_avro understands starts at byte 5. substring's start
        // is 1-based, so this keeps everything from the 6th byte onward.
        Column avroPayload = expr("substring(value, 6, length(value) - 5)");

        Dataset<Row> decoded = kafkaRecords
                .withColumn("record", from_avro(avroPayload, valueSchema))
                .select(
                        col("record.id").as("id"),
                        col("record.text").as("text"),
                        col("record.createdAt").as("createdAt"),
                        col("topic"),
                        col("partition"),
                        col("offset"),
                        col("timestamp"));

        StreamingQuery query = decoded.writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", false)
                .option("checkpointLocation", config.getProperty("checkpoint.location"))
                .start();

        query.awaitTermination();
    }

    private static Map<String, String> kafkaSecurityOptions(Properties config) {
        Map<String, String> options = new HashMap<>();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("kafka.")) {
                options.put(key, config.getProperty(key));
            }
        }
        return options;
    }
}
