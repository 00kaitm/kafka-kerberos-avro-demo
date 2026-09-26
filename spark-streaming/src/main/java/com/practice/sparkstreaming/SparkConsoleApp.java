package com.practice.sparkstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Properties;

/**
 * Reads the Avro DummyMessage topic the Spring Boot app produces to, over the same
 * Kerberos/SASL_SSL + TLS secured Kafka listener, and prints the decoded records to the console.
 *
 * Run from the repo root with: bash spark-streaming/run.sh
 */
public class SparkConsoleApp {

    public static void main(String[] args) throws Exception {
        // Must be set before any Kafka/Kerberos client class loads, same as
        // SpringBootPracticeApplication.main does for the producer/consumer app.
        System.setProperty("java.security.krb5.conf", "docker/kerberos/krb5-client.conf");

        Properties config = ConfigLoader.load();
        String valueSchema = SchemaRegistry
                .fetchLatest(config.getProperty("schema.registry.url"), config.getProperty("topic") + "-value")
                .schema();

        SparkSession spark = SparkSession.builder()
                .appName("dummy-topic-console")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> decoded = KafkaAvroSource.readDecoded(spark, config, valueSchema);

        StreamingQuery query = decoded.writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", false)
                .option("checkpointLocation", config.getProperty("checkpoint.location"))
                .start();

        query.awaitTermination();
    }
}
