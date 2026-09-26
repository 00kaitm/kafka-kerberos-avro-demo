package com.practice.sparkstreaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.count_if;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.when;

/**
 * Reads dummy-topic and decodes the Confluent-wire-format Avro DummyMessage records. Shared by
 * every app in this module; each app's own writeStream().start() still re-reads Kafka
 * independently, even when they all call this method on the same SparkSession - sharing this
 * method never means sharing a Kafka consumer or any state between queries.
 *
 * Records that aren't valid Confluent Avro (a "poison pill") are dropped rather than failing the
 * query, and counted: see README "Poison pills and the Spark jobs".
 */
final class KafkaAvroSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSource.class);

    /** Name of the per-batch metrics Dataset.observe() attaches; reported in each query's progress. */
    static final String DECODE_METRICS = "dummy_topic_decode";

    private static final Set<SparkSession> SESSIONS_LOGGING_SKIPS =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

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

        logSkippedRecords(spark);
        return decode(kafkaRecords, valueSchema);
    }

    /** Takes the Kafka source's rows (binary `value` plus topic/partition/offset/timestamp). */
    static Dataset<Row> decode(Dataset<Row> kafkaRecords, String valueSchema) {
        // Confluent wire format: byte 0 is a magic byte (always 0), bytes 1-4 are the writer
        // schema's ID, and the Avro binary payload from_avro understands starts at byte 5.
        // Anything too short for that header, or with a different first byte, was never written
        // by a Confluent Avro serializer, so don't even try to decode it.
        Column hasConfluentHeader = col("value").isNotNull()
                .and(length(col("value")).geq(5))
                .and(expr("substring(value, 1, 1) = X'00'"));

        // substring's start is 1-based, so this keeps everything from the 6th byte onward.
        Column avroPayload = expr("substring(value, 6, length(value) - 5)");

        // PERMISSIVE: a payload from_avro can't parse no longer throws. The default (FAILFAST)
        // fails the whole micro-batch, and since a failed batch never reaches the checkpoint, a
        // restarted query would hit the same record again, forever.
        Column record = when(hasConfluentHeader,
                from_avro(avroPayload, valueSchema, Map.of("mode", "PERMISSIVE")));

        // Careful: for a record schema, PERMISSIVE's "null result" is a struct whose fields are
        // all null, not a null struct - so `record IS NOT NULL` alone lets a parse failure through
        // as a row of NULLs. `id` is a required (non-nullable) field in DummyMessage.avsc, so no
        // genuinely decoded record can have it null. `record` itself is null when the header
        // check above skipped from_avro entirely.
        Column decoded = col("record").isNotNull().and(col("record.id").isNotNull());

        return kafkaRecords
                .withColumn("record", record)
                .observe(DECODE_METRICS,
                        count(lit(1)).as("received"),
                        count_if(not(decoded)).as("skipped"))
                .filter(decoded)
                .select(
                        col("record.id").as("id"),
                        col("record.text").as("text"),
                        col("record.createdAt").as("createdAt"),
                        col("topic"),
                        col("partition"),
                        col("offset"),
                        col("timestamp"));
    }

    /**
     * Logs a warning for every micro-batch that dropped records. Registered once per SparkSession;
     * it only reacts to queries whose plan includes decode()'s observe() metrics.
     */
    private static void logSkippedRecords(SparkSession spark) {
        if (!SESSIONS_LOGGING_SKIPS.add(spark)) {
            return;
        }
        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {
            }

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                Row metrics = event.progress().observedMetrics().get(DECODE_METRICS);
                if (metrics == null) {
                    return;
                }
                long skipped = metrics.<Long>getAs("skipped");
                if (skipped > 0) {
                    log.warn("Query {} batch {}: skipped {} of {} Kafka records that aren't valid Confluent Avro",
                            event.progress().name(), event.progress().batchId(),
                            skipped, metrics.<Long>getAs("received"));
                }
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
            }
        });
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
