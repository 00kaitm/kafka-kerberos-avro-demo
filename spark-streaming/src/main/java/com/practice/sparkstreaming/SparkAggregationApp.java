package com.practice.sparkstreaming;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;

/**
 * Runs three independent Structured Streaming queries against dummy-topic on one SparkSession:
 * the same console pass-through as SparkConsoleApp, plus a tumbling-window, watermarked count of
 * messages per `text` value written to both Parquet and Postgres.
 *
 * Each writeStream().start() call below is a fully separate query with its own Kafka consumer,
 * its own checkpoint and its own state, even though the Parquet and Postgres queries are both
 * built from the one windowedCounts Dataset object - reusing that Java object never means
 * reusing a Kafka read or any state between the two queries built from it.
 *
 * Run from the repo root with: bash spark-streaming/run.sh aggregation
 */
public class SparkAggregationApp {

    private static final String UPSERT_SQL =
            "INSERT INTO topic_counts (window_start, window_end, text, count) VALUES (?, ?, ?, ?) "
                    + "ON CONFLICT (window_start, window_end, text) DO UPDATE SET count = EXCLUDED.count";

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf", "docker/kerberos/krb5-client.conf");

        Properties config = ConfigLoader.load();
        String valueSchema = SchemaRegistry
                .fetchLatest(config.getProperty("schema.registry.url"), config.getProperty("topic") + "-value")
                .schema();

        SparkSession spark = SparkSession.builder()
                .appName("dummy-topic-aggregation")
                .master("local[*]")
                // Spark's default (200) is sized for a cluster; on a laptop, for this tiny
                // amount of data, it just means 200 mostly-empty tasks to schedule per batch
                // instead of a handful.
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        String windowDuration = config.getProperty("window.duration");
        String watermarkDelay = config.getProperty("watermark.delay");
        Trigger trigger = Trigger.ProcessingTime(config.getProperty("trigger.interval"));

        // --- Console: same pass-through as SparkConsoleApp, kept alive alongside the new sinks.
        StreamingQuery consoleQuery = KafkaAvroSource.readDecoded(spark, config, valueSchema)
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", false)
                .option("checkpointLocation", config.getProperty("checkpoint.location"))
                .start();

        // --- Tumbling window count per `text`, with a watermark so both sinks below can tell a
        // window is "done" instead of holding its state forever. withWatermark must come before
        // groupBy in the chain - it marks which column is event time and how much lateness to
        // tolerate for it.
        Dataset<Row> windowedCounts = KafkaAvroSource.readDecoded(spark, config, valueSchema)
                .withWatermark("createdAt", watermarkDelay)
                .groupBy(window(col("createdAt"), windowDuration), col("text"))
                .count()
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("text"),
                        col("count"));

        // --- Parquet: append-only, because a file sink can never rewrite a row it already
        // wrote. The watermark is what makes append legal here: Spark only emits a window's row
        // once the watermark has passed window_end, i.e. once it's sure no more data can arrive
        // for it, so every window is still written exactly once, just later than it occurred.
        StreamingQuery parquetQuery = windowedCounts.writeStream()
                .format("parquet")
                .outputMode("append")
                .option("path", config.getProperty("parquet.output.path"))
                .option("checkpointLocation", config.getProperty("parquet.checkpoint.location"))
                .trigger(trigger)
                .start();

        // --- Postgres: no built-in streaming JDBC sink, so foreachBatch with a manual upsert.
        // update mode (rather than append) so a window's row is visible - and corrected - while
        // it's still filling, not only once at the end.
        String jdbcUrl = config.getProperty("jdbc.url");
        String jdbcUser = config.getProperty("jdbc.user");
        String jdbcPassword = config.getProperty("jdbc.password");
        StreamingQuery postgresQuery = windowedCounts.writeStream()
                .outputMode("update")
                .option("checkpointLocation", config.getProperty("postgres.checkpoint.location"))
                .trigger(trigger)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>)
                        (batchDf, batchId) -> upsertBatch(batchDf, jdbcUrl, jdbcUser, jdbcPassword))
                .start();

        spark.streams().awaitAnyTermination();
    }

    /**
     * Runs once per micro-batch, once per partition. Spark's fault tolerance for foreachBatch is
     * at-least-once, not exactly-once - there's no transaction spanning Spark's checkpoint and
     * Postgres, so a crash between writing this batch and the checkpoint recording it as done
     * makes Spark replay the same batch after a restart. The upsert (ON CONFLICT ... DO UPDATE)
     * is what makes that safe: a replayed batch just overwrites a window's row with the same
     * count instead of inserting a duplicate.
     */
    private static void upsertBatch(Dataset<Row> batchDf, String jdbcUrl, String jdbcUser, String jdbcPassword) {
        batchDf.foreachPartition((Iterator<Row> rows) -> {
            if (!rows.hasNext()) {
                return;
            }
            try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(UPSERT_SQL)) {
                    int batched = 0;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        stmt.setTimestamp(1, row.getAs("window_start"));
                        stmt.setTimestamp(2, row.getAs("window_end"));
                        stmt.setString(3, row.getAs("text"));
                        stmt.setLong(4, row.<Long>getAs("count"));
                        stmt.addBatch();
                        if (++batched % 500 == 0) {
                            stmt.executeBatch();
                        }
                    }
                    stmt.executeBatch();
                }
                conn.commit();
            }
        });
    }
}
