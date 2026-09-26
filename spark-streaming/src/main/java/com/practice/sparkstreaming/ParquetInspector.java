package com.practice.sparkstreaming;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * A plain batch (non-streaming) read of the Parquet output SparkAggregationApp writes, so
 * inspecting it doesn't require installing anything beyond what this module already needs.
 *
 * Usage (from the repo root): bash spark-streaming/run.sh parquet-inspect
 */
public class ParquetInspector {

    public static void main(String[] args) throws Exception {
        Properties config = ConfigLoader.load();

        SparkSession spark = SparkSession.builder()
                .appName("dummy-topic-counts-parquet-inspect")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        spark.read()
                .parquet(config.getProperty("parquet.output.path"))
                .orderBy("window_start", "text")
                .show(100, false);

        spark.stop();
        // exec-maven-plugin interrupts lingering threads on the way out rather than waiting for
        // them; some of Hadoop's background housekeeping threads don't respond to that cleanly,
        // which prints harmless but alarming-looking warnings. A hard exit avoids that entirely.
        System.exit(0);
    }
}
