# kafka-kerberos-avro-demo

A practice project that sends and receives Avro-encoded messages over a **secured Apache Kafka** cluster
(Kerberos/SASL + TLS) using Spring Boot. Everything runs locally: the app on your machine, and Kafka, a
Kerberos KDC and a Schema Registry in Docker.

## What it does so far

1. You `POST` a plain-text message to `http://localhost:8080/dummy-topic/messages`.
2. The **producer** wraps it in an Avro `DummyMessage` (`id`, `text`, `createdAt`) and publishes it to the
   Kafka topic `dummy-topic`.
3. The **consumer**, in the same app, receives it from that topic and logs it.
4. If the consumer fails on a record, it **retries** with back off; if it still can't process it (or it can
   never succeed, like bytes that aren't Avro), the record goes to the **dead-letter topic** `dummy-topic-dlt`
   instead of being lost or blocking the topic. See [Reliability](#reliability).

```
 curl / Postman
      │  POST /dummy-topic/messages   (or /dummy-topic/poison-pill for the failure demo)
      ▼
┌──────────────── Spring Boot app :8080 ─────────────────┐     ┌───────────── Docker ─────────────┐
│ DummyMessageController ─► DummyProducer ── Avro ───────┼────►│ dummy-topic                      │
│ PoisonPillController ── raw bytes (demo) ──────────────┼────►│                                  │
│                                                        │     │                                  │
│ DummyConsumer ◄ ───────────────────────────────────────┼─────│ dummy-topic                      │
│   │ throws                                             │     │                                  │
│   ▼                                                    │     │                                  │
│ DefaultErrorHandler: retries, then ────────────────────┼────►│ dummy-topic-dlt                  │
│                                                        │     │                                  │
│ DeadLetterInspector ◄ ─────────────────────────────────┼─────│ dummy-topic-dlt                  │
└────────────────────────────────────────────────────────┘     │                                  │
                                                               │ Kafka broker :9094 (KRaft, ACLs) │
                                                               │ KDC (Kerberos) :88               │
                                                               │ Schema Registry :8081            │
                                                               └──────────────────────────────────┘
```

Expected log output when it works:

```
DummyProducer : Sent message to dummy-topic partition 0 offset 0
DummyConsumer : Received message: id=06fb7c55-... text=hello createdAt=2026-09-19T02:06:16.249Z
```

### Code map

| File | Role |
|---|---|
| `kafka/DummyMessageController.java` | `POST /dummy-topic/messages`. Passes the body to the producer and returns `queued`. |
| `kafka/DummyProducer.java` | Builds a `DummyMessage` (random UUID, current time) and sends it with `KafkaTemplate`. Logs success or failure asynchronously. |
| `kafka/DummyConsumer.java` | `@KafkaListener` on the topic, group `dummy-consumer-group`. Logs each message, with its delivery attempt number. |
| `kafka/KafkaErrorHandlingConfig.java` | The `DefaultErrorHandler`: capped exponential back off, which exceptions skip retries, and the `DeadLetterPublishingRecoverer`. |
| `kafka/DeadLetterInspector.java` | `@KafkaListener` on `dummy-topic-dlt`, group `dummy-dlt-inspector`. Logs where each dead letter came from and why it failed. |
| `kafka/FailureDemo.java` | Makes `DummyConsumer` fail on purpose for texts starting with `fail-transient`, `fail-always` or `fail-invalid`. |
| `kafka/PoisonPillController.java` | `POST /dummy-topic/poison-pill`. Publishes the body as raw bytes, not Avro. Demo only (`app.kafka.failure-demo.enabled`). |
| `kafka/RawBytesProducer.java` | A `byte[]` producer sharing Boot's producer config, for the DLT and the poison-pill endpoint. |
| `kafka/RetryableProcessingException.java`, `kafka/InvalidMessageException.java` | The two failure kinds: worth retrying, and never worth retrying. |
| `src/main/avro/DummyMessage.avsc` | Avro schema. The `avro-maven-plugin` generates the `DummyMessage` class at build time. |
| `src/main/resources/application.yml` | Kafka, Kerberos, TLS and Schema Registry client settings; topic names, retry/back off and failure-demo settings under `app.kafka`. |

## How the security setup works

The broker only accepts clients that authenticate with **Kerberos (SASL/GSSAPI) over TLS**, and it enforces **ACLs**.

- **Kerberos:** a KDC container hosts the realm `EXAMPLE.COM` and creates three service/user principals with
  keytabs written to `docker/kerberos/keytabs/`: `kafka/*` (broker), `client@` (this app) and `registry/localhost@`
  (Schema Registry). The app logs in with `client.keytab` (see `sasl.jaas.config` in `application.yml`).
- **TLS:** `docker/certs/` holds a dev CA plus keystores and a truststore, generated by
  `docker/certs/generate-certs.sh` with random passwords.
- **ACLs:** `client` may write, read and describe `dummy-topic` and `dummy-topic-dlt`, and read groups
  `dummy-consumer-group` and `dummy-dlt-inspector`.
  `registry` owns the `_schemas` topic. `docker/init-topics.sh` creates the topics and ACLs. The `topic-init`
  service in `docker-compose.yml` runs it automatically on `docker compose up`, and the Schema Registry waits for it
  to finish, because the registry fails on startup if `_schemas` and its ACLs don't exist yet.
- **krb5 config:** `SpringBootPracticeApplication.main` sets `java.security.krb5.conf` to
  `docker/kerberos/krb5-client.conf`, which points at the KDC on `localhost:88`. Because the app connects to
  `localhost:9094`, the Kerberos service principal it requests is `kafka/localhost@EXAMPLE.COM`.

> **No secrets are committed.** Keys, keystores, keytabs and the KDC database are git-ignored, and every password is
> random and generated on your machine into `docker/.env` (also git-ignored). `docker/.env.example` lists the
> variable names. These are still local-dev credentials for a throwaway stack, not something to reuse elsewhere.

## Prerequisites

- **JDK 17** (`JAVA_HOME` set). The Maven wrapper (`mvnw`) is included, so you don't need Maven installed.
- **Docker Desktop** running. On an 8 GB machine, close heavy apps first, because the stack uses about 2 GB.
- Free local ports: **88** (KDC), **9094** (Kafka), **8081** (Schema Registry), **8080** (the app),
  **5432** (Postgres, only needed for `SparkAggregationApp`).

## Running the application

### First-time setup (fresh clone only)

The TLS material and passwords are deliberately not committed, so generate them once:

```bash
bash docker/certs/generate-certs.sh
```

Run it from Git Bash on Windows, or any bash shell. It needs `openssl` and the JDK's `keytool` on your `PATH`.
It writes, all git-ignored:
- the dev CA, the broker, client and registry keystores, the shared truststore, and the `broker_*_creds` files the
  broker image reads, in `docker/certs/`;
- `docker/certs/admin-sasl-ssl.properties`, the Kafka CLI client config that `init-topics.sh` uses;
- **`docker/.env`**, holding a random password for each store and for the KDC. Docker Compose reads it
  automatically, and the app loads it through `spring.config.import` in `application.yml`. That is why the app
  must run with the project root as its working directory.

Re-running the script replaces the certs and passwords, so restart the stack afterwards
(`docker compose up -d --force-recreate`). It keeps the existing KDC master password, because that has to match
the KDC database. To rotate that too, delete `docker/.env`, empty `docker/kerberos/kdc-data/` and
`docker/kerberos/keytabs/`, and run the script again.

The Kerberos keytabs and KDC database need no setup: the KDC creates them the first time `docker compose up` runs.

### 1. Start the infrastructure

```powershell
cd docker
docker compose up -d
docker compose ps -a     # kafka-kdc, kafka-broker, kafka-schema-registry "running"; kafka-topic-init "exited (0)"
```

That one command does everything, in order: the KDC and broker start, the one-shot `kafka-topic-init` job creates
the topics and ACLs and exits, and only then does the Schema Registry start. On a brand-new broker it takes about
a minute. `kafka-topic-init` showing `exited (0)` is normal: it is a job, not a server.

The broker keeps its topics and ACLs in the `kafka-data` volume, so they survive `docker compose stop`, `down`
and container recreation, and the job simply confirms them on the next `up`. To run it again by hand:

```powershell
docker compose up topic-init
```

### 2. Start the app

**From a terminal** (run from the project root):

```powershell
.\mvnw.cmd spring-boot:run
```

**From IntelliJ**, see [Run configurations](#run-configurations-intellij) below.

### 3. Send a test message

```powershell
Invoke-WebRequest -Uri http://localhost:8080/dummy-topic/messages -Method POST -Body "hello" -ContentType "text/plain"
```

or `curl -X POST -H "Content-Type: text/plain" -d "hello" http://localhost:8080/dummy-topic/messages`.

Then check the app's console for the `Sent message` and `Received message` lines.

### 4. Shut down

```powershell
cd docker
docker compose stop      # stops the containers but keeps the data
```

## Run configurations (IntelliJ)

The app is **one program that is both producer and consumer**. Which role a running instance plays comes from
configuration, so you can create one run configuration per role.

**Settings every configuration needs** (*Run → Edit Configurations… → + → Spring Boot*, or *Application* on
Community Edition):

| Field | Value |
|---|---|
| Main class | `com.practice.springbootpractice.SpringBootPracticeApplication` |
| JDK / `-cp` module | Java 17, module `SpringBootPractice` |
| **Working directory** | **The project root** (`$MODULE_WORKING_DIR$`, or the folder that contains `docker/`) |

The working directory is essential. `application.yml` and `main()` use relative paths such as
`docker/kerberos/keytabs/client.keytab` and `docker/certs/truststore.jks`. If the working directory is anything
else, the app fails at startup with Kerberos or keystore "file not found" errors.

### Consumer (default)

The plain configuration above is a consumer, because it starts the `@KafkaListener` automatically. It joins group
`dummy-consumer-group`, is assigned `dummy-topic-0`, and logs every message that reaches the topic. You don't
need to `POST` to it. Messages sent by any producer will show up.

Name it something like `App - consumer + producer`. It also exposes the `POST` endpoint, so on its own it does both.

### Producer only

Create a second configuration that turns the listener off, on a different port so both can run together:

| Field | Value |
|---|---|
| Name | `App - producer only` |
| Program arguments | `--server.port=8082 --spring.kafka.listener.auto-startup=false` |

(Equivalent environment variables: `SERVER_PORT=8082;SPRING_KAFKA_LISTENER_AUTO_STARTUP=false`.)

`POST` to `http://localhost:8082/dummy-topic/messages`. This instance only publishes: you'll see
`Sent message ...` in its console. If the consumer configuration is also running, the `Received message ...`
line appears in *that* console.

Don't use port **8081** for the app: the Schema Registry already listens there.

### Consumer only

There is no separate consumer-only mode. Run the default configuration and don't `POST` to it, or run it alongside the
producer-only instance above and use that one to send.

## Tests

```powershell
.\mvnw.cmd test
```

All tests run **without Docker**. They use an in-process Kafka broker and a `mock://` schema registry with plaintext
security, configured by `src/test/resources/application-test.yml` (profile `test`).

| Test | What it covers |
|---|---|
| `DummyProducerTest` | Producer logic with a mocked `KafkaTemplate`: topic, text, UUID id, timestamp, failure handling. |
| `DummyMessageAvroTest` | Avro serialize/deserialize round trip and millisecond timestamp precision. |
| `DummyMessageControllerTest` | `@WebMvcTest` of the endpoint: 200 `queued`, 400 with no body, 405 on GET. |
| `SpringBootPracticeApplicationTests` | The Spring context loads with no broker running. |
| `DummyMessageFlowIntegrationTest` | Producer → Avro → embedded Kafka → consumer, both directly and via an HTTP `POST`. |
| `DummyMessageMalformedPayloadIntegrationTest` | A non-Avro record and a null-valued record on the topic don't block later valid messages. |
| `ProducerReliabilityConfigTest` | The producer factory really has `acks=all` and `enable.idempotence=true` (see "Reliability"). |
| `DeadLetterIntegrationTest` | Each failure path: retried then succeeded, retries exhausted then DLT, not retryable so straight to DLT, and a poison pill reaching the DLT with its original bytes and headers. |

The tests don't exercise Kerberos, TLS or ACLs. The Docker stack and a manual `POST` cover those.

## Spark Structured Streaming (`spark-streaming/`)

A second, independent reader of `dummy-topic`: Java Spark Structured Streaming jobs that decode the same
Avro `DummyMessage` records. `SparkConsoleApp` just prints them; `SparkAggregationApp` additionally counts
them per `text` value in tumbling windows and writes those counts to Parquet and Postgres. It's a separate,
standalone Maven project (its own `pom.xml`, not a module of the root one), because it has nothing to do
with the Spring Boot app other than reading the same topic.

```
                                                        ┌──► DummyConsumer (@KafkaListener, group dummy-consumer-group)
                                                        │
Spring Boot producer ──Avro──► dummy-topic ─────────────┼──► SparkConsoleApp (console)
                                                        │
                                                        └──► SparkAggregationApp ──┬──► console (raw pass-through)
                                                                                   ├──► Parquet (windowed counts)
                                                                                   └──► Postgres (windowed counts)
```

Every arrow out of `dummy-topic` above is an independent read - see "Every query reads Kafka
independently" below for why that's true even *within* `SparkAggregationApp`, where two of the three
sinks share one Java `Dataset` object in the code. Spark's Kafka source doesn't join a consumer group the
way `DummyConsumer` does - see "How Spark tracks offsets" - so none of these compete for partitions or
interfere with each other.

### How Spark tracks offsets

`DummyConsumer` uses a regular Kafka consumer group (`dummy-consumer-group`): the broker assigns it
partitions, and after processing each record the consumer commits its offset back *to the broker*, so
other members of the group (and the broker itself) know what's been read.

Spark's Kafka source doesn't do any of that, despite appearances: it *does* set a `group.id` - a unique
one it auto-generates per query (`spark-kafka-source-<uuid>-...`) if you don't set one yourself - but only
because the Kafka protocol requires some group ID to be present on the wire; Spark never uses Kafka's
group-coordination protocol to get partitions assigned, and it never commits offsets back to the broker.
Instead, at the end of every micro-batch, Spark writes the offsets it just processed into its own
checkpoint directory (see "Checkpointing" below) and assigns itself the topic's partitions directly. On
the next batch, or after a restart *from that same checkpoint*, it reads the checkpoint to know where to
resume.

### Confluent's wire format, and why `from_avro` alone isn't enough

Every record `KafkaAvroSerializer` writes has 5 bytes in front of the actual Avro binary: 1 magic byte,
then a 4-byte big-endian ID identifying which schema (registered in the Schema Registry) wrote it. Spark's
`from_avro` function expects to start reading Avro binary at byte 0, so handing it the raw Kafka record
value produces garbage or an error - it doesn't know about that 5-byte header at all.

Two ways to handle it:
- **Strip the header and supply the schema** (what this does): drop the first 5 bytes with
  `substring(value, 6, length(value) - 5)`, fetch the schema once from the Schema Registry's REST API, and
  pass it to `from_avro`. A few lines of Java, no extra dependency, and it makes the wire format visible
  instead of hiding it.
- **ABRiS**: a library that does Schema-Registry-aware Avro decoding for you. It's Scala-first (implicit
  DataFrame extensions), usable from Java but less natural, and it hides the byte-level detail the first
  option makes explicit.

`SchemaRegistry.fetchLatest` (in `spark-streaming/src/main/java/com/practice/sparkstreaming/`) does a
plain `GET /subjects/dummy-topic-value/versions/latest` against the registry - unauthenticated in this
stack, see `docker-compose.yml` - and `SparkConsoleApp` does the byte-stripping and calls
`org.apache.spark.sql.avro.functions.from_avro(Column, String)`.

### Config

`spark-streaming/src/main/resources/spark-streaming.properties` holds the bootstrap servers, topic, Schema
Registry URL, checkpoint location, and the Kafka client security settings, passed straight through to
Spark's Kafka source via its `kafka.`-prefixed options (the same settings `application.yml` sets under
`spring.kafka.properties`, just re-pointed at the same keytab, truststore and client keystore). Passwords
are `${VAR}` placeholders resolved from `docker/.env` at startup by `ConfigLoader`, the same file
`application.yml` reads via Spring's `spring.config.import`.

### Running it

**One-time, Windows only:** Spark's checkpointing goes through Hadoop's local filesystem code, which on
Windows needs a `winutils.exe` helper Spark doesn't ship. Run:

```bash
bash spark-streaming/setup-windows-hadoop.sh
```

This downloads `winutils.exe` and `hadoop.dll` from [cdarlint/winutils](https://github.com/cdarlint/winutils)
(the community-maintained continuation of the original winutils project) into `spark-streaming/hadoop-local/`,
git-ignored. Not needed on macOS/Linux.

Then, with the Docker stack up (`docker compose up -d` in `docker/`, same as for the Spring Boot app):

```bash
bash spark-streaming/run.sh
```

`POST` a message the same way you would to test the Spring Boot app (see "Send a test message" above), and
within a couple of seconds it prints a batch like:

```
+------------------------------------+------+-----------------------+-----------+---------+------+-----------------------+
|id                                  |text  |createdAt              |topic      |partition|offset|timestamp              |
+------------------------------------+------+-----------------------+-----------+---------+------+-----------------------+
|7b2b9ee6-b765-4803-8821-b6d94081a815|hello |2026-09-25 18:34:36.061|dummy-topic|0        |6     |2026-09-25 18:34:36.385|
+------------------------------------+------+-----------------------+-----------+---------+------+-----------------------+
```

`run.sh` exists because `mvn exec:java` never forks a separate process, so the `--add-opens` flags Spark
needs on Java 17 have to be set on Maven's own JVM via the `MAVEN_OPTS` environment variable rather than in
`pom.xml` - the script sets that, plus `PATH` and `-Dhadoop.home.dir` for `winutils.exe`, and runs from the
repo root so the relative keytab/truststore/checkpoint paths resolve.

### Checkpointing

`checkpointLocation` (in `spark-streaming.properties`, under `spark-streaming/checkpoint/`) is where a
Structured Streaming query records which Kafka offsets it has already processed, plus other query state.
On every batch it writes there before producing output, so if the job crashes and restarts *pointed at the
same location*, it resumes exactly where it left off instead of reprocessing everything or skipping
records - the same guarantee a consumer group's committed offsets give a regular Kafka consumer, but
tracked entirely on the Spark side.

Setting it explicitly isn't actually mandatory - if you omit `checkpointLocation`, Spark quietly creates a
temporary one under your OS temp directory and deletes it when the query or the `SparkSession` stops. The
query still runs fine either way; the difference only shows up on the *next* run. Set it explicitly (as
every query in this module does) and a restart resumes; leave it out and a restart starts over from
`startingOffsets`, because the thing it would have resumed from no longer exists.

### Java 17 and Spark

Spark 4.2.0 requires Java 17+, matching this project's `java.version`, but on Java 17 it needs
`--add-opens` for several `java.base` packages its (Scala-based) internals access reflectively - without
them it fails at startup with `InaccessibleObjectException`. `run.sh` sets these via `MAVEN_OPTS`. To run
`SparkConsoleApp` from IntelliJ instead, copy the flags out of `run.sh` into the run configuration's VM
options, set its working directory to the repo root (same requirement as the Spring Boot app, see
"Run configurations (IntelliJ)" above), and add `hadoop-local/bin` to its `PATH` environment variable.

## Windowed aggregation, Parquet and Postgres (`SparkAggregationApp`)

`SparkAggregationApp` counts messages per `text` value in 1-minute tumbling windows (`window.duration`),
with a 2-minute watermark (`watermark.delay`) for late-arriving events, and writes those counts to Parquet
and to Postgres. Console keeps working exactly as `SparkConsoleApp` left it - this app just also starts
two more queries alongside it.

If `docker/.env` already existed before this section was added to the repo, it won't have
`POSTGRES_PASSWORD` yet: re-run `bash docker/certs/generate-certs.sh` (this also rotates the Kafka certs,
per its own docs above, so follow it with `docker compose up -d --force-recreate` in `docker/`) once, then:

```bash
bash spark-streaming/run.sh aggregation
```

### Why `text`, not `id`, and why `createdAt`, not Kafka's own timestamp

The schema has exactly three fields: `id` (a random UUID, unique per message), `text` (free-form, whatever
you `POST`), and `createdAt` (when the producer built the message). Grouping by `id` would count exactly 1
in every window, forever - there's nothing to demonstrate. Grouping by `text` is what lets you send the
same value twice and watch the count become 2.

For *event time* - the column the window and watermark are measured against - `createdAt` is the right
choice over the Kafka source's own `timestamp` column (when the broker appended the record). They're
usually only milliseconds apart locally, but they answer different questions: `createdAt` is when the
event actually happened; Kafka's `timestamp` is when it arrived at the pipeline. Late data is about the
gap between those two, so measuring event time against the pipeline's own arrival time would erase the
exact thing this section demonstrates. `LateDataProducer` (below) only works because `createdAt` is
something you can set independently of when the record is actually sent.

### Event time vs. processing time, in plain terms

*Processing time* is the clock on the machine running Spark - "it's 7:45pm here, right now." *Event time*
is a timestamp carried inside the data itself - "this thing happened at 7:40pm," regardless of when Spark
gets around to looking at it. A window defined on processing time groups by when Spark happened to see the
data, which shifts if Spark is slow or a batch is delayed. A window defined on event time (what `window()`
does here, over `createdAt`) groups by when the event actually occurred, which is what you almost always
want for something like "how many things happened per minute" - and it's the only kind of window a
watermark can reason about, since a watermark is itself a statement about event time.

### How the watermark is calculated, and what it protects

Spark computes the watermark as `(the latest event time it has seen so far across the whole stream) -
watermark.delay`. That's it - it's not wall-clock-based at all. Every time a new micro-batch arrives with a
record whose `createdAt` is later than anything seen before, the watermark moves forward by the same
amount. If the stream goes quiet, the watermark freezes wherever it last was, because there's no new
"latest event time" to compute it from.

A row is included in the aggregation only if its event time is at or after the current watermark;
otherwise it's dropped before it ever reaches the `groupBy`. This is exactly what `LateDataProducer`
demonstrates: a message 30 seconds old is still at or after (`latest seen` − 2 minutes), so it's counted;
one 5 minutes old is well before it, so it's silently dropped - not an error, not logged by default, just
absent from the output.

The watermark is also what lets Spark bound its memory: without one, a windowed aggregation would have to
keep state (the running count) for every window that has *ever* opened, forever, since in principle a
record for any of them could still arrive. With a watermark, once it passes a window's end, Spark evicts
that window's state - it can prove no more data for that window is possible - and any further data that
would have belonged to it just gets dropped instead. That eviction is also *why* append mode is legal for
the Parquet sink at all: Spark only writes a window's final row once it evicts that window's state, so
each window is written exactly once, never revised.

### Output modes, and why each sink needs a different one

- **Parquet → `append`.** A file sink can only add new files, never rewrite one already written, so a
  window's row can only be emitted once, ever. Append-mode is only *legal* on a streaming aggregation
  because there's a watermark - without one, Spark has no way to know a window is "done," and refuses to
  start the query at all rather than risk writing an incomplete row and never being able to correct it.
- **Postgres → `update`.** `foreachBatch` doesn't have the file sink's constraint - each batch just gets
  handed a `Dataset<Row>` and you decide what to do with it - so `update` mode is available: every batch,
  Spark hands you whichever windows' counts *changed* in that batch, including windows still accumulating.
  That's more useful here: `topic_counts` shows a window's count rising in near-real-time, not just a
  single final value ~2 minutes after the fact. The `ON CONFLICT ... DO UPDATE` upsert (below) is what
  makes writing the same window repeatedly, as its count keeps changing, safe.
- **Console → `append`** (unchanged from `SparkConsoleApp`): there's no aggregation in that query at all,
  just a row-for-row pass-through, so append (emit each new row once) is the only mode that makes sense.

### `foreachBatch`, replays, and why the Postgres write has to be idempotent

Spark has no built-in streaming JDBC sink, so `SparkAggregationApp` uses `foreachBatch`: once per
micro-batch, Spark hands the batch's `Dataset<Row>` to a callback, and what that callback does with it is
entirely your own code - here, `upsertBatch` opens a JDBC connection per partition and executes:

```sql
INSERT INTO topic_counts (window_start, window_end, text, count) VALUES (?, ?, ?, ?)
ON CONFLICT (window_start, window_end, text) DO UPDATE SET count = EXCLUDED.count
```

Structured Streaming's fault-tolerance guarantee for `foreachBatch` is **at-least-once, not
exactly-once** - and this is *why*: Spark's checkpoint and Postgres are two separate systems with no shared
transaction between them. If the process crashes after the JDBC write commits but before the checkpoint
records that batch as done, Spark has no way to know the write actually happened, so on restart it reruns
that exact batch. Without the upsert, that replay would double the count; with it, the replay just writes
the same `(window_start, window_end, text, count)` row again, and `DO UPDATE SET count = EXCLUDED.count`
overwrites it with the same value - harmless. The primary key on `(window_start, window_end, text)` (see
`docker/postgres/init-topic-counts.sql`) is what the upsert's `ON CONFLICT` targets, so it's also what
makes that triple this table's idempotency key.

### One app, three queries - and why "reads Kafka independently" is true even so

`SparkAggregationApp` starts all three queries (console, Parquet, Postgres) on one `SparkSession`, rather
than three separate job classes. Since each query manages its own Kafka consumer and its own checkpoint no
matter what, splitting into separate processes wouldn't buy any isolation this design doesn't already
have - it would just mean three JVMs and three things to start instead of one. The genuinely
counterintuitive part: `windowedCounts` (the aggregated `Dataset`) is built **once** in the Java code and
handed to *two* `.writeStream()` calls (Parquet and Postgres). It's tempting to assume that means Kafka
gets read once and the result fans out to both sinks - it doesn't. Every `.start()` call creates a fully
independent streaming query with its own execution plan, including its own instantiation of the Kafka
source, *regardless* of whether it was built from a `Dataset` object another query also uses. Reusing the
Java object saves writing the same `groupBy`/`window` code twice; it does not save a Kafka read. Between
`SparkConsoleApp`'s own read, `SparkAggregationApp`'s console query, and its Parquet and Postgres queries,
that's up to four independent reads of `dummy-topic` if everything is running at once.

### Triggers

A trigger controls how often Spark checks the source for new data and runs a micro-batch. With no trigger
specified (as in `SparkConsoleApp`), Spark runs the next batch immediately after the previous one finishes
- as fast as it can. `Trigger.ProcessingTime(trigger.interval)`, used for the Parquet and Postgres queries
here, instead runs a batch on a fixed cadence (`trigger.interval`, 10 seconds by default): if a batch
finishes early, Spark waits out the rest of the interval; if it runs long, the next batch starts
immediately after, and Spark logs a "falling behind" warning rather than piling up concurrent batches.

### Demonstrating late data

```bash
bash spark-streaming/run.sh late-data counted_late 30     # ~30s old: inside the watermark, gets counted
bash spark-streaming/run.sh late-data dropped_too_old 300  # 5 minutes old: outside it, silently dropped
```

`LateDataProducer` builds the Confluent wire format by hand (same as `KafkaAvroSource` strips on the way
in) and publishes directly to `dummy-topic` with a `createdAt` you choose, bypassing the Spring Boot
producer's `Instant.now()` entirely. Its `<text>` argument can't contain spaces (Maven's `exec.args`
splits on whitespace with no quoting) - use underscores as above.

The console query prints both records regardless - it has no aggregation or watermark, so it shows every
raw message unconditionally. The difference only shows up in the aggregated output: query
`topic_counts` (below) or inspect the Parquet output, and `counted_late` is there while `dropped_too_old`
never appears, in either sink.

### Querying Postgres

```bash
docker exec kafka-postgres psql -U spark -d dummy_topic_counts -c "SELECT * FROM topic_counts ORDER BY window_start, text;"
```

Re-run it a few seconds apart while messages are flowing and you can watch a window's `count` climb in
`update` mode, then stop changing once the watermark passes that window's end.

### Inspecting the Parquet output

```bash
bash spark-streaming/run.sh parquet-inspect
```

`ParquetInspector` is a plain batch (non-streaming) read of `parquet.output.path`, so this stays Java-only
rather than requiring pandas/DuckDB/etc. just to look at the data. Remember append-mode's implication: a
window you just triggered won't appear here (or in Postgres, for that matter, since Postgres output for
that window is still climbing) until the watermark has actually passed its end - for the defaults here,
usually a few minutes after the messages that filled it were sent, not immediately.

## Reliability

### Idempotent producer

Both producers - the Spring Boot `DummyProducer` (`application.yml`) and `LateDataProducer`
(`spark-streaming.properties`, `producer.*` keys) - set these explicitly:

| Setting | Value | Why it's there |
|---|---|---|
| `acks` | `all` | The partition leader only confirms a write once every in-sync replica has it. |
| `enable.idempotence` | `true` | The broker drops duplicates caused by the producer's own retries. |

Both have been the client defaults since Kafka 3.0 (KIP-679), so behaviour didn't change; being explicit
matters because of a quiet rule in the client: if you set a config that conflicts with idempotence
(`acks=1`, `retries=0`, or more than 5 in-flight requests) *without* explicitly enabling idempotence,
the client just turns idempotence off. With `enable.idempotence=true` set explicitly, the same conflict
fails at startup with a `ConfigException` instead.

The related settings are left at their defaults, which are what idempotence expects:
`retries` = `2147483647`, `max.in.flight.requests.per.connection` = `5` (idempotence requires ≤ 5),
`delivery.timeout.ms` = `120000`. With retries effectively unlimited, `delivery.timeout.ms` is the real
limit: the total time a `send()` may spend retrying before it reports failure.

Spring Boot has a dedicated property for `acks` (`spring.kafka.producer.acks`) but not for idempotence,
so that one goes in the pass-through map: `spring.kafka.producer.properties.enable.idempotence`. To confirm
what the client actually ended up with, look for the `ProducerConfig values:` block the Kafka client logs
when it creates a producer. Spring creates it lazily, so that's on the first `send()` (your first `POST`),
not at application startup:

```
	acks = -1                        <- "all" is logged as -1
	enable.idempotence = true
	max.in.flight.requests.per.connection = 5
	retries = 2147483647
```

followed by `Instantiated an idempotent producer.`

**What idempotence does, in plain terms.** Each producer gets a producer ID from the broker, and numbers
every batch it sends to each partition (a sequence number). If a send times out and the producer retries,
but the first attempt had actually been written, the broker sees a sequence number it already has and
discards the copy. It also rejects out-of-order sequences, which is what keeps retries from reordering
records even with 5 requests in flight.

**What it doesn't protect against.** That dedup only works *within one producer session*, per partition.
If the app restarts, it gets a new producer ID and sequence numbers start over, so the broker has no way
to tell a resent record from a new one. The same goes for application-level resends: if a caller `POST`s
the same text twice, or code calls `send()` twice for the same thing, those are two different records as
far as Kafka is concerned. Deduplicating those needs a business key on the consumer side (or the
transactions covered later).

### What one broker can and can't show

This stack runs a single broker, and every topic has replication factor 1.

- **`acks=all` on one broker behaves exactly like `acks=1`.** "All in-sync replicas" is just the leader,
  because the leader is the only replica. The setting is correct and future-proof, but it adds no
  durability here.
- **Replication factor can't exceed the number of brokers**, so RF=1 is the ceiling. If the broker's
  disk is lost, the data is gone, and while the broker is down nothing can be read or written.
- **`min.insync.replicas`** (default 1) is the minimum number of in-sync replicas an `acks=all` write
  needs. What it's *for* - refusing writes rather than accepting them onto too few copies - only has
  meaning with more replicas. On one broker you can't even force it: see below.

A 3-broker cluster with RF=3 and `min.insync.replicas=2` is the usual production setup: every
acknowledged write is on at least 2 brokers, one broker can be down (or restarting for an upgrade) with no
errors and no data loss, and if two are down, `acks=all` writes are refused with `NotEnoughReplicas`
rather than written to a single copy. Leader failover - another replica taking over the partition - also
needs more than one broker to demonstrate.

**Why you can't trigger `NotEnoughReplicas` on one broker, even on purpose.** Kafka lets you set
`min.insync.replicas` higher than a topic's replication factor, but the broker doesn't enforce the
impossible value: it uses `min(replication factor, min.insync.replicas)` (`effectiveMinIsr` in the
broker's `Partition.scala`, present in the 3.8.0 broker used here). So an RF=1 topic with
`min.insync.replicas=2` is treated as min ISR 1, and `acks=all` writes to it succeed. This was tried
against this stack: creating such a topic works, and an `acks=all` console-producer write to it goes
through with no error. The refusal only happens when a topic has *more* replicas than are currently in
sync, which needs followers, which needs more than one broker.

**What you'd see in a real cluster, and the timeout trap.** `NotEnoughReplicasException` is a
*retriable* error in the Kafka client, because a lagging replica usually catches up within seconds. So the
producer doesn't fail right away: it logs `NOT_ENOUGH_REPLICAS` warnings and keeps retrying until
`delivery.timeout.ms` (default 2 minutes) runs out, then reports the failure - either as
`NotEnoughReplicasException` or as a `TimeoutException` ("Expiring 1 record(s)"), depending on whether
time ran out during a request or while waiting to retry. From the application's side, that looks like a
2-minute hang, not an error.

**Harmless CLI warning.** Any Kafka CLI command run against this stack (including `init-topics.sh`) ends
with `WARN ... TGT renewal thread has been interrupted and will exit`. That's the Kerberos login's
background ticket-renewal thread being stopped as the short-lived CLI process exits - not an error.

### ACLs for idempotence

None needed beyond what `client` already has. Since Kafka 2.8 (KIP-679), `Write` on a topic is enough for
an idempotent producer. The older cluster-level `IdempotentWrite` permission isn't required, and the
broker here is 3.8. Transactions (added later) are different: they need permissions on a
`TransactionalId` resource.

### Client and broker versions

The three Kafka components here are on three different versions:

| Component | Kafka version |
|---|---|
| Broker (`apache/kafka` image) | 3.8.0 |
| Spring Boot app (`kafka-clients`, managed by Spring Boot 4.1.1) | 4.2.1 |
| `spark-streaming/` (`kafka-clients`, pinned to what Spark 4.2.0 resolves) | 3.9.2 |

This is fine, and normal. Kafka clients and brokers negotiate on connect (the `ApiVersions` request)
and each side uses the newest version of each request type both support, so a newer client works against
an older broker and the other way round, within the supported range. Kafka 4.x clients need a 2.1+
broker, and 3.8 is well above that. The catch is that **a feature only works if the broker supports it**, whatever
the client version. For example, Kafka 4's new consumer group protocol (KIP-848, `group.protocol=consumer`)
needs a 4.0+ broker, so against this 3.8 broker the consumers use the classic protocol. When a Kafka
feature's docs say "since version X", check it against the **broker** version (3.8) as well as the client's.

### Retries and the dead-letter topic

When `DummyConsumer.listen` throws, Spring hands the record to an **error handler**, which decides between
three outcomes: try the same record again, give up and park it on a **dead-letter topic (DLT)**, or both
(retry a few times, then park it). Parking it matters because a Kafka consumer reads each partition in
order: until a record is either processed or moved out of the way, nothing behind it on that partition
gets processed either.

#### Blocking vs. non-blocking retries

Spring Kafka offers two styles:

| | **Blocking** (`DefaultErrorHandler` + `BackOff`) - used here | **Non-blocking** (`@RetryableTopic`) |
|---|---|---|
| How a retry happens | The consumer seeks back to the failed record, sleeps for the back off, and redelivers it. | The failed record is published to a retry topic named after its delay (e.g. `dummy-topic-retry-1000`) and its offset committed; a consumer on the retry topic delivers it again once its delay is up. |
| Other records on the same partition | Wait until the failed one is done (processed or dead-lettered). | Keep flowing; only the failed record is delayed. |
| Ordering | **Preserved.** Nothing overtakes a failing record. | **Lost** for failed records: later records are processed before an earlier one's retry. |
| Extra topics | Just the DLT. | One retry topic per delay level (or one with a fixed delay), plus the DLT, plus ACLs for each. |
| Good for | Short, bounded retries where order matters. | Long delays (minutes) where holding up the partition is worse than reordering. |

This project uses **blocking retries**. The back off is short (seconds), `DummyConsumer` has no reason to
accept reordering (Spring's docs: with non-blocking retries "you lose Kafka's ordering guarantees for that
topic"), and while Spring would normally create the retry topics itself, `client` has no `Create` ACL on this
broker, so each one would have to be provisioned in `init-topics.sh`. The cost is head-of-line
blocking: while one record is being retried, the rest of its partition waits.

#### Which failures are retried

`KafkaErrorHandlingConfig` retries **everything except** failures that can't be fixed by trying again:

| Failure | Retried? | Why |
|---|---|---|
| `DeserializationException` (bytes that aren't valid Avro) | No - straight to the DLT | The bytes won't change. Built into Spring's not-retryable list, along with `MessageConversionException`, `ConversionException`, `MethodArgumentResolutionException`, `NoSuchMethodException` and `ClassCastException`. |
| `InvalidMessageException` (decoded fine, breaks a business rule) | No - straight to the DLT | The record won't change either. Added with `addNotRetryableExceptions`. |
| `RetryableProcessingException`, or anything else | Yes, with back off | Might be a temporary problem (a downstream timeout, a lock). |

Spring checks the exception's *causes* too, so it doesn't matter that the listener's exception arrives
wrapped in a `ListenerExecutionFailedException`.

#### Back off, and the worst-case time

`app.kafka.retry` in `application.yml`: first wait 1s, doubling each time, **capped at 4s**, at most 4
retries - so 5 attempts, with waits of **1s, 2s, 4s, 4s = 11s of back off in total**. Without the cap
(`max-interval-ms`), the 4th wait would be 8s, and each extra retry would double it again.

Why the cap matters: Kafka's `max.poll.interval.ms` (default 300s) is the longest a consumer may go between
`poll()` calls before the broker decides it's stuck, kicks it out of the group and rebalances. Spring's
default back off handler **sleeps the consumer thread**, so back off time counts toward that limit. The worst
case for one failing record, from its first failure to its offset being committed:

| Step | Worst case |
|---|---|
| 5 attempts of `DummyConsumer.listen` | ~milliseconds each here |
| Back off between them | 11s |
| Publishing to the DLT | up to 125s, *only if the broker is unreachable*: the recoverer waits for the send result for `delivery.timeout.ms` (120s) + 5s. Normally milliseconds. |
| **Total** | **~136s worst case, ~11s normally** - against a 300s limit |

That's the pessimistic reading, counting the whole sequence as one gap between polls. In practice
`DefaultErrorHandler` seeks back and returns to `poll()` after every failed attempt, so each individual gap is
at most one back off (4s) plus processing - except the last, which includes the DLT publish. If you ever
need waits longer than `max.poll.interval.ms`, that's what `@RetryableTopic`, or Spring's
`ContainerPausingBackOffHandler`, is for.

#### The dead-letter topic

`DeadLetterPublishingRecoverer` publishes the failed record to `dummy-topic-dlt` (`app.kafka.dlt-topic`),
**to the same partition number** it came from, and adds headers recording what happened:

| Header | Content |
|---|---|
| `kafka_dlt-original-topic` / `-partition` / `-offset` / `-timestamp` | Where the record originally was. Partition and offset are binary (4-byte int, 8-byte long). |
| `kafka_dlt-original-consumer-group` | The group that failed to process it (`dummy-consumer-group`). |
| `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn` | The exception class, and its cause's. For processing failures the outer one is Spring's `ListenerExecutionFailedException`; the cause is yours. |
| `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace` | The message and full stack trace. |

The record's own key, value and headers are kept. The DLT therefore holds **two kinds of value**:
- **Processing failures** (`fail-always`, `fail-invalid`) had already been decoded into a `DummyMessage`, so
  they're re-encoded as Avro. The Avro serializer registers a new Schema Registry subject,
  `dummy-topic-dlt-value`, the first time this happens.
- **Deserialization failures** (poison pills) never became a `DummyMessage`. `ErrorHandlingDeserializer`
  saves the original bytes in a header, and the recoverer writes **those exact bytes** as the DLT value,
  through a `byte[]` producer (`RawBytesProducer`). `KafkaErrorHandlingConfig` maps value types to
  producers so each kind gets the right serializer.

`DeadLetterInspector` reads the DLT as raw bytes (so it can handle both kinds), tries to decode each value as
Avro, and logs one line per dead letter. It never throws: it uses the same error handler, so a failure in it
would dead-letter a dead letter.

Nothing re-processes the DLT automatically. That's deliberate - a DLT is for a person (or a separate, careful
tool) to look at, fix the cause, and decide whether to replay.

```
 dummy-topic ─► DummyConsumer.listen()
                     │
         succeeded ──┼──► offset committed, next record
                     │
             threw   ▼
          DefaultErrorHandler: what kind of exception?
             │                                   │
             │ not retryable                     │ retryable
             │ (DeserializationException,        │ (RetryableProcessingException,
             │  InvalidMessageException, ...)    │  anything else)
             │                                   ▼
             │                        retries left? ── yes ──► sleep 1s / 2s / 4s / 4s,
             │                                   │              redeliver the same record
             │                                   │ no           (rest of the partition waits)
             ▼                                   ▼
          DeadLetterPublishingRecoverer ──► dummy-topic-dlt, same partition #, + kafka_dlt-* headers
             │                                   │
             ▼                                   ▼
   offset committed, next record       DeadLetterInspector logs it
```

#### Triggering each path

If your Docker stack was created before the DLT existed, create the new topic and ACLs first, from `docker/`:

```powershell
docker compose up topic-init
```

Then, with the app running, from PowerShell:

```powershell
$u = "http://localhost:8080/dummy-topic"
Invoke-WebRequest -Uri "$u/messages"    -Method POST -ContentType "text/plain" -Body "fail-transient-1"  # fails twice, then succeeds
Invoke-WebRequest -Uri "$u/messages"    -Method POST -ContentType "text/plain" -Body "fail-always-1"     # 5 attempts over ~11s, then DLT
Invoke-WebRequest -Uri "$u/messages"    -Method POST -ContentType "text/plain" -Body "fail-invalid-1"    # 1 attempt, then DLT
Invoke-WebRequest -Uri "$u/poison-pill" -Method POST -ContentType "text/plain" -Body "not avro"          # not Avro at all: DLT
```

The markers are prefixes (`app.kafka.failure-demo.*`), so add a suffix to tell attempts apart. What to look
for in the app's log:

| Sent | Log |
|---|---|
| `fail-transient-1` | Two `FailureDemo : Simulating a transient failure ... attempt 1 of 2` / `attempt 2 of 2` warnings, about 1s and 2s apart, then `DummyConsumer : Received message ... (delivery attempt 3)`. |
| `fail-always-1` | Five `Simulating a failure that never recovers ... attempt N` warnings over ~11s, then `DeadLetterInspector : Dead letter at dummy-topic-dlt-0@... from dummy-topic-0@... failed with ...RetryableProcessingException: Simulated permanent outage ... (attempt 5) \| value: Avro {...}`. |
| `fail-invalid-1` | No retries: straight to `Dead letter ... failed with ...InvalidMessageException: Simulated validation failure ...`. |
| poison pill | The endpoint answers `sent raw bytes to dummy-topic partition 0 offset N`, then `Dead letter ... from dummy-topic-0@N ... failed with org.springframework.kafka.support.serializer.DeserializationException: failed to deserialize \| value: 8 raw bytes, not decodable as Avro: hex 6e6f74206176726f / text "not avro"`. |

To see the headers exactly as stored, read the DLT with the console consumer (from `docker/`):

```powershell
docker compose run --rm --no-deps --entrypoint /opt/kafka/bin/kafka-console-consumer.sh topic-init `
  --bootstrap-server kafka:9095 --consumer.config /etc/kafka/secrets/admin-sasl-ssl.properties `
  --topic dummy-topic-dlt --from-beginning --timeout-ms 10000 `
  --property print.headers=true --property print.offset=true
```

Text headers print as text. `kafka_dlt-original-partition` and `-offset` are binary numbers, so they show
up as unreadable characters there; `DeadLetterInspector`'s log line decodes them.

To turn the demo triggers off (the markers become ordinary text, and the poison-pill endpoint disappears),
set `app.kafka.failure-demo.enabled=false`.

### Poison pills and the Spark jobs

The poison pill goes to `dummy-topic`, which both Spark apps also read. They don't use Spring's error
handler, so they need their own defence.

**What would happen without one.** `from_avro` defaults to `FAILFAST`: a record it can't decode throws,
which fails the whole micro-batch, which stops the query. Because the batch failed, the checkpoint never
records it as done, so restarting the job re-reads exactly the same batch, hits the same record, and fails
again, forever. The only ways out would be deleting the checkpoint (reprocessing everything, or skipping
everything with `startingOffsets=latest`) or hand-editing it past the bad offset.

**What `KafkaAvroSource` does instead**, before any app-specific logic sees the data:
1. **Header check.** A Confluent Avro record is at least 5 bytes and starts with magic byte `0`. Anything
   else (the poison pill's text, a null tombstone) is skipped without calling `from_avro` at all.
2. **`from_avro` in `PERMISSIVE` mode.** A payload that has the header but won't parse no longer throws.
   One subtlety, found while testing this: for a record schema, `PERMISSIVE` doesn't return a null
   struct, it returns a struct with *every field* null. So the filter checks that `id` - a required field in
   `DummyMessage.avsc` - is non-null, not just that the struct is.
3. **Counting.** `Dataset.observe` attaches a `received` / `skipped` count to every micro-batch, and a
   `StreamingQueryListener` logs a warning for any batch that skipped something:
   `WARN KafkaAvroSource: Query ... batch N: skipped 1 of 1 Kafka records that aren't valid Confluent Avro`.
   The same numbers appear under `observedMetrics` in each query's progress (`query.lastProgress()`).

Skipped records are dropped, not dead-lettered: the Spark apps only read `dummy-topic`, and the Spring
app's DLT already has a copy of anything that failed to deserialize there.

**What this can't catch.** Avro binary isn't self-describing: it's just values back to back, and the
schema says how to read them. So a payload that starts with `0` + a schema ID and whose remaining bytes
*happen* to parse against the schema decodes "successfully" into nonsense. For example,
`00 00 00 00 01 02 61 02 62 00` decodes as `id="a", text="b", createdAt=1970-01-01`. Nothing can detect
that from the bytes alone; only validation of the decoded values could.

## Project layout

```
docker/
  docker-compose.yml        KDC, Kafka broker (KRaft), Schema Registry and Postgres
  init-topics.sh            creates dummy-topic, dummy-topic-dlt, _schemas and the ACLs (idempotent; run by the topic-init service)
  certs/                    generate-certs.sh; the CA, keystores and truststore it creates are git-ignored
  .env.example              the variable names of docker/.env (the real, generated .env is git-ignored)
  kerberos/                 KDC image, krb5 configs, keytabs (generated), KDC database (generated)
  postgres/init-topic-counts.sql   creates topic_counts; run once by the postgres image on first startup
src/main/avro/              Avro schema
src/main/java/...           controllers, producer, consumer, error handling / DLT
src/test/                   unit, web-layer and embedded-Kafka tests
spark-streaming/             standalone Maven project, not a module of the root pom.xml
  pom.xml                    Spark 4.2.0, spark-sql-kafka-0-10, spark-avro, postgresql JDBC driver
  setup-windows-hadoop.sh    one-time, Windows only: downloads winutils.exe/hadoop.dll
  run.sh                     picks the app, sets MAVEN_OPTS (add-opens, hadoop.home.dir), always recompiles
  src/main/resources/        spark-streaming.properties: bootstrap servers, topic, security, window/watermark
  src/main/java/...          SparkConsoleApp, SparkAggregationApp, LateDataProducer, ParquetInspector,
                             KafkaAvroSource, SchemaRegistry, ConfigLoader
  hadoop-local/               downloaded winutils.exe/hadoop.dll, git-ignored
  checkpoint/                 one subdirectory per query's Structured Streaming checkpoint, git-ignored
  output/                     Parquet output (dummy-topic-counts), git-ignored
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `docker version` says the daemon isn't running | Docker Desktop hasn't finished starting. If it never does, check `wsl -l -v`. A `docker-desktop` distro stuck in *Uninstalling* means running `wsl --shutdown`, then `wsl --unregister docker-desktop` (this keeps images and volumes), then relaunching Docker Desktop. |
| `kafka-kdc` exits with `Cannot open DB2 database ... File exists` | The KDC stash file is missing. `kdc.conf` now keeps it in the bind mount (`key_stash_file`). If the stash is lost, regenerate it: `docker compose run --rm --no-deps --entrypoint sh kdc -c 'kdb5_util -r EXAMPLE.COM stash -P "$KDC_MASTER_PASSWORD"'`. |
| `docker compose` reports a required variable is missing, or the app fails with `Could not resolve placeholder 'KAFKA_TRUSTSTORE_PASSWORD'` | `docker/.env` doesn't exist yet. Run `bash docker/certs/generate-certs.sh`. |
| `kafka-schema-registry` exits with `TopicAuthorizationException`, or never starts | The registry starts only after `kafka-topic-init` succeeds. Check that job with `docker logs kafka-topic-init`. Once it's fixed, `docker compose up -d` again. |
| App fails with a keytab / keystore "file not found" | The run configuration's working directory isn't the project root. |
| App can't authenticate (`Cannot locate KDC`, `Server not found in Kerberos database`) | The KDC isn't running, or the app isn't using `docker/kerberos/krb5-client.conf`. Check `docker compose ps`. |
| Docker commands from Git Bash mangle paths | Use PowerShell, or set `MSYS_NO_PATHCONV=1` and add Docker's `resources/bin` folder to `PATH`. |
| Spark job fails with `HADOOP_HOME and hadoop.home.dir are unset`, or `Could not locate Hadoop executable: .../winutils.exe` | Windows only. Run `bash spark-streaming/setup-windows-hadoop.sh`, then use `spark-streaming/run.sh` (or copy its `MAVEN_OPTS`/`PATH` setup into your run configuration) rather than a bare `mvn exec:java`. |
| Spark job fails with `InaccessibleObjectException` | Missing `--add-opens` flags for Java 17. Use `spark-streaming/run.sh`, which sets them via `MAVEN_OPTS`, rather than a bare `mvn exec:java`. |
| `spark-streaming/run.sh` fails with `Fatal error compiling: error: invalid target release: 17`, or paths in its own output look like `/mnt/c/Users/...` | Typing `bash ...` inside PowerShell often launches WSL's `bash.exe` instead of Git Bash's, since WSL's usually sits earlier on `PATH` - and WSL has its own separate Java/Maven install. `run.sh` detects this and exits with a pointer to open Git Bash directly, but if you ever bypass it: open Git Bash itself (Start menu, or right-click the folder > "Git Bash Here") rather than typing `bash` from PowerShell or cmd. |
| `SparkAggregationApp`'s Parquet/Postgres batches take minutes instead of seconds, with repeated `WARN HDFSBackedStateStoreProvider ... partitionId=<large number>` lines | An existing checkpoint from before `spark.sql.shuffle.partitions` was tuned down (see Design notes) still has state for the old, much larger partition count. Delete `spark-streaming/checkpoint/dummy-topic-counts-parquet` and `.../dummy-topic-counts-postgres` (and `TRUNCATE topic_counts` if you want a clean table) and restart. |
| A failing record never reaches the DLT; the app logs `TopicAuthorizationException` or `UNKNOWN_TOPIC_OR_PARTITION` for `dummy-topic-dlt` | The Docker stack predates the DLT, so the topic and its ACLs don't exist yet. From `docker/`, run `docker compose up topic-init`. |
| `kafka-postgres` exits immediately with a message about `pg_ctlcluster`-style directories and an "unused mount/volume" | The postgres image version in use expects its volume mounted at `/var/lib/postgresql`, not `/var/lib/postgresql/data` (this changed in the postgres:18 image; `docker-compose.yml` already reflects it - relevant only if you change the image tag). |

## Design notes

- Use `spring-boot-starter-kafka`, not raw `spring-kafka`. This Spring Boot version needs the starter to
  auto-configure a `KafkaTemplate`.
- Don't add `spring-boot-devtools`. Its restart classloader breaks Avro deserialization (the generated class is
  loaded twice).
- The consumer's value deserializer is wrapped in Spring's `ErrorHandlingDeserializer` (delegating to
  `KafkaAvroDeserializer`). Without the wrapper, one malformed record makes the consumer retry the same offset
  forever and block every message behind it. With it, the bad record goes to `dummy-topic-dlt` straight away
  (with its original bytes), because Spring treats a `DeserializationException` as non-retryable. See
  "Poison pills" under Reliability.
- Don't declare a `KafkaTemplate` or `ProducerFactory` bean of your own. Spring Boot only auto-configures its
  (Avro) ones when none exist, so a second one silently removes the template `DummyProducer` relies on.
  `RawBytesProducer` wraps its `byte[]` template instead of exposing it as a bean for that reason.
- Avro strings decode as `org.apache.avro.util.Utf8`, so call `.toString()` when comparing them to a `String`.
- `spark-streaming/run.sh` always runs `mvn ... compile exec:java`, never a bare `exec:java`. Maven only
  runs the goals you ask for - `exec:java` alone is a single goal, not the default lifecycle, so it will
  silently execute whatever's already in `target/classes` even after you've edited and saved a `.java`
  file, with no warning that anything's stale.
- `SparkAggregationApp` sets `spark.sql.shuffle.partitions` to 4. Spark's default (200) is sized for a
  cluster; for a handful of test rows in `local[*]`, it just means scheduling 200 mostly-empty tasks (and,
  per-partition, opening a JDBC connection for any that aren't) every batch instead of a handful.
