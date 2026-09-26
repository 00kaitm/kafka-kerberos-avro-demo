#!/bin/bash
# Runs one of this module's apps. Run from Git Bash, from the repo root or from spark-streaming/:
#   bash spark-streaming/run.sh                                  # console (default)
#   bash spark-streaming/run.sh aggregation                      # console + Parquet + Postgres
#   bash spark-streaming/run.sh late-data "some text" 30         # backdated test message
#   bash spark-streaming/run.sh parquet-inspect                  # batch-read the Parquet output
#
# late-data's <text> is passed through Maven's exec.args, which splits on whitespace with no
# quoting, so keep it to one word (use underscores/hyphens instead of spaces).
#
# exec-maven-plugin's `java` goal never forks a separate process, so the JVM flags Spark needs on
# Java 17 (see README: "Java 17 and Spark") have to land on Maven's own JVM via MAVEN_OPTS, and
# the working directory has to be the repo root so the relative keytab/truststore/checkpoint
# paths in spark-streaming.properties resolve, same convention application.yml uses.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HADOOP_LOCAL="$SCRIPT_DIR/hadoop-local"

# On Windows, typing `bash` in PowerShell/cmd often launches WSL's bash.exe (from
# C:\Windows\System32) rather than Git Bash's, since it usually sits earlier on PATH. WSL has its
# own separate Java/Maven install, so this fails confusingly (e.g. "invalid target release: 17")
# instead of a clear error, unless we check for it - a repo root path under /mnt/c/... is WSL's
# view of the Windows filesystem, so that's the signal.
case "$REPO_ROOT" in
  /mnt/*)
    echo "This looks like WSL's bash, not Git Bash (repo root resolved to $REPO_ROOT)." >&2
    echo "Open Git Bash directly (Start menu, or right-click the folder > 'Git Bash Here') and run this from there instead." >&2
    exit 1
    ;;
esac

APP="${1:-console}"
shift || true

case "$APP" in
  console)          MAIN_CLASS=com.practice.sparkstreaming.SparkConsoleApp ;;
  aggregation)      MAIN_CLASS=com.practice.sparkstreaming.SparkAggregationApp ;;
  late-data)        MAIN_CLASS=com.practice.sparkstreaming.LateDataProducer ;;
  parquet-inspect)  MAIN_CLASS=com.practice.sparkstreaming.ParquetInspector ;;
  *)
    echo "Unknown app: $APP (expected console, aggregation, late-data, or parquet-inspect)" >&2
    exit 1
    ;;
esac

if [ ! -f "$HADOOP_LOCAL/bin/winutils.exe" ]; then
  echo "Missing $HADOOP_LOCAL/bin/winutils.exe - run: bash spark-streaming/setup-windows-hadoop.sh" >&2
  exit 1
fi

export PATH="$HADOOP_LOCAL/bin:$PATH"
export MAVEN_OPTS="-Dhadoop.home.dir=$HADOOP_LOCAL -Djava.library.path=$HADOOP_LOCAL/bin \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
-Djdk.reflect.useDirectMethodHandle=false"

cd "$REPO_ROOT"
# compile explicitly: a bare `exec:java` is a single goal, not the default lifecycle, so it
# never recompiles on its own and will silently run stale classes after an edit.
if [ "$#" -gt 0 ]; then
  mvn -f spark-streaming/pom.xml compile exec:java "-Dexec.mainClass=$MAIN_CLASS" "-Dexec.args=$*"
else
  mvn -f spark-streaming/pom.xml compile exec:java "-Dexec.mainClass=$MAIN_CLASS"
fi
