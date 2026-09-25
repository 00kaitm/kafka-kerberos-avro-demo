#!/bin/bash
# Runs the Spark console job. Run from Git Bash, from the repo root or from spark-streaming/:
#   bash spark-streaming/run.sh
#
# exec-maven-plugin's `java` goal never forks a separate process, so the JVM flags Spark needs on
# Java 17 (see README: "Java 17 and Spark") have to land on Maven's own JVM via MAVEN_OPTS, and
# the working directory has to be the repo root so the relative keytab/truststore/checkpoint
# paths in spark-streaming.properties resolve, same convention application.yml uses.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HADOOP_LOCAL="$SCRIPT_DIR/hadoop-local"

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
mvn -f spark-streaming/pom.xml exec:java
