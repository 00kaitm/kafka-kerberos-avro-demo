#!/bin/bash
# Windows-only, first-time setup. Run from Git Bash or any bash shell.
#
# Spark's Structured Streaming checkpointing goes through Hadoop's local filesystem code, which
# on Windows shells out to a winutils.exe helper for basic POSIX-style operations (chmod, mkdir
# permissions) that Hadoop assumes are available natively on Linux/macOS. Without it, the job
# fails immediately with "HADOOP_HOME and hadoop.home.dir are unset" or, once that's set,
# "Could not locate Hadoop executable: .../winutils.exe".
#
# This downloads a prebuilt winutils.exe + hadoop.dll from cdarlint/winutils, the de facto
# community-maintained build most Spark-on-Windows setup guides point to (the original
# steveloughran/winutils repo stopped publishing new versions). It's a third-party binary the
# project runs locally, not something from Maven Central, so this is a deliberate, visible step
# rather than an automatic download.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

HADOOP_BUILD_VERSION="3.3.6"
BASE_URL="https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-${HADOOP_BUILD_VERSION}/bin"

mkdir -p hadoop-local/bin
for file in winutils.exe hadoop.dll; do
  if [ -f "hadoop-local/bin/$file" ]; then
    echo "hadoop-local/bin/$file already present, skipping."
    continue
  fi
  echo "Downloading $file (Hadoop $HADOOP_BUILD_VERSION build)..."
  curl -fSL -o "hadoop-local/bin/$file" "$BASE_URL/$file"
done

echo "Done. hadoop-local/bin/ now has:"
ls -la hadoop-local/bin/
