#!/bin/bash
# Idempotent provisioning of the topics and ACLs the app and Schema Registry need,
# run as the super-user "kafka" principal.
#
# docker compose runs this automatically as the one-shot "topic-init" service, and
# the Schema Registry only starts once it has finished. To run it again by hand:
#   docker compose up topic-init
set -euo pipefail

# Inside the compose network the broker is reached on its internal listener.
BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9094}
CONFIG=/etc/kafka/secrets/admin-sasl-ssl.properties
TOPICS=/opt/kafka/bin/kafka-topics.sh
ACLS=/opt/kafka/bin/kafka-acls.sh

topics() { "$TOPICS" --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" "$@"; }
acls()   { "$ACLS"   --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" "$@"; }

echo "Waiting for the broker..."
for _ in $(seq 1 60); do
  if topics --list >/dev/null 2>&1; then break; fi
  sleep 2
done
topics --list >/dev/null

topics --create --if-not-exists --topic dummy-topic --partitions 1 --replication-factor 1
topics --create --if-not-exists --topic _schemas --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact

# App client: produce to and consume from dummy-topic.
acls --add --allow-principal User:client \
  --operation Write --operation Read --operation Describe --topic dummy-topic
acls --add --allow-principal User:client --operation Read --group dummy-consumer-group

# Schema Registry: owns the _schemas topic.
acls --add --allow-principal User:registry \
  --operation Write --operation Read --operation Describe \
  --operation DescribeConfigs --operation AlterConfigs --topic _schemas
acls --add --allow-principal User:registry --operation Read --group schema-registry

echo "Topics and ACLs are in place."
