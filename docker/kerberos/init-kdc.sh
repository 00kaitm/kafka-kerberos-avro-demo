#!/bin/bash
# Local dev KDC bootstrap: creates the realm and principals (the Kafka broker,
# a client, and the Schema Registry), exports their keytabs to a shared volume,
# then starts krb5kdc in the foreground. The master password comes from the
# environment (docker/.env via Docker Compose), never from this file.
set -euo pipefail

REALM=EXAMPLE.COM
MASTER_PASS=${KDC_MASTER_PASSWORD:?KDC_MASTER_PASSWORD is not set - run docker/certs/generate-certs.sh}
STASH=/var/lib/krb5kdc/.k5.${REALM}

if [ ! -f "$STASH" ]; then
  echo "Initializing KDC database for realm ${REALM}"
  kdb5_util create -s -r "${REALM}" -P "${MASTER_PASS}"
fi

if [ ! -f /keytabs/kafka.keytab ]; then
  echo "Creating broker principals (one per advertised hostname)"
  kadmin.local -q "addprinc -randkey kafka/localhost@${REALM}"
  kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/localhost@${REALM}"
  kadmin.local -q "addprinc -randkey kafka/kafka@${REALM}"
  kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/kafka@${REALM}"
fi

if [ ! -f /keytabs/client.keytab ]; then
  echo "Creating client principal client@${REALM}"
  kadmin.local -q "addprinc -randkey client@${REALM}"
  kadmin.local -q "ktadd -k /keytabs/client.keytab client@${REALM}"
fi

if [ ! -f /keytabs/registry.keytab ]; then
  echo "Creating schema registry principal registry/localhost@${REALM}"
  kadmin.local -q "addprinc -randkey registry/localhost@${REALM}"
  kadmin.local -q "ktadd -k /keytabs/registry.keytab registry/localhost@${REALM}"
fi

chmod 644 /keytabs/*.keytab

exec krb5kdc -n
