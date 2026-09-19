FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-config && \
    rm -rf /var/lib/apt/lists/*
COPY krb5-kdc.conf /etc/krb5.conf
COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY kadm5.acl /etc/krb5kdc/kadm5.acl
COPY init-kdc.sh /init-kdc.sh
RUN chmod +x /init-kdc.sh
EXPOSE 88/udp 88/tcp
ENTRYPOINT ["/init-kdc.sh"]
