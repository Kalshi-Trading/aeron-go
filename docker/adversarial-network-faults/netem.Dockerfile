# NET_ADMIN sidecar for systests/adversarial/network-faults: runs tc/iptables against the host's loopback
# (network_mode: host) so the tests can inject loss, reordering, duplication, delay, corruption and partitions.
ARG BASE_IMAGE=eclipse-temurin:21-jre
FROM ${BASE_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["sleep", "infinity"]
