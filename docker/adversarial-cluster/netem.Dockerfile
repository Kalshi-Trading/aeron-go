# Network fault sidecar for systests/adversarial/cluster: run with --net container:<node> --cap-add NET_ADMIN to drop
# (iptables) or delay/lose (tc netem) the node's traffic inside its own network namespace.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables && rm -rf /var/lib/apt/lists/*
