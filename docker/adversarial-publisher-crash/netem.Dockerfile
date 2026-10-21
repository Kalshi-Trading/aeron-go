# tc/netem sidecar (run with --network host --cap-add NET_ADMIN) for network faults on lo.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["tc"]
