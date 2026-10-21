# Cross-language interop tests (Go <-> Java Aeron 1.53.3)

`InteropTool` (src/) is a small Java program compiled against `aeron-all-1.53.3.jar`. It runs one role
(`pub`, `sub`, `record`, `replay`, `replaymerge`) on the shared archiving driver and reports JSON events on stdout;
the Go tests in `interop/` drive it as a subprocess and compare payload digests.

```sh
docker compose -f docker/docker-compose.yml up -d --build archiving-driver
docker compose -f docker/interop/docker-compose.yml build
AERON_E2E=1 go test ./interop/ -v
```

Payload format (both languages): message `i` has length `sizes[i % len(sizes)]`, bytes `[0,8)` are `i`
little-endian, byte `j >= 8` is `byte(i*31 + j)`, reserved value is `(i << 32) | 0xCAFE`.

Env: `AERON_DIR` (default `/dev/shm/aeron-compose`), `AERON_INTEROP_IMAGE`, `AERON_INTEROP_JAVA_CP` (run a local
`java -cp` instead of docker), `AERON_INTEROP_MCAST_IFACE` (default `localhost`, `off` skips multicast).
