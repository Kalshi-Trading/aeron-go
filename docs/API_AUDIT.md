# aeron-go public API audit vs Java Aeron 1.53.3

Scope: every non-generated package in `github.com/lirm/aeron-go` on the `parity` branch, compared with
the Java client 1.53.3 (`io.aeron`, `io.aeron.archive.client`, `io.aeron.cluster.client`,
`io.aeron.cluster.service`, `org.agrona`). Written while porting `io.aeron.samples` (see
`examples/README.md`): every friction point hit while writing the samples is listed here.

Each finding has a severity (**B**roken / **M**isaligned with Java / **D**esign smell), a concrete
fix, and the workstream area that should own it. "Breaking" marks fixes that change public API.

Areas: `client` (aeron package core), `counters`, `logbuffer`, `errors/logging`, `archive`,
`replaymerge`, `cluster-client`, `cluster-service`, `idle`, `util`, `examples`.

## 1. Cross-cutting

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| X1 | D | **Panics instead of errors** in library code: `aeron/publication.go` (3), `aeron/logbuffer/descriptor.go` (6), `aeron/logbuffer/logbuffers.go` (3), `aeron/ringbuffer/manytoone.go` (3), `aeron/ringbuffer/recorddesc.go` (1), `aeron/broadcast/copyreceiver.go` (3), `cluster/consensus_module_proxy.go` (4), `cluster/clustered_service_agent.go` (2), `cluster/service_adapter.go`, `cluster/bounded_log_adapter.go`, `aeron/logging` `MustGetLogger`. A malformed log buffer, a mapping failure or an oversized message takes the whole process down. | Keep panics only for programmer errors that Java also treats as `IllegalArgumentException` on the hot path (e.g. message too long in `Offer`); return `error` from constructors/mapping (`logbuffer.Wrap`, `NewLogBuffers`, `MapFile`) and route runtime failures through `Context.ErrorHandler`. Breaking for constructors. | errors/logging, logbuffer, cluster-service |
| X2 | D | **Global loggers**: package-level `logging.MustGetLogger` in `aeron`, `logbuffer`, `counters`, `memmap`, `driver`, `archive`, `cluster`, `cluster/client`. `archive.Options.ArchiveLoglevel/AeronLoglevel` and `client.Options.Loglevel` mutate *process-global* levels from per-instance options; two archive clients fight over the level. | Add `Context.Logger(*zap.Logger)` (default no-op/`zap.NewNop`), plumb it through conductor/archive/cluster; deprecate the `*Loglevel` option fields. | errors/logging |
| X3 | M | **No `context.Context` cancellation** anywhere: `Aeron.AddPublication/AddSubscription/AddCounter` block until the driver answers or `MediaDriverTimeout`; every synchronous `archive.Archive` call blocks on the response poller with only a fixed timeout; `cluster/client` connect is a hand-rolled poll loop; `ClusteredServiceAgent.StartAndRun` cannot be stopped from outside. | Add `...Ctx(ctx, ...)` variants (or make the async `AsyncAdd*` + `Get*` pair the documented path and add `AwaitPublication(ctx, id)`); archive: `NewArchiveCtx(ctx, ...)` and per-call ctx; agent: `Run(ctx)`. | client, archive, cluster-client, cluster-service |
| X4 | M | **Channels where Java uses callbacks/return values**: `AddSubscriptionDeprecated`, `AddPublicationDeprecated`, `AddExclusivePublicationDeprecated` return `chan`; the top-level `README.md` still teaches `<-a.AddSubscription(...)` which no longer compiles. | Remove the `*Deprecated` channel variants in the next breaking release; README fixed in this PR. | client |
| X5 | D | **Exported internals**: `aeron.ClientConductor`, `DriverProxy`, `ImageFactory`, `IdAndAvailableCounterHandler`/`NewIdAndAvailablePair`, `RegistrationStatus`, `counters.InitAndWrapMetaData`, `counters.MetaDataFlyweight` fields, `logbuffer` frame helpers (`SetFrameLength`, `RotateLog`, ...), `logbuffer.NewTestingLogbuffer`, `archive.Proxy`/`Control` internals, `aeron/stat` is a `package main` inside the library tree, `systests` is part of the module. | Move to `internal/` (conductor, driver proxy, flyweights, frame helpers, test helpers); move `aeron/stat` to `examples/aeronstat` (done here as a new sample; delete the old main once the errors workstream agrees). Breaking. | client, logbuffer, counters, archive |
| X6 | M | **Naming inconsistent with Go and with itself**: `Image.SessionID()` vs `Header.SessionId()`; `Counter.Id`/`TypeId`; `archive.GetRecordingId`, `StopRecordingBySubscriptionId`, `ReplaySessionIdToSessionId`, `ClientId`; SCREAMING constants `RECORDING_POSITION_COUNTER_TYPE_ID`, `NULL_RECORDING_ID`, `SESSION_ID_OFFSET`; `Offer2`; `Get*` prefixes on getters (`GetCounterValue`, `GetKeyPartInt32`, `GetStartPosition`); `cluster/client` `IngressStreamId`. | Pick Go initialisms (`ID`) everywhere, drop `Get` prefixes, CamelCase constants; keep old names as deprecated aliases for one release. `Offer2` -> `OfferParts`/`OfferVectored` matching Java `offer(DirectBuffer, int, int, DirectBuffer, int, int)`. | all (one sweep PR to avoid conflicts) |
| X7 | M | **Defaults don't match Java**: `aeron.NewContext()` ignores `AERON_DIR`/`aeron.dir`; Go sample defaults were `-p /dev/shm` (not an Aeron dir) and ipc stream 10 instead of `SampleConfiguration`. | `NewContext` should honour `AERON_DIR` (Java honours `-Daeron.dir`, C honours `AERON_DIR`). Samples fixed here. | client, examples |

## 2. `aeron` (client core)

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| A1 | M | `Image` interface lacks `SourceIdentity()`, `MtuLength()`, `Subscription()`, `StreamId` (via subscription), `JoinPosition()`. Sample `PrintAvailableImage` cannot print `channel/streamId/source` like Java `SamplesUtil`. | Add the accessors (data is already in the image ready message). | client |
| A2 | M | `Publication` is one struct for both concurrent and exclusive publications; Java has `ConcurrentPublication`/`ExclusivePublication` with exclusive-only `appendPadding`, `offerBlock`, `termId/termOffset`. | Introduce `ExclusivePublication` type (embed common part) with the exclusive-only methods; `AddExclusivePublication` returns it. Breaking. | client |
| A3 | M | `Offer` result codes are bare `int64` constants (`NotConnected`, ...); no `String()`; `MaxPositionExceeded` missing from older docs. Samples had to reimplement Java's message mapping. | `type OfferResult int64` with `String()`, `IsTerminal()`; keep int64 return for zero-alloc. | client |
| A4 | D | `Aeron.Close()` returns error but `Publication.Close()`/`Subscription.Close()` semantics vs Java `close()` (idempotent, async to driver) undocumented. | Document; make all `Close` idempotent and safe after client close. | client |
| A5 | M | `ChannelUri.Set`/`Remove` have value receivers that mutate a shared map, so `Clone()` is required to avoid aliasing; `ParseChannelUri` returns a value. Java `ChannelUri` is a mutable reference type. | Use pointer receivers consistently and return `*ChannelUri`; add `ChannelUri.AddSessionId` helper (archive has its own `AddSessionIdToChannel`). | client |
| A6 | D | `Context` setters return `*Context` but there are also `Aeron*` setters on `archive.Archive` (`SetAeronDir`, `SetAeronMediaDriverTimeout`, ...) which act after the client is connected and therefore do nothing useful. | Remove the `archive.Archive.SetAeron*` methods; configure the `aeron.Context` before `NewArchive`. Breaking. | archive |
| A7 | M | No `Aeron.Context().aeronDirectoryName()`-style accessor and no `CncFileName` on `Aeron`; samples re-derive paths. | Add `Aeron.Context()` returning the (read-only) context. | client |

## 3. `aeron/counters`

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| C1 | M | `Reader.Scan` callback gets `Counter{Id,TypeId,Value,Label}` without the key buffer; Java `CountersReader.forEach(MetaData)` passes `(counterId, typeId, keyBuffer, label)`. Stat tools must call `GetKeyPart*` per field. | Add `ForEach(func(counterId, typeId int32, key *atomic.Buffer, label string))`. | counters |
| C2 | B | `MapFile` leaks the mmap when the CnC version mismatches (returns `nil,nil,err` without unmapping) and checks only for equality with `CurrentCncVersion` (Java checks *major* version compatibility via `SemanticVersion`). | Unmap on error; compare major version only. | counters |
| C3 | M | Error log and loss report readers now exist on `parity` (`aeron/cnc`, `aeron/errorlog`, `aeron/lossreport`) and `errorstat`/`lossstat` use them. Still missing: driver heartbeat age, stream-counter key decoding and backlog aggregation (kept in `examples/internal/cnc` for `aeronstat`/`backlogstat`). | Add `cnc.File.DriverHeartbeatAgeMs`, stream-counter key decoder and a `BacklogStat` helper, then shrink `examples/internal/cnc`. | counters |
| C4 | M | No typed constants for driver counter type ids (`AeronCounters.*_TYPE_ID`) or stream-counter key offsets. | Export `counters.PublisherLimitTypeID` ... and `StreamCounterKey` decoder. | counters |
| C5 | D | `GetCounterValue` returns 0 for an invalid id instead of an error/`NullValue`. | Return `(int64, bool)` or document Java behaviour (Java throws). | counters |

## 4. `aeron/logbuffer`, `term`, `atomic`, `idlestrategy`, `util`

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| L1 | D | `FragmentAssembler`/`ControlledFragmentAssembler` constructors take an initial buffer length `int32` while Java has a no-arg default; callers pass `aeron.DefaultFragmentAssemblyBufferLength` everywhere. | Functional option or `NewFragmentAssembler(handler)` default. | logbuffer |
| L2 | M | `atomic.Buffer.PutBytesArray(index, *[]byte, srcint32, length)` takes a pointer to a slice and an oddly named parameter. | `PutBytes(index int32, src []byte)` (Java `putBytes(int, byte[])`). | util |
| L3 | D | `atomic.MakeBuffer(args ...interface{})` is untyped variadic (slice, or ptr+len). | `NewBuffer([]byte)` / `NewBufferFromPointer(unsafe.Pointer, int)`. | util |
| L4 | M | Idle strategies: `Sleeping{SleepFor}` vs Java `SleepingIdleStrategy(ns)`, no `SleepingMillisIdleStrategy`, `NoOpIdleStrategy`, no `Reset()`/alias (`IdleStrategy.alias()`), `Busy` is a value type while backoff is a pointer. | Add `Reset()` and `Alias()` to `Idler`, add `NoOp`; keep current types. | idle |
| L5 | D | `term.FragmentHandler` vs `ControlledFragmentHandler` naming fine, but `term.ReservedValueSupplier` must be passed as `nil` on every `Offer`. | Make the common `Offer(buf, off, len)` the 3-arg form and add `OfferWithSupplier`. Breaking. | client |

## 5. `archive`

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| R1 | M | `FindCounterIdBySession(reader, sessionId)` has no `archiveId` parameter; Java 1.53.3 `RecordingPos.findCounterIdBySession(counters, sessionId, archiveId)` disambiguates multiple archives on one driver. `IsRecordingActive` likewise. | Done: `archive/status.FindCounterIdByRecording/FindCounterIdBySession(reader, id, archiveId)`, `IsActive`, `GetArchiveId`; the `archive.*` wrappers are deprecated (archiveId = NullValue). | archive |
| R2 | D | Archive listeners are a struct of function fields (`ArchiveListeners`) plus package-level `Logging*Listener` defaults that log through the global logger. Java uses `RecordingSignalConsumer`, `ErrorHandler`, `RecordingEventsListener` interfaces set on `AeronArchive.Context`. | Accept listeners through `Options`; default to no-op, not logging. | archive |
| R3 | M | `StartRecording(channel, stream, isLocal bool, autoStop bool)`: `isLocal` bool instead of `codecs.SourceLocation`, inconsistent with `ExtendRecording` which takes `codecs.SourceLocationEnum`. | Use a `SourceLocation` type in both. Breaking. | archive |
| R4 | D | `PollForErrorResponse() (int, error)` - Java returns the error message string or null; Go returns a count plus error, and the count is meaningless to callers. | `PollForErrorResponse() error`. | archive |
| R5 | M | Recording a UDP publication locally without a network subscriber gives `NotConnected` unless `ssc=true` (Java behaves the same, but the old Go sample hid this with `MdcChannel + "|ssc=true"`). Documented in the sample. | Doc only. | examples |
| R6 | D | `ListRecordings*` return `[]*codecs.RecordingDescriptor` (generated codec type with SBE fields) instead of a consumer callback / plain struct as in Java `RecordingDescriptorConsumer`. Ties users to codec regeneration. | Return a stable `archive.RecordingDescriptor` struct. Breaking. | archive |
| R7 | D | `Archive.Aeron()` exposes the owned client; closing it breaks the archive. Java separates `ownsAeronClient`. | Allow passing an existing `*aeron.Aeron` (`Options.AeronClient`) and do not close it when not owned. | archive |
| R8 | M | Sync API only; no `AsyncConnect`/`ArchiveProxy` + `ControlResponsePoller` usage pattern for agent-style apps. | Add `AsyncConnect` or document current limitation. | archive |

## 6. `cluster/client`

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| K1 | M | `NewAeronCluster` always calls `aeron.Connect` internally, so a process cannot share one Aeron client between archive, cluster and plain pubs (Java `AeronCluster.Context.aeronClient()`). | Accept an optional `*aeron.Aeron`. | cluster-client |
| K2 | M | Connect is not a single call: callers must loop `Poll()` until `IsConnected()` (no `AeronCluster.connect()` equivalent with timeout / `AsyncConnect`). | `Connect(ctx)` blocking helper + keep the async poll form. | cluster-client |
| K3 | D | `Offer` returns raw `int64`; no `TryClaim`; `SendKeepAlive` returns bool; `Close()` returns nothing. | Match Java (`tryClaim`, `offer(DirectBufferVector[])`), `Close() error`. | cluster-client |
| K4 | D | `EgressListener` is one interface with 5 methods; Java splits `EgressListener` (onMessage, onSessionEvent, onNewLeader, onAdminResponse) and has `ControlledEgressListener`. Missing `onSessionEvent`/`onAdminResponse`. | Add the missing callbacks via optional interfaces. | cluster-client |
| K5 | D | `Options.IngressStreamId`/`EgressStreamId` naming; `Options.Loglevel` global (X2). | Rename to `...StreamID`. | cluster-client |

## 7. `cluster` (service side)

| # | Sev | Finding | Recommended fix | Area |
|---|-----|---------|-----------------|------|
| S1 | M | `ClusteredServiceAgent.StartAndRun()` blocks forever; no stop/ctx; termination only via cluster. | `Run(ctx)` and `Close()`. | cluster-service |
| S2 | B | Panics in `consensus_module_proxy.go`, `service_adapter.go`, `bounded_log_adapter.go` on unexpected messages / offer failures instead of reporting via error handler. | Return errors / call `ErrorHandler`. | cluster-service |
| S3 | M | Sample `examples/cluster/echo_service.go` configured only via env vars (`AERON_DIR`, `CLUSTER_DIR`, `NO_OP_IDLE`) - no `ClusteredServiceContainer.Context` equivalent with defaults from Java properties. | Options struct with Java-property-style defaults. | cluster-service |

## 8. Samples parity (this PR)

| Java 1.53.3 sample | Go | Status |
|---|---|---|
| BasicPublisher / BasicSubscriber | `examples/basic_publisher`, `basic_subscriber` | fixed (defaults, AERON_DIR, inverted log level, signal handling, `-exit`) |
| ExclusivePublisher | `examples/exclusive_publisher` | new |
| SimplePublisher / SimpleSubscriber | `examples/simple_publisher`, `simple_subscriber` | new |
| MultiplePublishersWithFragmentation / MultipleSubscribersWithFragmentAssembly | `examples/multiple_publishers_with_fragmentation`, `multiple_subscribers_with_fragment_assembly` | new |
| StreamingPublisher / RateSubscriber / RateReporter | `examples/streaming_publisher`, `rate_subscriber`, `examples.RateReporter` | new |
| Ping / Pong | `examples/ping`, `pong` | fixed (Java channels/streams, warmup back-pressure panic, exit) |
| EmbeddedExclusiveThroughput | `examples/exclusive_throughput` | new (external driver; Go has no embedded driver) |
| AeronStat / ErrorStat / LossStat / BacklogStat | `examples/aeronstat`, `errorstat`, `lossstat`, `backlogstat` | new (parsers in `examples/internal/cnc`) |
| archive RecordedBasicPublisher / ReplayedBasicSubscriber | `archive/examples/basic_recording_publisher`, `basic_replayed_subscriber` | fixed (counter wait instead of sleep, `os.Exit(1)` on success, stop recording, `-exit`) |
| archive EmbeddedReplayThroughput | `archive/examples/replay_throughput` | new (external archive) |
| cluster EchoService / client | `examples/cluster` (service, existing), `examples/cluster_echo_client` (new) | new client, verified vs Java `EchoServiceNode` (`docker/cluster.compose.yml`) |
| StreamStat, ImageRateSubscriber, FileSender/Receiver, LogInspector, Embedded*Ipc* variants, ReplayMergeSubscriber (other workstream), PersistentSubscriber, RecordingReplicator | - | not ported (lower value; ReplayMerge owned elsewhere) |

## 9. Suggested order for the review fleet

1. X6 naming sweep + X5 `internal/` move in one coordinated breaking PR (touches everything, do it last after feature PRs merge).
2. X2 logger injection and X1 panic removal (behavioural, high value for embedding in services).
3. X3 ctx variants for blocking calls (client, archive, cluster connect).
4. C1/C3/C4 counters + reports API, then shrink `examples/internal/cnc` to use it.
5. A1/A2/A3, R1-R7, K1-K4 per-area.
