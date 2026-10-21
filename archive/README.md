# aeron-go/archive

Implementation of [Aeron Archive](https://github.com/real-logic/Aeron/tree/master/aeron-archive) client in Go.

The [Aeron Archive
protocol](http://github.com/real-logic/aeron/blob/master/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml)
is specified in xml using the [Simple Binary Encoding (SBE)](https://github.com/real-logic/simple-binary-encoding)

## Current State
The implementation is the second beta release. The API may still be changed for bug fixes or significant issues.

# Design

## Guidelines

The structure of the archive client library is heavily based on the
Java archive library. It's hoped this will aid comprehension, bug fixing,
feature additions etc.

Many design choices are also based upon the golang client library as
the archive library is a layering on top of that.

Finally golang idioms are used where reasonable.

The archive library does not lock and concurrent calls on an archive
client to invoke the aeron-archive protocol calls should be externally
locked to ensure only one concurrent access.

### Naming and other choices

Function names used in archive.go which contains the main API are
based on the Java names so that developers can more easily switch
between languages and so that any API documentation is more useful
across implementations. Some differences exist due to capitalization
requirements, lack of polymorphism, etc.

Function names used in encoders.go and proxy.go are based on the
protocol specification. Where the protocol specifies a type that can
be naturally represented in golang, the golang type is used used where
possible until encoding. Examples include the use of `bool` rather than
`BooleanType` and `string` over `[]uint8`

## Structure

The archive protocol is largely an RPC mechanism built on top of
Aeron. Each Archive instance has it's own aeron instance running a
[proxy](proxy.go) (publication/request) and [control](control.go) (subscription/response)
pair. This mirrors the Java implementation. The proxy invokes the
encoders to marshal packets using SBE.

Additionally there are some asynchronous events that can arrive on a
[recordingevents](recordingevents.go) subscription. These
are not enabled by default to avoid using resources when not required.

## Synchronous unlocked API optionally using polling

The implementation provides a synchronous API as the underlying
mechanism is largely an RPC mechanism and archive operations are not
considered high frequency.

If needed it is simple in golang to wrap a synchronous API with a
channel (see for example aeron.AddSubscription().

Some asynchronous events do exist (e.g, recording events and
heartbeats) and to be delivered the polling mechanisms of
RecordingEventsPoll() and PollForErrorResponse() are provided. These
may be easily wrapped in a goroutine if desired,

## Examples

Ports of the Java archive samples are in [examples/](examples/README.md):
[basic_recording_publisher](examples/basic_recording_publisher/basic_recording_publisher.go),
[basic_replayed_subscriber](examples/basic_replayed_subscriber/basic_replayed_subscriber.go),
[basic_replay_merge](examples/basic_replay_merge/basic_replay_merge.go) (ReplayMerge),
[persistent_subscriber](examples/persistent_subscriber/persistent_subscriber.go) (PersistentSubscription) and
[replay_throughput](examples/replay_throughput/replay_throughput.go). They interoperate with the Java samples and
run against the Java 1.53.3 archiving driver in [docker/](../docker/README.md).

## ReplayMerge

[`replaymerge.ReplayMerge`](replaymerge/replaymerge.go) (Java `ReplayMerge`) replays a recording into a
`control-mode=manual` multi-destination subscription, adds the live destination once the replay is close to the
live position and removes the replay once both are aligned, so the image continues on the live stream without a
gap or duplicate. Call `Poll` in the duty cycle until `IsMerged()`, check `HasFailed()`; after merging keep polling
`Image()` (or the ReplayMerge). `MergeProgressTimeoutDefaultMs` (5 s) bounds each step.

## PersistentSubscription

[`persistentsubscription.PersistentSubscription`](persistentsubscription/persistent_subscription.go) (Java
`PersistentSubscription`) consumes a recorded stream from `StartPosition` (`FromStart`, `FromLive` or a position):
it replays from the archive, joins the live channel when the replay catches up, and falls back to replay if the
live image goes away. Configure it with `persistentsubscription.NewContext()` (`RecordingID`, `LiveChannel`,
`LiveStreamID`, `ReplayChannel`, `ReplayStreamID`, optional `Listener` for live joined/left/errors) and drive it with
`Poll`/`ControlledPoll` from a single goroutine; `HasFailed()`/`FailureReason()` report terminal errors.

## Security

Set `Options.CredentialsSupplier` (Java `CredentialsSupplier`): `StaticCredentialsSupplier{Credentials, ChallengeResponse}`
or `CredentialsSupplierFuncs` for dynamic challenge responses. The default `NullCredentialsSupplier` sends empty
credentials. `make e2e` runs the authentication E2E tests against `docker/auth.compose.yml`.

The actual semantics of the security are dependent upon which authenticator supplier you use and is tested against [secure-logging-archiving-media-driver](secure-logging-archiving-media-driver).

# Backlog
 * godoc improvements
 * more testing
  * archive-media-driver mocking/execution
  * test cleanup in the media driver can be problematic
 * Auth should provide some callout mechanism
 * various FIXMEs
 * There seems to be problems if there are multiple archive
   instances. Particularly noticeable when calling aeron.Close()

# Bigger picture issues
 * It would be nice to silence the OnAvailableCounter noise
 * Within aeron-go there are cases of Log.Fatalf(), see for example trying to add a publication on a "bogus" channel.

## Release Notes

### 1.0b3 (in-progress)
 * Add PollForErrorResponse()
 * concurrency improvements by having the library lock around RPCs

### 1.0b2
 * Handle different archive clients using same channel/stream pairing
 * Provide Subscription.IsConnected() and IsRecordingEventsConnected()
 * Replace go-logging with zap to avoid reentrancy crashes in logging library
 * Improve the logging by downgrading in severity and message tone some warning level messages
 * Fix a race condition looking up correlationIDs on ControlResponse
 * Fix a return code error in StopRecordingById()
 * Fix unused argumentin StopRecording()
 * Cosmetic improvements for golint and staticcheck
 * Make the Listeners used for async events be per archive client instead of global
 