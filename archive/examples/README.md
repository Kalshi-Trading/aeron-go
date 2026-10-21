# archive samples

Run against the Java 1.53.3 ArchivingMediaDriver in [`docker/`](../../docker/README.md) with
`-prefix /dev/shm/aeron-compose` (archive control `aeron:udp?endpoint=localhost:8010` stream 10).
Flags are in `examplesconfig.go` (`-samplechannel`, `-samplestream`, `-messages`, `-payload`, `-verbose`).

* `basic_recording_publisher` (Java `RecordedBasicPublisher`): starts a local recording of
  `-samplechannel|ssc=true`, waits for the archive's RecordingPos counter, publishes `-messages`
  messages every `-interval`, waits for the recording to catch up and stops the recording.
  `ssc=true` (spies simulate connection) lets offers succeed with only the archive spy attached.
* `basic_replayed_subscriber` (Java `ReplayedBasicSubscriber`): finds the latest recording for the
  sample channel/stream and replays it to stream `samplestream+1`; `-exit N` stops after N messages.
* `replay_throughput` (Java `EmbeddedReplayThroughput`, external archive): records `-messages` IPC
  messages, replays them and prints record/replay MB/s and msgs/s, then purges the recording.
* `basic_replay_merge` (Java `ReplayMergeSubscriber`): finds the latest recording of
  `-samplechannel`/`-samplestream`, subscribes with `control-mode=manual|session-id=<recording session>`
  and uses `replaymerge.ReplayMerge` to replay from the recording's start position to
  `-replaydestination` (default `aeron:udp?endpoint=localhost:0`) and merge onto the live
  `-samplechannel`. Prints `ReplayMerge has joined live stream.` on merge; `-exit N` stops after N messages.
* `persistent_subscriber` (Java `PersistentSubscriber`): a `persistentsubscription.PersistentSubscription`
  on the latest recording of the sample channel/stream, starting at `-start` (`-1` FROM_START, the
  default, `-2` FROM_LIVE, or a frame-aligned position), replaying on `-replaychannel`/`-replaystream`
  (default `aeron:udp?endpoint=localhost:0`, 5000). Listener callbacks print live joined/left and errors.

Try ReplayMerge / PersistentSubscription against the docker harness (two terminals):

```sh
go run ./archive/examples/basic_recording_publisher -prefix /dev/shm/aeron-compose -messages 600 -interval 100ms
go run ./archive/examples/basic_replay_merge -prefix /dev/shm/aeron-compose -exit 600
# or: go run ./archive/examples/persistent_subscriber -prefix /dev/shm/aeron-compose -exit 600
```

E2E coverage: `examples/e2e` (`TestArchiveRecordAndReplay`, `TestArchiveReplayMerge`,
`TestArchivePersistentSubscriber`, `TestArchiveReplayThroughput`) and
`archive/examples` (`TestFindLatestRecordingListsEveryRecording`).
