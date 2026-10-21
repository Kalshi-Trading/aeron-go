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
* `basic_replay_merge`: owned by the ReplayMerge workstream.

E2E coverage: `examples/e2e` (`TestArchiveRecordAndReplay`, `TestArchiveReplayThroughput`).
