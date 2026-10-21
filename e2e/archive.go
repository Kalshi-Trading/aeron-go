package e2e

import (
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/archive/replaymerge"
)

// ConnectArchive connects an archive client through the driver in aeronDir.
func ConnectArchive(aeronDir string, controlChannel string, controlStream int32) (*archive.Archive, error) {
	options := archive.DefaultOptions()
	options.RequestChannel = controlChannel
	options.RequestStream = controlStream
	options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
	return archive.NewArchive(options, NewContext(aeronDir))
}

// MaxRecordingID returns the highest recording id known to the archive, or -1 if there are none.
func MaxRecordingID(arch *archive.Archive) int64 {
	max := int64(-1)
	descriptors, _ := arch.ListRecordings(0, 1<<20)
	for _, d := range descriptors {
		if d.RecordingId > max {
			max = d.RecordingId
		}
	}
	return max
}

// WaitForRecording polls the archive's recordings of stream matching channelFragment until match accepts one.
func WaitForRecording(arch *archive.Archive, channelFragment string, stream int32, timeout time.Duration,
	match func(*codecs.RecordingDescriptor) bool) (*codecs.RecordingDescriptor, error) {
	deadline := time.Now().Add(timeout)
	for {
		descriptors, err := arch.ListRecordingsForUri(0, 1<<20, channelFragment, stream)
		for i := len(descriptors) - 1; err == nil && i >= 0; i-- {
			if match == nil || match(descriptors[i]) {
				return descriptors[i], nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no matching recording of %q stream %d after %v (last error: %v)", channelFragment, stream, timeout, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitRecordingPosition waits until the active recording has reached position.
func WaitRecordingPosition(arch *archive.Archive, recordingID int64, position int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		current, err := arch.GetRecordingPosition(recordingID)
		if err == nil && current >= position {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("recording %d at position %d, want %d after %v (last error: %v)", recordingID, current, position, timeout, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitRecordingStopped waits until the recording has a stop position and returns it.
func WaitRecordingStopped(arch *archive.Archive, recordingID int64, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		stop, err := arch.GetStopPosition(recordingID)
		if err == nil && stop != archive.RecordingPositionNull {
			return stop, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("recording %d not stopped after %v (last error: %v)", recordingID, timeout, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WithSessionID returns channel with its session-id parameter set.
func WithSessionID(channel string, sessionID int32) (string, error) {
	uri, err := aeron.ParseChannelUri(channel)
	if err != nil {
		return "", err
	}
	uri.SetSessionID(sessionID)
	return uri.String(), nil
}

// Replay starts a replay of recordingID from position for length bytes (archive.RecordingPositionNull to
// follow the recording) to replayChannel/replayStream and returns a subscription to the replay image.
func Replay(arch *archive.Archive, recordingID int64, position int64, length int64, replayChannel string,
	replayStream int32) (*aeron.Subscription, int64, error) {
	replaySessionID, err := arch.StartReplay(recordingID, position, length, replayChannel, replayStream)
	if err != nil {
		return nil, 0, fmt.Errorf("StartReplay(%d): %w", recordingID, err)
	}
	channel, err := WithSessionID(replayChannel, int32(replaySessionID))
	if err != nil {
		return nil, 0, err
	}
	sub, err := arch.AddSubscription(channel, replayStream)
	if err != nil {
		_ = arch.StopReplay(replaySessionID)
		return nil, 0, err
	}
	return sub, replaySessionID, nil
}

// ReplayMergeOptions configures StartReplayMerge.
type ReplayMergeOptions struct {
	Recording         *codecs.RecordingDescriptor
	StartPosition     int64
	ReplayDestination string // e.g. aeron:udp?endpoint=localhost:0
	LiveDestination   string // e.g. aeron:udp?endpoint=localhost:0|control=localhost:20141
	ProgressTimeoutMs int64
}

// StartReplayMerge subscribes in manual control mode to the recorded session and starts a ReplayMerge.
func StartReplayMerge(arch *archive.Archive, opts ReplayMergeOptions) (*replaymerge.ReplayMerge, *aeron.Subscription, error) {
	if opts.ProgressTimeoutMs == 0 {
		opts.ProgressTimeoutMs = 5000
	}
	sessionID := opts.Recording.SessionId
	subChannel, err := WithSessionID("aeron:udp?control-mode=manual", sessionID)
	if err != nil {
		return nil, nil, err
	}
	replayChannel, err := WithSessionID("aeron:udp", sessionID)
	if err != nil {
		return nil, nil, err
	}
	sub, err := arch.AddSubscription(subChannel, opts.Recording.StreamId)
	if err != nil {
		return nil, nil, err
	}
	merge, err := replaymerge.NewReplayMerge(sub, arch, replayChannel, opts.ReplayDestination, opts.LiveDestination,
		opts.Recording.RecordingId, opts.StartPosition, opts.ProgressTimeoutMs)
	if err != nil {
		sub.Close()
		return nil, nil, err
	}
	return merge, sub, nil
}

// MergePoller adapts a ReplayMerge to PollFunc, failing if the merge fails.
func MergePoller(merge *replaymerge.ReplayMerge) PollFunc {
	return func(handler term.FragmentHandler, fragmentLimit int) (int, error) {
		n, err := merge.Poll(handler, fragmentLimit)
		if err == nil && merge.HasFailed() {
			err = fmt.Errorf("replay merge failed")
		}
		return n, err
	}
}
