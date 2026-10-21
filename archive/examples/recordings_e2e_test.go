// Copyright 2026 Kalshi Trading.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package examples

import (
	"os"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

// More recordings than the 100-entry page the samples used to list, so a first-page-only lookup misses the latest.
func TestFindLatestRecordingListsEveryRecording(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java archiving driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	arch, err := archive.NewArchive(archive.DefaultOptions(), aeron.NewContext().AeronDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	const channel = "aeron:ipc?term-length=65536"
	stream := int32(100000 + time.Now().UnixNano()%100000)
	if _, err := arch.StartRecording(channel, stream, true, false); err != nil {
		t.Fatal(err)
	}
	defer arch.StopRecording(channel, stream)

	var lastSessionID int32
	from := int64(0)
	for i := 0; i < 101; i++ {
		pub, err := arch.AddExclusivePublication(channel, stream)
		if err != nil {
			t.Fatal(err)
		}
		lastSessionID = pub.SessionID()
		deadline := time.Now().Add(10 * time.Second)
		for found := false; !found; {
			if time.Now().After(deadline) {
				t.Fatalf("recording %d for session %d not created", i, lastSessionID)
			}
			if _, err := arch.ListRecordingsForUriWithConsumer(from, 1, "aeron:ipc", stream,
				func(d *codecs.RecordingDescriptor) {
					if d.SessionId == lastSessionID {
						found = true
						from = d.RecordingId + 1
					}
				}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		pub.Close()
	}

	latest, err := FindLatestRecording(arch, "aeron:ipc", stream)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.SessionId != lastSessionID {
		t.Fatalf("FindLatestRecording = %+v, want session %d", latest, lastSessionID)
	}
}
