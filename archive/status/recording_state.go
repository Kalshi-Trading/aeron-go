// Copyright 2026 Kalshi Trading
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

package status

import (
	"fmt"

	"github.com/lirm/aeron-go/archive/codecs"
)

// RecordingStateString returns the Java enum name of a catalog recording state (io.aeron.archive.codecs.RecordingState).
func RecordingStateString(state codecs.RecordingStateEnum) string {
	switch state {
	case codecs.RecordingState.INVALID:
		return "INVALID"
	case codecs.RecordingState.VALID:
		return "VALID"
	case codecs.RecordingState.DELETED:
		return "DELETED"
	case codecs.RecordingState.NullValue:
		return "NULL_VAL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(state))
	}
}

// ParseRecordingState is the inverse of RecordingStateString for the INVALID, VALID and DELETED states.
func ParseRecordingState(name string) (codecs.RecordingStateEnum, error) {
	switch name {
	case "INVALID":
		return codecs.RecordingState.INVALID, nil
	case "VALID":
		return codecs.RecordingState.VALID, nil
	case "DELETED":
		return codecs.RecordingState.DELETED, nil
	default:
		return codecs.RecordingState.NullValue, fmt.Errorf("unknown RecordingState %q", name)
	}
}
