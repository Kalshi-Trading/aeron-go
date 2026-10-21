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

package term

import (
	"github.com/lirm/aeron-go/aeron/atomic"
)

// BlockHandler is the callback for Image.BlockPoll / Subscription.BlockPoll. It receives a block of whole message
// fragments, including their frame headers, from a single term of a single image.
type BlockHandler func(buffer *atomic.Buffer, offset int32, length int32, sessionID int32, termID int32)

// RawBlockHandler is the callback for Image.RawPoll / Subscription.RawPoll. It receives a block of whole message
// fragments, including their frame headers, together with the log buffer file name and the absolute offset of the
// block within that file so the data can be transferred directly from the file (e.g. with sendfile/splice).
// Java passes a FileChannel; Go passes the file name of the memory mapped log buffer instead.
type RawBlockHandler func(
	logFileName string,
	fileOffset int64,
	termBuffer *atomic.Buffer,
	termOffset int32,
	length int32,
	sessionID int32,
	termID int32)
