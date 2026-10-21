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

// Package status provides read-only helpers for locating and decoding the counters an Aeron Archive
// publishes into the media driver's CnC counters file. It ports io.aeron.archive.status.RecordingPos,
// io.aeron.archive.ArchiveCounters and io.aeron.archive.ControlSessionCounter (lookups only; the
// allocation side lives in the Java archive) from Aeron 1.53.3.
package status
