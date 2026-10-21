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

package aeron

// ClientConductor behaviour driven by Context settings: client name in the heartbeat counter label, pre-touching of
// mapped log buffers, close handlers, publication error frames and the conductor agent invoker.

import (
	"fmt"
	"runtime/debug"

	"github.com/lirm/aeron-go/aeron/atomic"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// ClientVersion is the Aeron protocol version this client targets, reported in the heartbeat counter label.
const ClientVersion = "1.53.3"

func clientCommit() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				return s.Value
			}
		}
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}
	return "unknown"
}

// heartbeatLabelSuffix is appended to the client heartbeat counter label, matching Java's
// " name=<clientName> version=<version> commit=<sha>".
func heartbeatLabelSuffix(clientName string) string {
	return " name=" + clientName + " version=" + ClientVersion + "-go commit=" + clientCommit()
}

// appendToCounterLabel appends value (ASCII) to a counter's label, truncated to MaxLabelLength, returning the number
// of bytes written (AeronCounters.appendToLabel).
func appendToCounterLabel(metaData *atomic.Buffer, counterID int32, value string) (int32, error) {
	recordOffset := counterID * ctr.MetadataLength
	if counterID < 0 || recordOffset+ctr.MetadataLength > metaData.Capacity() {
		return 0, fmt.Errorf("counter id %d out of range", counterID)
	}
	if state := metaData.GetInt32Volatile(recordOffset); state != ctr.RecordAllocated {
		return 0, fmt.Errorf("counter id %d is not allocated, state: %d", counterID, state)
	}
	existing := metaData.GetInt32(recordOffset + ctr.LabelOffset)
	remaining := int32(ctr.MaxLabelLength) - existing
	if remaining <= 0 {
		return 0, nil
	}
	b := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c > 127 {
			c = '?'
		}
		b = append(b, c)
	}
	if int32(len(b)) > remaining {
		b = b[:remaining]
	}
	if len(b) == 0 {
		return 0, nil
	}
	metaData.PutBytesSlice(recordOffset+ctr.LabelOffset+4+existing, b)
	metaData.PutInt32Ordered(recordOffset+ctr.LabelOffset, existing+int32(len(b)))
	return int32(len(b)), nil
}

func (cc *ClientConductor) onHeartbeatCounterFound(counterID int32) {
	if cc.counterMetaDataBuffer == nil {
		return
	}
	if _, err := appendToCounterLabel(cc.counterMetaDataBuffer, counterID, heartbeatLabelSuffix(cc.clientName)); err != nil {
		logger.Warningf("unable to append client name to heartbeat counter %d: %v", counterID, err)
	}
}

// preTouchLogBuffers faults in every page of the log buffers (LogBuffers.preTouch).
func preTouchLogBuffers(lb *logbuffer.LogBuffers) {
	pageSize := lb.Meta().PageSize()
	if pageSize <= 0 {
		pageSize = 4 * 1024
	}
	for i := 0; i < logbuffer.PartitionCount; i++ {
		buf := lb.Buffer(i)
		for off := int32(0); off < buf.Capacity(); off += pageSize {
			buf.CompareAndSetInt32(off, 0, 0)
		}
	}
}

func (cc *ClientConductor) wrapLogBuffers(logFileName string) *logbuffer.LogBuffers {
	lb := logbuffer.Wrap(logFileName)
	if cc.preTouchMappedMemory {
		preTouchLogBuffers(lb)
	}
	return lb
}

// callSubscriberHandler runs an image handler, routing panics to the subscriber error handler.
func (cc *ClientConductor) callSubscriberHandler(handler func(Image), image Image) {
	if handler == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("image handler panic: %v", r)
			if cc.subscriberErrorHandler != nil {
				cc.subscriberErrorHandler(err)
			} else {
				cc.reportError(err)
			}
		}
	}()
	handler(image)
}

// initDutyCycleTimes resets the conductor timers prior to the first duty cycle.
func (cc *ClientConductor) initDutyCycleTimes() {
	cc.adminLock.Lock()
	now := cc.nanoClock()
	cc.timeOfLastKeepaliveNs = now
	cc.timeOfLastServiceNs = now
	cc.adminLock.Unlock()
}

// ConductorAgentInvoker drives the client conductor on the caller's goroutine when
// Context.UseConductorAgentInvoker(true) is set.
type ConductorAgentInvoker struct {
	aeron *Aeron
}

// Invoke performs one conductor duty cycle (and the driver agent invoker's, if configured), returning the work
// count. It returns 0 once the client is closed.
func (i *ConductorAgentInvoker) Invoke() int {
	cc := &i.aeron.conductor
	if cc.IsClosed() {
		return 0
	}
	work := 0
	if inv := i.aeron.context.driverAgentInvoker; inv != nil {
		work += inv.Invoke()
	}
	n, terminated := cc.doWork()
	if terminated {
		// Java AgentInvoker runs Agent.onClose on termination: close handlers, lingering cleanup and ClientClose.
		cc.onClose()
	}
	return work + n
}

// ConductorAgentInvoker returns the invoker for driving the conductor, or nil if the client was not configured with
// Context.UseConductorAgentInvoker(true).
func (aeron *Aeron) ConductorAgentInvoker() *ConductorAgentInvoker {
	return aeron.invoker
}

// Context returns the concluded context the client was connected with.
func (aeron *Aeron) Context() *Context {
	return aeron.context
}
