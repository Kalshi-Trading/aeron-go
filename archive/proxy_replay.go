// Copyright (C) 2021-2022 Talos, Inc.
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

package archive

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

func (proxy *Proxy) offerPacket(bytes []byte, err error) error {
	if err != nil {
		return err
	}
	if proxy.archive.State() == ArchiveStateClosed {
		return NewArchiveException("client is closed", ErrorCodeGeneric, NullValue)
	}
	return proxy.offerBytes(bytes)
}

// offerBytes offers an encoded request regardless of the client state, used to send CloseSession during Close.
func (proxy *Proxy) offerBytes(bytes []byte) error {
	ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	switch {
	case ret >= 0:
		return nil
	case ret == aeron.PublicationClosed:
		return NewArchiveException("connection to the archive has been closed", ErrorCodeGeneric, NullValue)
	case ret == aeron.NotConnected:
		return NewArchiveException("connection to the archive is no longer available", ErrorCodeGeneric, NullValue)
	case ret == aeron.MaxPositionExceeded:
		return NewArchiveException("offer failed due to max position being reached", ErrorCodeGeneric, NullValue)
	default:
		return NewArchiveException(fmt.Sprintf("failed to send archive request: %d", ret), ErrorCodeGeneric, NullValue)
	}
}

// ReplayRequestWithParams sends a ReplayRequest, or a BoundedReplayRequest if params.BoundingLimitCounterID
// is set, carrying all of the ReplayParams fields. A nil params uses DefaultReplayParams().
func (proxy *Proxy) ReplayRequestWithParams(correlationID int64, recordingID int64, replayChannel string, replayStream int32, params *ReplayParams) error {
	if params == nil {
		params = NewReplayParams()
	}
	if params.IsBounded() {
		return proxy.offerPacket(codecs.BoundedReplayWithParamsPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
			proxy.archive.SessionID, correlationID, recordingID, params.Position, params.Length,
			params.BoundingLimitCounterID, replayStream, params.FileIoMaxLength, params.ReplayToken, replayChannel))
	}
	return proxy.offerPacket(codecs.ReplayRequestWithParamsPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID, recordingID, params.Position, params.Length,
		replayStream, params.FileIoMaxLength, params.ReplayToken, replayChannel))
}

// ReplayTokenRequest packet and offer
func (proxy *Proxy) ReplayTokenRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.ReplayTokenRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID, recordingID))
}

// MaxRecordedPositionRequest packet and offer
func (proxy *Proxy) MaxRecordedPositionRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.MaxRecordedPositionRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID, recordingID))
}

// ArchiveIdRequest packet and offer
func (proxy *Proxy) ArchiveIdRequest(correlationID int64) error {
	return proxy.offerPacket(codecs.ArchiveIdRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID))
}
