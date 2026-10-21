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

// Port of io.aeron.CommonContext: Aeron directory resolution, CnC file helpers, driver liveness checks and driver
// termination requests, plus the shared configuration property names and channel URI constants.

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// Configuration property names (Java system property names) and their environment variable equivalents (as used by
// the C client: upper-cased with '.' replaced by '_'). Environment variables take effect in NewContext.
const (
	AeronDirPropName                   = "aeron.dir"
	AeronDirEnv                        = "AERON_DIR"
	DriverTimeoutPropName              = "aeron.driver.timeout"
	DriverTimeoutEnv                   = "AERON_DRIVER_TIMEOUT"
	ClientNamePropName                 = "aeron.client.name"
	ClientNameEnv                      = "AERON_CLIENT_NAME"
	IdleSleepDurationPropName          = "aeron.client.idle.sleep.duration"
	IdleSleepDurationEnv               = "AERON_CLIENT_IDLE_SLEEP_DURATION"
	ResourceLingerDurationPropName     = "aeron.client.resource.linger.duration"
	ResourceLingerDurationEnv          = "AERON_CLIENT_RESOURCE_LINGER_DURATION"
	CloseLingerDurationPropName        = "aeron.client.close.linger.duration"
	CloseLingerDurationEnv             = "AERON_CLIENT_CLOSE_LINGER_DURATION"
	PreTouchMappedMemoryPropName       = "aeron.pre.touch.mapped.memory"
	PreTouchMappedMemoryEnv            = "AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY"
	EnableExperimentalFeaturesPropName = "aeron.enable.experimental.features"
)

// Client defaults, matching io.aeron.CommonContext and io.aeron.Aeron.Configuration.
const (
	// DefaultDriverTimeout is the default timeout after which the driver is considered inactive (10s).
	DefaultDriverTimeout = 10 * time.Second
	// DefaultKeepAliveInterval is the interval between client keepalives / heartbeat updates.
	DefaultKeepAliveInterval = 500 * time.Millisecond
	// DefaultIdleSleepDuration is the sleep duration of the default conductor idle strategy.
	DefaultIdleSleepDuration = 16 * time.Millisecond
	// DefaultAwaitingIdleSleepDuration is the sleep used while awaiting driver responses.
	DefaultAwaitingIdleSleepDuration = time.Millisecond
	// DefaultResourceLingerDuration is how long released resources linger before being unmapped.
	DefaultResourceLingerDuration = 3 * time.Second
	// DefaultCloseLingerDuration is how long to linger on close before unmapping lingering resources.
	DefaultCloseLingerDuration = time.Duration(0)
	// DefaultPublicationConnectionTimeout is a Go-specific timeout used to determine Publication connectivity.
	DefaultPublicationConnectionTimeout = 5 * time.Second
	// MaxClientNameLength is the maximum length of Context.ClientName.
	MaxClientNameLength = 100
	// NullSessionID is the null value for a session id.
	NullSessionID = NullValue

	maxIdleSleepDuration = time.Second
)

// Channel URI constants from io.aeron.CommonContext that are not already declared in channeluri.go.
const (
	UdpChannel                                  = "aeron:udp"
	ControlModeResponse                         = "response"
	ReservedOffset                              = "reserved"
	ResponseEndpointParamName                   = "response-endpoint"
	ResponseCorrelationIdParamName              = "response-correlation-id"
	PrototypeCorrelationId                      = "prototype"
	NakDelayParamName                           = "nak-delay"
	UntetheredWindowLimitTimeoutParamName       = "untethered-window-limit-timeout"
	UntetheredLingerTimeoutParamName            = "untethered-linger-timeout"
	UntetheredRestingTimeoutParamName           = "untethered-resting-timeout"
	MaxResendParamName                          = "max-resend"
	StreamIdParamName                           = "stream-id"
	PublicationWindowLengthParamName            = "pub-wnd"
	terminateDriverMsgTypeID              int32 = 0x0E
)

// CncVersion is the CnC file version this client understands (0.2.0), see CncFileDescriptor.CNC_VERSION.
var CncVersion = util.SemanticVersionCompose(0, 2, 0)

// ErrDriverTimeout is wrapped by errors returned when the media driver is not active within the driver timeout.
var ErrDriverTimeout = errors.New("driver timeout")

// cncMetaDataLength is CncFileDescriptor.META_DATA_LENGTH.
const cncMetaDataLength = util.CacheLineLength * 2

// filePageSizeFieldOffset is CncFileDescriptor.FILE_PAGE_SIZE_FIELD_OFFSET.
const filePageSizeFieldOffset = 6*4 + 3*8

// DefaultAeronDirectoryName returns the default Aeron directory. As with CommonContext.getAeronDirectoryName() it is
// taken from the AERON_DIR environment variable if set, otherwise /dev/shm/aeron-<user> on Linux when /dev/shm exists,
// otherwise <tmpdir>/aeron-<user>.
func DefaultAeronDirectoryName() string {
	if dir := os.Getenv(AeronDirEnv); dir != "" {
		return dir
	}
	return defaultAeronDirBase() + "-" + userName()
}

func defaultAeronDirBase() string {
	if runtime.GOOS == "linux" {
		if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
			return "/dev/shm/aeron"
		}
	}
	return filepath.Join(os.TempDir(), "aeron")
}

func userName() string {
	if UserName != "" {
		return UserName
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "default"
}

// CncFileName returns the path of the CnC file within the given Aeron directory (CommonContext.newCncFile).
func CncFileName(aeronDir string) string {
	return filepath.Join(aeronDir, counters.CncFile)
}

// DefaultCncFileName returns the path of the CnC file in the default Aeron directory.
func DefaultCncFileName() string {
	return CncFileName(DefaultAeronDirectoryName())
}

// CheckCncVersion returns an error if the CnC file version is not compatible with this client: the major versions
// must match (CncFileDescriptor.checkVersion) and the driver's minor version must not be older than the client's.
func CheckCncVersion(cncVersion int32) error {
	v := uint32(cncVersion)
	if util.SemanticVersionMajor(v) != util.SemanticVersionMajor(CncVersion) {
		return fmt.Errorf("CnC version not compatible: app=%s file=%s",
			util.SemanticVersionToString(CncVersion), util.SemanticVersionToString(v))
	}
	if util.SemanticVersionMinor(v) < util.SemanticVersionMinor(CncVersion) {
		return fmt.Errorf("driverVersion=%s insufficient for clientVersion=%s",
			util.SemanticVersionToString(v), util.SemanticVersionToString(CncVersion))
	}
	return nil
}

// cncMapping is a mapped CnC file with the to-driver ring buffer wrapped.
type cncMapping struct {
	file     *memmap.File
	meta     *counters.MetaDataFlyweight
	toDriver rb.ManyToOne
}

func (m *cncMapping) Close() error {
	return m.file.Close()
}

func (m *cncMapping) filePageSize() int32 {
	buf := atomic.MakeBuffer(m.file.GetMemoryPtr(), m.file.GetMemorySize())
	return buf.GetInt32(filePageSizeFieldOffset)
}

func cncFileUsable(cncFile string) bool {
	fi, err := os.Stat(cncFile)
	return err == nil && !fi.IsDir() && fi.Size() > int64(cncMetaDataLength)
}

// mapCncFile maps an existing CnC file. It returns (nil, nil) when the version field is not yet initialised.
func mapCncFile(cncFile string) (*cncMapping, error) {
	file, err := memmap.MapExisting(cncFile, 0, 0)
	if err != nil {
		return nil, err
	}
	buf := atomic.MakeBuffer(file.GetMemoryPtr(), file.GetMemorySize())
	version := buf.GetInt32Volatile(0)
	if version == 0 {
		file.Close()
		return nil, nil
	}
	if err := CheckCncVersion(version); err != nil {
		file.Close()
		return nil, err
	}
	var meta counters.MetaDataFlyweight
	meta.Wrap(buf, 0)
	if int64(meta.Size()) > int64(file.GetMemorySize()) {
		file.Close()
		return nil, nil
	}
	m := &cncMapping{file: file, meta: &meta}
	m.toDriver.Init(meta.ToDriverBuf.Get())
	return m, nil
}

// awaitCncFile waits, until the deadline, for the CnC file to exist, be initialised and have a live driver heartbeat,
// mirroring Aeron.Context.connectToDriver().
func awaitCncFile(cncFile string, driverTimeout time.Duration) (*cncMapping, error) {
	deadline := time.Now().Add(driverTimeout)
	for {
		if !cncFileUsable(cncFile) {
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("%w: CnC file not created: %s", ErrDriverTimeout, cncFile)
			}
			time.Sleep(DefaultIdleSleepDuration)
			continue
		}
		m, err := mapCncFile(cncFile)
		if err != nil {
			return nil, err
		}
		if m == nil {
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("%w: CnC file is created but not initialised: %s", ErrDriverTimeout, cncFile)
			}
			time.Sleep(DefaultAwaitingIdleSleepDuration)
			continue
		}
		for m.toDriver.ConsumerHeartbeatTime() == 0 {
			if time.Now().After(deadline) {
				m.Close()
				return nil, fmt.Errorf("%w: no driver heartbeat detected", ErrDriverTimeout)
			}
			time.Sleep(DefaultAwaitingIdleSleepDuration)
		}
		nowMs := time.Now().UnixMilli()
		if m.toDriver.ConsumerHeartbeatTime() < nowMs-driverTimeout.Milliseconds() {
			m.Close()
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("%w: no driver heartbeat detected", ErrDriverTimeout)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return m, nil
	}
}

// IsDriverActive reports whether a media driver is active in the given Aeron directory, i.e. its CnC file exists and
// the driver's to-driver consumer heartbeat is no older than driverTimeout (CommonContext.isDriverActive). The
// optional logger receives informational messages. An error is returned for incompatible CnC files or when the CnC
// file exists but is not initialised within driverTimeout.
func IsDriverActive(aeronDir string, driverTimeout time.Duration, logger func(string)) (bool, error) {
	cncFile := CncFileName(aeronDir)
	if !cncFileUsable(cncFile) {
		return false, nil
	}
	if logger != nil {
		logger("INFO: Aeron CnC file exists: " + cncFile)
	}
	deadline := time.Now().Add(driverTimeout)
	for {
		m, err := mapCncFile(cncFile)
		if err != nil {
			return false, err
		}
		if m == nil {
			if time.Now().After(deadline) {
				return false, fmt.Errorf("%w: CnC file is created but not initialised", ErrDriverTimeout)
			}
			time.Sleep(time.Millisecond)
			continue
		}
		ageMs := time.Now().UnixMilli() - m.toDriver.ConsumerHeartbeatTime()
		m.Close()
		if logger != nil {
			logger(fmt.Sprintf("INFO: Aeron toDriver consumer heartbeat age is (ms): %d", ageMs))
		}
		return ageMs <= driverTimeout.Milliseconds(), nil
	}
}

// RequestDriverTermination sends a TERMINATE_DRIVER command with the given token to the driver in aeronDir
// (CommonContext.requestDriverTermination). It returns true if the command was written. The driver only acts on it
// if its TerminationValidator accepts the token.
func RequestDriverTermination(aeronDir string, token []byte) (bool, error) {
	cncFile := CncFileName(aeronDir)
	if !cncFileUsable(cncFile) {
		return false, nil
	}
	m, err := mapCncFile(cncFile)
	if err != nil || m == nil {
		return false, err
	}
	defer m.Close()

	clientID := m.toDriver.NextCorrelationID()
	return writeTerminateDriver(&m.toDriver, clientID, token), nil
}

// writeTerminateDriver encodes a TerminateDriverFlyweight: clientId, correlationId, tokenLength, token bytes.
func writeTerminateDriver(toDriver *rb.ManyToOne, clientID int64, token []byte) bool {
	length := 8 + 8 + 4 + len(token)
	msg := make([]byte, length)
	buf := atomic.MakeBuffer(unsafe.Pointer(&msg[0]), length)
	buf.PutInt64(0, clientID)
	buf.PutInt64(8, NullValue)
	buf.PutInt32(16, int32(len(token)))
	if len(token) > 0 {
		buf.PutBytesSlice(20, token)
	}
	return toDriver.Write(terminateDriverMsgTypeID, buf, 0, int32(length))
}

// parseDurationConfig parses a duration in the format accepted by Aeron's getDurationInNanos / C
// aeron_config_parse_duration_ns: a plain integer is nanoseconds; suffixes ns, us, ms, s are supported.
func parseDurationConfig(value string) (time.Duration, error) {
	v := strings.TrimSpace(strings.ToLower(value))
	mult := time.Nanosecond
	for _, s := range []struct {
		suffix string
		mult   time.Duration
	}{{"ns", time.Nanosecond}, {"us", time.Microsecond}, {"ms", time.Millisecond}, {"s", time.Second}} {
		if strings.HasSuffix(v, s.suffix) {
			v = strings.TrimSpace(strings.TrimSuffix(v, s.suffix))
			mult = s.mult
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	return time.Duration(n) * mult, nil
}

func envDuration(name string, def time.Duration) time.Duration {
	if s, ok := os.LookupEnv(name); ok && s != "" {
		if d, err := parseDurationConfig(s); err == nil {
			return d
		}
		logger.Warningf("ignoring invalid %s=%q", name, s)
	}
	return def
}

func envMillis(name string, def time.Duration) time.Duration {
	if s, ok := os.LookupEnv(name); ok && s != "" {
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
		logger.Warningf("ignoring invalid %s=%q", name, s)
	}
	return def
}

func envBool(name string, def bool) bool {
	if s, ok := os.LookupEnv(name); ok && s != "" {
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
		logger.Warningf("ignoring invalid %s=%q", name, s)
	}
	return def
}
