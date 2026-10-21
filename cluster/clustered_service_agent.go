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
package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	syncatomic "sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/status"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const NullValue = -1
const NullPosition = -1
const markFileUpdateIntervalMs = 1000
const terminationAckAttempts = 5
const slowTickIntervalNs = int64(time.Millisecond)
const closeSessionAttempts = 3

// maxUdpPayloadLength mirrors io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH.
const maxUdpPayloadLength = 65504

const (
	commitPosCounterTypeId     = 203
	recoveryStateCounterTypeId = 204
)

var logger = logging.MustGetLogger("cluster")

// ClusterTerminationError is returned from StartAndRun when the service is terminated by the consensus module
// (Java: ClusterTerminationException). Expected is true for a requested termination at the termination position.
type ClusterTerminationError struct {
	Expected bool
}

func (e *ClusterTerminationError) Error() string {
	return fmt.Sprintf("cluster terminated, expected=%v", e.Expected)
}

type lifecycleCallback string

const (
	lifecycleNone             lifecycleCallback = ""
	lifecycleOnStart          lifecycleCallback = "onStart"
	lifecycleOnTerminate      lifecycleCallback = "onTerminate"
	lifecycleOnRoleChange     lifecycleCallback = "onRoleChange"
	lifecycleDoBackgroundWork lifecycleCallback = "doBackgroundWork"
)

// ClusteredServiceAgent hosts a ClusteredService and implements Cluster.
// Mirrors io.aeron.cluster.service.ClusteredServiceAgent (1.53.3).
type ClusteredServiceAgent struct {
	aeronClient              *aeron.Aeron
	ownsAeronClient          bool
	aeronCtx                 *aeron.Context
	opts                     *Options
	proxy                    *consensusModuleProxy
	counters                 *counters.Reader
	serviceAdapter           *serviceAdapter
	logAdapter               *boundedLogAdapter
	logSubscription          *aeron.Subscription
	markFile                 *ClusterMarkFile
	activeLogEvent           *activeLogEvent
	lastSlowTickNs           int64
	markFileUpdateDeadlineMs int64
	logPosition              int64
	clusterTime              int64
	timeUnit                 codecs.ClusterTimeUnitEnum
	memberId                 int32
	ackId                    int64
	terminationPosition      int64
	terminationAckRequired   bool
	requestedAckPosition     int64
	standbySnapshotFlags     int32
	isServiceActive          bool
	isTerminated             bool
	terminationErr           error
	activeLifecycleCallback  lifecycleCallback
	role                     Role
	service                  ClusteredService
	backgroundWorker         BackgroundWorker
	sessionByIdMap           map[int64]*containerClientSession
	sessions                 []*containerClientSession
	commitPosition           syncatomic.Pointer[counters.ReadableCounter]
	unavailableCounterRegId  int64
	sessionMsgHdrBuffer      *atomic.Buffer
	messageBuffer            *atomic.Buffer
}

func NewClusteredServiceAgent(
	aeronCtx *aeron.Context,
	options *Options,
	service ClusteredService,
) (*ClusteredServiceAgent, error) {
	if !isIpcChannel(options.ArchiveOptions.RequestChannel) {
		return nil, fmt.Errorf("archive request channel must be IPC: %s", options.ArchiveOptions.RequestChannel)
	}
	if !isIpcChannel(options.ArchiveOptions.ResponseChannel) {
		return nil, fmt.Errorf("archive response channel must be IPC: %s", options.ArchiveOptions.ResponseChannel)
	}
	if options.ServiceId < 0 || options.ServiceId > 127 {
		return nil, fmt.Errorf("serviceId is outside allowed range (0-127): %d", options.ServiceId)
	}
	if service == nil {
		return nil, errors.New("service is nil")
	}
	if options.AppVersionValidator == nil {
		options.AppVersionValidator = SemanticVersionMajorValidator
	}

	logging.SetLevel(options.Loglevel, "cluster")

	aeronClient := options.AeronClient
	ownsAeronClient := false
	if aeronClient == nil {
		var err error
		if aeronClient, err = aeron.Connect(aeronCtx); err != nil {
			return nil, err
		}
		ownsAeronClient = true
	}
	closeOnErr := func(err error) (*ClusteredServiceAgent, error) {
		if ownsAeronClient {
			_ = aeronClient.Close()
		}
		return nil, err
	}

	pub, err := aeronClient.AddExclusivePublication(options.ControlChannel, options.ConsensusModuleStreamId)
	if err != nil {
		return closeOnErr(err)
	}
	sub, err := aeronClient.AddSubscription(options.ControlChannel, options.ServiceStreamId)
	if err != nil {
		return closeOnErr(err)
	}

	cmf, err := NewClusterMarkFile(
		options.ClusterDir+"/"+ServiceMarkFileName(options.ServiceId), ClusterComponentContainer,
		ErrorBufferLength)
	if err != nil {
		return closeOnErr(err)
	}

	agent := &ClusteredServiceAgent{
		aeronClient:          aeronClient,
		ownsAeronClient:      ownsAeronClient,
		opts:                 options,
		aeronCtx:             aeronCtx,
		proxy:                newConsensusModuleProxy(pub),
		counters:             aeronClient.CounterReader(),
		markFile:             cmf,
		role:                 Follower,
		service:              service,
		memberId:             NullValue,
		logPosition:          NullPosition,
		terminationPosition:  NullPosition,
		requestedAckPosition: NullPosition,
		standbySnapshotFlags: ClusterActionFlagsDefault,
		sessionByIdMap:       map[int64]*containerClientSession{},
		sessionMsgHdrBuffer:  newSessionMessageHeaderBuffer(),
	}
	agent.backgroundWorker, _ = service.(BackgroundWorker)
	agent.serviceAdapter = newServiceAdapter(sub, agent)
	agent.logAdapter = newBoundedLogAdapter(options, agent)

	cmf.SetArchiveStreamId(options.ArchiveOptions.RequestStream)
	cmf.SetServiceStreamId(options.ServiceStreamId)
	cmf.SetConsensusModuleStreamId(options.ConsensusModuleStreamId)
	cmf.SetIngressStreamId(NullValue)
	cmf.SetMemberId(NullValue)
	cmf.SetServiceId(options.ServiceId)
	cmf.SetClusterId(options.ClusterId)
	cmf.EncodeStrings(filepath.Dir(aeronCtx.CncFileName()), options.ControlChannel, "", options.ServiceName, "", "")
	cmf.SignalReady(time.Now().UnixMilli())

	return agent, nil
}

func isIpcChannel(channel string) bool {
	uri, err := aeron.ParseChannelUri(channel)
	return err == nil && uri.IsIpc()
}

func newSessionMessageHeaderBuffer() *atomic.Buffer {
	buf := atomic.MakeBuffer(make([]byte, SBEHeaderLength+SessionMessageHeaderLength))
	buf.PutUInt16(0, SessionMessageHdrBlockLength)
	buf.PutUInt16(2, SessionMessageHeaderTemplateId)
	buf.PutUInt16(4, ClusterSchemaId)
	buf.PutUInt16(6, ClusterSchemaVersion)
	return buf
}

// ServiceMarkFileName returns the mark file name for a service (Java ClusterMarkFile.markFilenameForService).
func ServiceMarkFileName(serviceId int32) string {
	return "cluster-mark-service-" + strconv.Itoa(int(serviceId)) + ".dat"
}

// StartAndRun recovers state, then runs the duty cycle until the service is terminated. A requested termination
// returns nil; failures are returned as errors (Java errors/exceptions surfaced from the agent).
func (agent *ClusteredServiceAgent) StartAndRun() (err error) {
	defer agent.onClose()
	defer func() {
		if r := recover(); r != nil {
			err = agent.recoveredError(r)
		}
	}()
	if err := agent.OnStart(); err != nil {
		agent.markFile.SignalFailedStart()
		return err
	}
	for !agent.isTerminated {
		agent.opts.IdleStrategy.Idle(agent.DoWork())
	}
	if t, ok := agent.terminationErr.(*ClusterTerminationError); ok && t.Expected {
		return nil
	}
	return agent.terminationErr
}

func (agent *ClusteredServiceAgent) recoveredError(r interface{}) error {
	switch e := r.(type) {
	case *ClusterTerminationError:
		if e.Expected {
			return nil
		}
		return e
	case error:
		return e
	default:
		return fmt.Errorf("%v", r)
	}
}

// OnStart awaits the consensus module counters and recovers service state from the latest snapshot.
func (agent *ClusteredServiceAgent) OnStart() error {
	agent.unavailableCounterRegId = agent.aeronClient.AddUnavailableCounterHandler(
		unavailableCounterFunc(agent.counterUnavailable))
	if err := agent.awaitCommitPositionCounter(); err != nil {
		return err
	}
	if err := agent.recoverState(); err != nil {
		return err
	}
	agent.isServiceActive = true
	return nil
}

func (agent *ClusteredServiceAgent) loadCommitPosition() *counters.ReadableCounter {
	return agent.commitPosition.Load()
}

func (agent *ClusteredServiceAgent) storeCommitPosition(c *counters.ReadableCounter) {
	agent.commitPosition.Store(c)
}

type unavailableCounterFunc func(countersReader *counters.Reader, registrationId int64, counterId int32)

func (f unavailableCounterFunc) Handle(countersReader *counters.Reader, registrationId int64, counterId int32) {
	f(countersReader, registrationId, counterId)
}

// counterUnavailable closes the commit position counter when the consensus module removes it (Java
// ClusteredServiceAgent.counterUnavailable). Called from the client conductor goroutine.
func (agent *ClusteredServiceAgent) counterUnavailable(_ *counters.Reader, registrationId int64, counterId int32) {
	if commitPos := agent.loadCommitPosition(); commitPos != nil &&
		commitPos.CounterId == counterId && commitPos.RegistrationId == registrationId {
		commitPos.Close()
	}
}

func (agent *ClusteredServiceAgent) onClose() {
	if agent.unavailableCounterRegId != 0 && !agent.aeronClient.IsClosed() {
		agent.aeronClient.RemoveUnavailableCounterHandlerByID(agent.unavailableCounterRegId)
	}
	if agent.isServiceActive {
		agent.isServiceActive = false
		agent.invokeOnTerminate()
	}
	for _, session := range agent.sessions {
		session.disconnect()
	}
	if agent.logAdapter.image != nil {
		_ = agent.logAdapter.Close()
	}
	if agent.logSubscription != nil {
		closeSubscription(agent.logSubscription)
		agent.logSubscription = nil
	}
	agent.serviceAdapter.close()
	closePublication(agent.proxy.publication)
	agent.markFile.SignalTerminated()
	agent.markFile.Close()
	if agent.ownsAeronClient && !agent.aeronClient.IsClosed() {
		if err := agent.aeronClient.Close(); err != nil {
			logger.Errorf("error closing aeron client: %v", err)
		}
	}
}

func (agent *ClusteredServiceAgent) awaitCommitPositionCounter() error {
	for {
		id := agent.counters.FindCounter(commitPosCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			return keyBuffer.GetInt32(0) == agent.opts.ClusterId
		})
		if id != counters.NullCounterId {
			commitPos, err := counters.NewReadableRegisteredCounter(
				agent.counters, agent.counters.GetCounterRegistrationID(id), id)
			if err != nil {
				return err
			}
			logger.Debugf("found commit position counter - id=%d value=%d", id, commitPos.Get())
			agent.storeCommitPosition(commitPos)
			return nil
		}
		agent.idle(0)
	}
}

func (agent *ClusteredServiceAgent) recoverState() error {
	counterId, leadershipTermId := agent.awaitRecoveryCounter()
	logger.Debugf("found recovery counter - id=%d leadershipTermId=%d logPos=%d clusterTime=%d",
		counterId, leadershipTermId, agent.logPosition, agent.clusterTime)
	agent.sessionMsgHdrBuffer.PutInt64(SBEHeaderLength, leadershipTermId)

	snapshotRecId := int64(NullValue)
	err := func() (err error) {
		agent.activeLifecycleCallback = lifecycleOnStart
		defer func() {
			agent.activeLifecycleCallback = lifecycleNone
			if r := recover(); r != nil {
				err = agent.recoveredError(r)
				if err == nil {
					err = errors.New("cluster terminated during start")
				}
			}
		}()
		if leadershipTermId == NullValue {
			agent.service.OnStart(agent, nil)
			return nil
		}
		serviceCount, err := agent.counters.GetKeyPartInt32(counterId, 28)
		if err != nil {
			return err
		}
		if agent.opts.ServiceId >= serviceCount {
			return fmt.Errorf("serviceId=%d not in recovery state serviceCount=%d", agent.opts.ServiceId, serviceCount)
		}
		if snapshotRecId, err = agent.counters.GetKeyPartInt64(
			counterId, 32+(agent.opts.ServiceId*util.SizeOfInt64)); err != nil {
			return err
		}
		return agent.loadSnapshot(snapshotRecId)
	}()

	// Java acks with relevantId=NULL_VALUE when recovery fails so the consensus module does not wait on the service.
	relevantId := agent.aeronClient.ClientID()
	if err != nil {
		relevantId = NullValue
	}
	id := agent.nextAckId()
	for !agent.proxy.ack(agent.logPosition, agent.clusterTime, id, relevantId, agent.opts.ServiceId) {
		agent.idle(0)
	}
	if err != nil {
		return fmt.Errorf("failed to start service=%d leadershipTermId=%d logPosition=%d clusterTime=%d "+
			"snapshotRecordingId=%d: %w", agent.opts.ServiceId, leadershipTermId, agent.logPosition,
			agent.clusterTime, snapshotRecId, err)
	}
	return nil
}

func (agent *ClusteredServiceAgent) invokeOnTerminate() {
	agent.activeLifecycleCallback = lifecycleOnTerminate
	defer func() {
		agent.activeLifecycleCallback = lifecycleNone
		if r := recover(); r != nil {
			logger.Errorf("error in OnTerminate: %v", r)
		}
	}()
	agent.service.OnTerminate(agent)
}

func (agent *ClusteredServiceAgent) invokeBackgroundWork(nowNs int64) int {
	if agent.backgroundWorker == nil {
		return 0
	}
	prev := agent.activeLifecycleCallback
	agent.activeLifecycleCallback = lifecycleDoBackgroundWork
	defer func() { agent.activeLifecycleCallback = prev }()
	return agent.backgroundWorker.DoBackgroundWork(nowNs)
}

func (agent *ClusteredServiceAgent) awaitRecoveryCounter() (int32, int64) {
	for {
		var leadershipTermId int64
		id := agent.counters.FindCounter(recoveryStateCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			if keyBuffer.GetInt32(24) == agent.opts.ClusterId {
				leadershipTermId = keyBuffer.GetInt64(0)
				agent.logPosition = keyBuffer.GetInt64(8)
				agent.clusterTime = keyBuffer.GetInt64(16)
				return true
			}
			return false
		})
		if id != counters.NullCounterId {
			return id, leadershipTermId
		}
		agent.idle(0)
	}
}

// snapshotArchiveOptions reuses the agent's Aeron client for snapshot archive connections, like Java's
// AeronArchive.Context.aeron(aeron): the agent's aeron.Context is already bound to that client.
func (agent *ClusteredServiceAgent) snapshotArchiveOptions() *archive.Options {
	opts := agent.opts.ArchiveOptions.Clone()
	opts.Aeron = agent.aeronClient
	opts.OwnsAeronClient = false
	return opts
}

func (agent *ClusteredServiceAgent) loadSnapshot(recordingId int64) error {
	arch, err := archive.NewArchive(agent.snapshotArchiveOptions(), agent.aeronCtx)
	if err != nil {
		return err
	}
	defer closeArchive(arch)

	channel := agent.opts.ReplayChannel
	streamId := agent.opts.ReplayStreamId
	replaySessionId, err := arch.StartReplay(recordingId, 0, NullValue, channel, streamId)
	if err != nil {
		return err
	}
	subChannel, err := archive.AddSessionIdToChannel(channel, archive.ReplaySessionIdToSessionId(replaySessionId))
	if err != nil {
		return err
	}

	logger.Debugf("replaying snapshot - recId=%d sessionId=%d streamId=%d", recordingId, replaySessionId, streamId)
	subscription, err := arch.AddSubscription(subChannel, streamId)
	if err != nil {
		return err
	}
	defer closeSubscription(subscription)

	img := agent.awaitImage(archive.ReplaySessionIdToSessionId(replaySessionId), subscription)
	loader := newSnapshotLoader(agent, img)
	for {
		fragments := loader.poll()
		if loader.isDone {
			break
		}
		if fragments == 0 {
			if _, err := arch.PollForErrorResponse(); err != nil {
				return err
			}
			if img.IsClosed() {
				return fmt.Errorf("snapshot ended unexpectedly: recordingId=%d", recordingId)
			}
		}
		agent.idle(fragments)
	}
	if loader.err != nil {
		return loader.err
	}
	if !agent.opts.AppVersionValidator(agent.opts.AppVersion, loader.appVersion) {
		return newClusterError("incompatible app version: %v snapshot=%v",
			util.SemanticVersionToString(uint32(agent.opts.AppVersion)),
			util.SemanticVersionToString(uint32(loader.appVersion)))
	}
	agent.timeUnit = loader.timeUnit
	agent.service.OnStart(agent, img)
	return nil
}

func (agent *ClusteredServiceAgent) addSession(session *containerClientSession) {
	agent.sessionByIdMap[session.id] = session
	idx := sort.Search(len(agent.sessions), func(i int) bool { return agent.sessions[i].id >= session.id })
	agent.sessions = append(agent.sessions, nil)
	copy(agent.sessions[idx+1:], agent.sessions[idx:])
	agent.sessions[idx] = session
}

func (agent *ClusteredServiceAgent) removeSession(id int64) *containerClientSession {
	session, ok := agent.sessionByIdMap[id]
	if !ok {
		return nil
	}
	delete(agent.sessionByIdMap, id)
	idx := sort.Search(len(agent.sessions), func(i int) bool { return agent.sessions[i].id >= id })
	if idx < len(agent.sessions) && agent.sessions[idx].id == id {
		agent.sessions = append(agent.sessions[:idx], agent.sessions[idx+1:]...)
	}
	return session
}

func (agent *ClusteredServiceAgent) addSessionFromSnapshot(session *containerClientSession) {
	agent.addSession(session)
}

func (agent *ClusteredServiceAgent) checkForClockTick(nowNs int64) bool {
	if agent.isTerminated {
		return false
	}
	if agent.aeronClient.IsClosed() {
		agent.abort(newClusterError("unexpected Aeron close"))
	}
	if nowNs-agent.lastSlowTickNs <= slowTickIntervalNs {
		return false
	}
	agent.lastSlowTickNs = nowNs
	if commitPos := agent.loadCommitPosition(); commitPos != nil && commitPos.IsClosed() {
		logger.Warningf("commit-pos counter unexpectedly closed, terminating")
		agent.abort(&ClusterTerminationError{Expected: true})
	}
	nowMs := nowNs / int64(time.Millisecond)
	if nowMs >= agent.markFileUpdateDeadlineMs {
		agent.markFileUpdateDeadlineMs = nowMs + markFileUpdateIntervalMs
		agent.markFile.UpdateActivityTimestamp(nowMs)
	}
	return true
}

func (agent *ClusteredServiceAgent) abort(err error) {
	agent.isServiceActive = false
	agent.isTerminated = true
	agent.terminationErr = err
	panic(err)
}

func (agent *ClusteredServiceAgent) pollServiceAdapter() int {
	work := agent.serviceAdapter.poll()

	if agent.activeLogEvent != nil && agent.logAdapter.image == nil {
		if err := agent.joinActiveLog(agent.activeLogEvent); err != nil {
			var regErr *aeron.RegistrationError
			if !errors.As(err, &regErr) {
				panic(err)
			}
			logger.Warningf("failed to join active log, will retry: %v", err)
		} else {
			agent.activeLogEvent = nil
		}
	}

	agent.checkServiceProtocolPositions()
	return work
}

// checkServiceProtocolPositions handles termination and requested acks once the log reaches their position.
func (agent *ClusteredServiceAgent) checkServiceProtocolPositions() {
	if agent.terminationPosition != NullPosition && agent.logPosition >= agent.terminationPosition {
		if agent.logPosition > agent.terminationPosition {
			logger.Errorf("service terminate: logPos=%d > terminationPos=%d", agent.logPosition, agent.terminationPosition)
		}
		agent.terminate(agent.logPosition == agent.terminationPosition)
		return
	}

	if agent.requestedAckPosition != NullPosition && agent.logPosition >= agent.requestedAckPosition {
		if agent.logPosition > agent.requestedAckPosition {
			logger.Errorf("service ack: logPos=%d > requestedAckPos=%d", agent.logPosition, agent.requestedAckPosition)
		}
		id := agent.nextAckId()
		for !agent.proxy.ack(agent.logPosition, agent.clusterTime, id, NullValue, agent.opts.ServiceId) {
			agent.idle(0)
		}
		agent.requestedAckPosition = NullPosition
	}
}

func (agent *ClusteredServiceAgent) terminate(isTerminationExpected bool) {
	agent.isServiceActive = false
	agent.invokeOnTerminate()

	if agent.terminationAckRequired {
		id := agent.nextAckId()
		for attempts := terminationAckAttempts; attempts > 0; attempts-- {
			if agent.proxy.ack(agent.logPosition, agent.clusterTime, id, NullValue, agent.opts.ServiceId) {
				break
			}
			agent.idle(0)
		}
	}
	agent.terminationPosition = NullPosition
	agent.isTerminated = true
	agent.terminationErr = &ClusterTerminationError{Expected: isTerminationExpected}
}

// DoWork performs one duty cycle of the agent and returns the work count.
func (agent *ClusteredServiceAgent) DoWork() int {
	work := 0
	nowNs := time.Now().UnixNano()

	if agent.checkForClockTick(nowNs) {
		work += agent.pollServiceAdapter()
	}
	if agent.isTerminated {
		return work
	}

	if agent.logAdapter.image != nil {
		polled := agent.logAdapter.poll(agent.loadCommitPosition().Get())
		work += polled
		if polled == 0 && agent.logAdapter.isDone() {
			agent.closeLog()
		}
	}

	work += agent.invokeBackgroundWork(nowNs)
	return work
}

func (agent *ClusteredServiceAgent) onJoinLog(
	logPosition int64,
	maxLogPosition int64,
	memberId int32,
	logSessionId int32,
	logStreamId int32,
	isStartup bool,
	role Role,
	logChannel string,
	isStandby bool,
) {
	logger.Debugf("onJoinLog - logPos=%d isStartup=%v role=%v isStandby=%v logChannel=%s",
		logPosition, isStartup, role, isStandby, logChannel)
	agent.logAdapter.maxLogPosition = logPosition
	agent.activeLogEvent = &activeLogEvent{
		logPosition:    logPosition,
		maxLogPosition: maxLogPosition,
		memberId:       memberId,
		logSessionId:   logSessionId,
		logStreamId:    logStreamId,
		isStartup:      isStartup,
		role:           role,
		logChannel:     logChannel,
		isStandby:      isStandby,
	}
}

func (agent *ClusteredServiceAgent) onServiceTerminationPosition(logPosition int64, ackRequired bool) {
	agent.terminationPosition = logPosition
	agent.terminationAckRequired = ackRequired
}

func (agent *ClusteredServiceAgent) onRequestServiceAck(logPosition int64) {
	agent.requestedAckPosition = logPosition
}

type activeLogEvent struct {
	logPosition    int64
	maxLogPosition int64
	memberId       int32
	logSessionId   int32
	logStreamId    int32
	isStartup      bool
	role           Role
	logChannel     string
	isStandby      bool
}

func (agent *ClusteredServiceAgent) joinActiveLog(event *activeLogEvent) error {
	if event.role != Leader {
		agent.disconnectEgress()
	}
	if event.isStandby {
		agent.standbySnapshotFlags = ClusterActionFlagsStandbySnapshot
	} else {
		agent.standbySnapshotFlags = ClusterActionFlagsDefault
	}

	channel := event.logChannel
	if uri, err := aeron.ParseChannelUri(channel); err == nil {
		uri.Set("alias", "log-sc-"+strconv.Itoa(int(agent.opts.ServiceId)))
		channel = uri.String()
	}
	logSub, err := agent.aeronClient.AddSubscription(channel, event.logStreamId)
	if err != nil {
		return err
	}
	img := agent.awaitImage(event.logSessionId, logSub)
	if img.JoinPosition() != agent.logPosition {
		closeSubscription(logSub)
		return newClusterError("cluster log must be contiguous for joining image: expectedPosition=%d joinPosition=%d",
			agent.logPosition, img.JoinPosition())
	}
	if event.logPosition != agent.logPosition {
		closeSubscription(logSub)
		return newClusterError("cluster log must be contiguous for active log event: expectedPosition=%d eventPosition=%d",
			agent.logPosition, event.logPosition)
	}
	agent.logSubscription = logSub
	agent.logAdapter.image = img
	agent.logAdapter.maxLogPosition = event.maxLogPosition

	id := agent.nextAckId()
	for !agent.proxy.ack(event.logPosition, agent.clusterTime, id, NullValue, agent.opts.ServiceId) {
		agent.idle(0)
	}

	agent.memberId = event.memberId
	agent.markFile.SetMemberId(agent.memberId)

	if event.role == Leader {
		for _, session := range agent.sessions {
			if agent.opts.IsRespondingService && !event.isStartup {
				session.connect(agent.aeronClient)
			}
			session.resetClosing()
		}
	}
	agent.setRole(event.role)
	return nil
}

func (agent *ClusteredServiceAgent) closeLog() {
	imageLogPos := agent.logAdapter.image.Position()
	if imageLogPos > agent.logPosition {
		agent.logPosition = imageLogPos
	}
	if err := agent.logAdapter.Close(); err != nil {
		logger.Errorf("error closing log image: %v", err)
	}
	if agent.logSubscription != nil {
		closeSubscription(agent.logSubscription)
		agent.logSubscription = nil
	}
	agent.disconnectEgress()
	agent.setRole(Follower)
}

func (agent *ClusteredServiceAgent) disconnectEgress() {
	for _, session := range agent.sessions {
		session.disconnect()
	}
}

func (agent *ClusteredServiceAgent) setRole(newRole Role) {
	if newRole != agent.role {
		agent.role = newRole
		agent.activeLifecycleCallback = lifecycleOnRoleChange
		defer func() { agent.activeLifecycleCallback = lifecycleNone }()
		agent.service.OnRoleChange(newRole)
	}
}

func (agent *ClusteredServiceAgent) awaitImage(sessionId int32, subscription *aeron.Subscription) aeron.Image {
	for {
		if img := subscription.ImageBySessionID(sessionId); img != nil {
			return img
		}
		agent.idle(0)
	}
}

func (agent *ClusteredServiceAgent) onSessionOpen(
	leadershipTermId int64,
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	responseStreamId int32,
	responseChannel string,
	encodedPrincipal []byte,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	if _, ok := agent.sessionByIdMap[clusterSessionId]; ok {
		panic(newClusterError("clashing open clusterSessionId=%d leadershipTermId=%d logPosition=%d",
			clusterSessionId, leadershipTermId, logPosition))
	}
	session := newContainerClientSession(clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, agent)
	if agent.role == Leader && agent.opts.IsRespondingService {
		session.connect(agent.aeronClient)
	}
	agent.addSession(session)
	agent.service.OnSessionOpen(session, timestamp)
}

func (agent *ClusteredServiceAgent) onSessionClose(
	leadershipTermId int64,
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	closeReason codecs.CloseReasonEnum,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp

	if session := agent.removeSession(clusterSessionId); session != nil {
		session.disconnect()
		agent.service.OnSessionClose(session, timestamp, closeReason)
	} else {
		logger.Errorf("onSessionClose: unknown session - id=%d leaderTermId=%d logPos=%d reason=%v",
			clusterSessionId, leadershipTermId, logPosition, closeReason)
	}
}

func (agent *ClusteredServiceAgent) onSessionMessage(
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	var clientSession ClientSession
	if s, ok := agent.sessionByIdMap[clusterSessionId]; ok {
		clientSession = s
	}
	agent.service.OnSessionMessage(clientSession, timestamp, buffer, offset, length, header)
}

func (agent *ClusteredServiceAgent) onNewLeadershipTermEvent(
	leadershipTermId int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberId int32,
	logSessionId int32,
	timeUnit codecs.ClusterTimeUnitEnum,
	appVersion int32,
) {
	if !agent.opts.AppVersionValidator(agent.opts.AppVersion, appVersion) {
		agent.abort(newClusterError("incompatible version: %v log=%v",
			util.SemanticVersionToString(uint32(agent.opts.AppVersion)),
			util.SemanticVersionToString(uint32(appVersion))))
	}
	agent.sessionMsgHdrBuffer.PutInt64(SBEHeaderLength, leadershipTermId)
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	agent.timeUnit = timeUnit

	agent.service.OnNewLeadershipTermEvent(
		leadershipTermId,
		logPosition,
		timestamp,
		termBaseLogPosition,
		leaderMemberId,
		logSessionId,
		timeUnit,
		appVersion)
}

func (agent *ClusteredServiceAgent) onServiceAction(
	leadershipTermId int64,
	logPos int64,
	timestamp int64,
	action codecs.ClusterActionEnum,
	flags int32,
) {
	agent.logPosition = logPos
	agent.clusterTime = timestamp
	if action != codecs.ClusterAction.SNAPSHOT || !agent.shouldSnapshot(flags) {
		return
	}
	recordingId, err := agent.takeSnapshot(logPos, leadershipTermId)
	if err != nil {
		logger.Errorf("take snapshot failed: %v", err)
		recordingId = NullValue
	}
	id := agent.nextAckId()
	for !agent.proxy.ack(logPos, timestamp, id, recordingId, agent.opts.ServiceId) {
		agent.idle(0)
	}
	if err != nil {
		panic(err)
	}
}

func (agent *ClusteredServiceAgent) shouldSnapshot(flags int32) bool {
	return flags == ClusterActionFlagsDefault || (flags&agent.standbySnapshotFlags) != 0
}

func (agent *ClusteredServiceAgent) onTimerEvent(logPosition int64, correlationId int64, timestamp int64) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	agent.service.OnTimerEvent(correlationId, timestamp)
}

func (agent *ClusteredServiceAgent) takeSnapshot(logPos int64, leadershipTermId int64) (int64, error) {
	pub, err := agent.aeronClient.AddExclusivePublication(agent.opts.SnapshotChannel, agent.opts.SnapshotStreamId)
	if err != nil {
		return NullValue, err
	}
	defer closePublication(pub)

	arch, err := archive.NewArchive(agent.snapshotArchiveOptions(), agent.aeronCtx)
	if err != nil {
		return NullValue, err
	}
	defer closeArchive(arch)

	channel, err := archive.AddSessionIdToChannel(agent.opts.SnapshotChannel, pub.SessionID())
	if err != nil {
		return NullValue, err
	}
	if _, err := arch.StartRecording(channel, agent.opts.SnapshotStreamId, true, true); err != nil {
		return NullValue, err
	}

	counterId, recordingId, err := agent.awaitRecordingCounter(pub.SessionID(), arch.ArchiveID(), arch)
	if err != nil {
		return NullValue, err
	}
	logger.Debugf("takeSnapshot - got recordingId: %d", recordingId)

	snapshotTaker := newSnapshotTaker(agent.opts, pub)
	if err := snapshotTaker.markBegin(logPos, leadershipTermId, agent.timeUnit, agent.opts.AppVersion); err != nil {
		return NullValue, err
	}
	for _, session := range agent.sessions {
		if err := snapshotTaker.snapshotSession(session); err != nil {
			return NullValue, err
		}
	}
	if err := snapshotTaker.markEnd(logPos, leadershipTermId, agent.timeUnit, agent.opts.AppVersion); err != nil {
		return NullValue, err
	}
	agent.checkForClockTick(time.Now().UnixNano())
	if _, err := arch.PollForErrorResponse(); err != nil {
		return NullValue, err
	}
	agent.service.OnTakeSnapshot(pub)

	if err := agent.awaitRecordingComplete(recordingId, pub.Position(), counterId, arch); err != nil {
		return NullValue, err
	}
	return recordingId, nil
}

func (agent *ClusteredServiceAgent) awaitRecordingCounter(sessionId int32, archiveId int64,
	arch *archive.Archive) (int32, int64, error) {
	start := time.Now()
	for {
		counterId := status.FindCounterIdBySession(agent.counters, sessionId, archiveId)
		if counterId != counters.NullCounterId {
			return counterId, status.GetRecordingId(agent.counters, counterId), nil
		}
		if time.Since(start) >= agent.opts.Timeout {
			return counters.NullCounterId, NullValue, fmt.Errorf(
				"timed out waiting for recording counter for sessionId=%d archiveId=%d", sessionId, archiveId)
		}
		if _, err := arch.PollForErrorResponse(); err != nil {
			return counters.NullCounterId, NullValue, err
		}
		agent.idle(0)
	}
}

func (agent *ClusteredServiceAgent) awaitRecordingComplete(recordingId, position int64, counterId int32,
	arch *archive.Archive) error {
	for agent.counters.GetCounterValue(counterId) < position {
		if !status.IsActive(agent.counters, counterId, recordingId) {
			return newClusterError("recording stopped unexpectedly: recordingId=%d", recordingId)
		}
		if _, err := arch.PollForErrorResponse(); err != nil {
			return err
		}
		agent.idle(0)
	}
	return nil
}

func (agent *ClusteredServiceAgent) nextAckId() int64 {
	ackId := agent.ackId
	agent.ackId++
	return ackId
}

func (agent *ClusteredServiceAgent) checkForValidInvocation() {
	if agent.activeLifecycleCallback != lifecycleNone {
		panic(newClusterError("sending messages or scheduling timers is not allowed from %s",
			agent.activeLifecycleCallback))
	}
	if agent.isTerminated {
		panic(newClusterError("cluster service is terminated"))
	}
}

func (agent *ClusteredServiceAgent) offerToSession(
	clusterSessionId int64,
	publication *aeron.Publication,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	agent.checkForValidInvocation()
	if agent.role != Leader {
		return ClientSessionMockedOffer
	}
	if publication == nil {
		return aeron.NotConnected
	}
	hdrBuf := agent.sessionMsgHdrBuffer
	hdrBuf.PutInt64(SBEHeaderLength+8, clusterSessionId)
	hdrBuf.PutInt64(SBEHeaderLength+16, agent.clusterTime)
	return publication.Offer2(hdrBuf, 0, hdrBuf.Capacity(), buffer, offset, length, reservedValueSupplier)
}

func (agent *ClusteredServiceAgent) tryClaimToSession(
	clusterSessionId int64,
	publication *aeron.Publication,
	length int32,
	claim *logbuffer.Claim,
) int64 {
	agent.checkForValidInvocation()
	if agent.role != Leader {
		if maxPayloadLength := maxUdpPayloadLength - logbuffer.DataFrameHeader.Length - SBEHeaderLength -
			SessionMessageHeaderLength; length > maxPayloadLength {
			panic(fmt.Errorf("claim exceeds maxPayloadLength=%d, length=%d", maxPayloadLength, length))
		}
		claimLength := logbuffer.DataFrameHeader.Length + SBEHeaderLength + SessionMessageHeaderLength + length
		if agent.messageBuffer == nil || agent.messageBuffer.Capacity() < claimLength {
			agent.messageBuffer = atomic.MakeBuffer(make([]byte, claimLength))
		}
		claim.Wrap(agent.messageBuffer, 0, claimLength)
		return ClientSessionMockedOffer
	}
	if publication == nil {
		return aeron.NotConnected
	}
	result := publication.TryClaim(SBEHeaderLength+SessionMessageHeaderLength+length, claim)
	if result > 0 {
		agent.writeSessionHeader(claim, clusterSessionId)
	}
	return result
}

func (agent *ClusteredServiceAgent) writeSessionHeader(claim *logbuffer.Claim, clusterSessionId int64) {
	hdrBuf := agent.sessionMsgHdrBuffer
	hdrBuf.PutInt64(SBEHeaderLength+8, clusterSessionId)
	hdrBuf.PutInt64(SBEHeaderLength+16, agent.clusterTime)
	claim.Buffer().PutBytes(claim.Offset(), hdrBuf, 0, hdrBuf.Capacity())
}

// idle runs the idle strategy while checking for liveness so long-running operations do not stall the agent.
func (agent *ClusteredServiceAgent) idle(workCount int) {
	agent.opts.IdleStrategy.Idle(workCount)
	if workCount <= 0 {
		nowNs := time.Now().UnixNano()
		agent.checkForClockTick(nowNs)
		if agent.isServiceActive {
			agent.invokeBackgroundWork(nowNs)
		}
	}
}

// Idle implements idlestrategy.Idler so the agent can be returned from Cluster.IdleStrategy().
func (agent *ClusteredServiceAgent) Idle(workCount int) {
	agent.idle(workCount)
}

func closeArchive(arch *archive.Archive) {
	if err := arch.Close(); err != nil {
		logger.Errorf("error closing archive connection: %v", err)
	}
}

func closeSubscription(sub *aeron.Subscription) {
	if err := sub.Close(); err != nil {
		logger.Errorf("error closing subscription, streamId=%d channel=%s: %v", sub.StreamID(), sub.Channel(), err)
	}
}

func closePublication(pub aeron.Publisher) {
	if err := pub.Close(); err != nil {
		logger.Errorf("error closing publication, streamId=%d channel=%s: %v", pub.StreamID(), pub.Channel(), err)
	}
}

// BEGIN CLUSTER IMPLEMENTATION

func (agent *ClusteredServiceAgent) MemberId() int32 {
	return agent.memberId
}

func (agent *ClusteredServiceAgent) Role() Role {
	return agent.role
}

func (agent *ClusteredServiceAgent) LogPosition() int64 {
	return agent.logPosition
}

func (agent *ClusteredServiceAgent) Aeron() *aeron.Aeron {
	return agent.aeronClient
}

func (agent *ClusteredServiceAgent) Options() *Options {
	return agent.opts
}

func (agent *ClusteredServiceAgent) GetClientSession(clusterSessionId int64) ClientSession {
	if s, ok := agent.sessionByIdMap[clusterSessionId]; ok {
		return s
	}
	return nil
}

func (agent *ClusteredServiceAgent) ClientSessions() []ClientSession {
	result := make([]ClientSession, len(agent.sessions))
	for i, s := range agent.sessions {
		result[i] = s
	}
	return result
}

func (agent *ClusteredServiceAgent) ForEachClientSession(action func(ClientSession)) {
	for _, s := range agent.sessions {
		action(s)
	}
}

func (agent *ClusteredServiceAgent) CloseClientSession(clusterSessionId int64) bool {
	agent.checkForValidInvocation()
	session, ok := agent.sessionByIdMap[clusterSessionId]
	if !ok {
		logger.Errorf("closeClientSession: unknown clusterSessionId=%d", clusterSessionId)
		return false
	}
	if session.IsClosing() {
		return true
	}
	for attempts := closeSessionAttempts; attempts > 0; attempts-- {
		if agent.proxy.closeSession(clusterSessionId) {
			session.markClosing()
			return true
		}
		agent.idle(0)
	}
	return false
}

func (agent *ClusteredServiceAgent) Time() int64 {
	return agent.clusterTime
}

func (agent *ClusteredServiceAgent) TimeUnit() codecs.ClusterTimeUnitEnum {
	return agent.timeUnit
}

func (agent *ClusteredServiceAgent) IdleStrategy() idlestrategy.Idler {
	return agent
}

func (agent *ClusteredServiceAgent) ScheduleTimer(correlationId int64, deadline int64) bool {
	agent.checkForValidInvocation()
	return agent.proxy.scheduleTimer(correlationId, deadline)
}

func (agent *ClusteredServiceAgent) CancelTimer(correlationId int64) bool {
	agent.checkForValidInvocation()
	return agent.proxy.cancelTimer(correlationId)
}

func (agent *ClusteredServiceAgent) Offer(buffer *atomic.Buffer, offset, length int32) int64 {
	agent.checkForValidInvocation()
	hdrBuf := agent.sessionMsgHdrBuffer
	hdrBuf.PutInt64(SBEHeaderLength+8, int64(agent.opts.ServiceId))
	hdrBuf.PutInt64(SBEHeaderLength+16, agent.clusterTime)
	return agent.proxy.offer2(hdrBuf, 0, hdrBuf.Capacity(), buffer, offset, length)
}

func (agent *ClusteredServiceAgent) TryClaim(length int32, claim *logbuffer.Claim) int64 {
	agent.checkForValidInvocation()
	result := agent.proxy.tryClaim(SBEHeaderLength+SessionMessageHeaderLength+length, claim)
	if result > 0 {
		agent.writeSessionHeader(claim, int64(agent.opts.ServiceId))
	}
	return result
}

// END CLUSTER IMPLEMENTATION
