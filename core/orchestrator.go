package core

import (
	"context"
	"errors"
	"time"

	"github.com/livepeer/go-tools/drivers"

	"github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"
)

const maxSegmentChannels = 4

// this is set to be higher than httpPushTimeout in server/mediaserver.go so that B has a chance to end the session
// based on httpPushTimeout before transcodeLoopTimeout is reached
var transcodeLoopTimeout = 70 * time.Second

// Gives us more control of "timeout" / cancellation behavior during testing
var transcodeLoopContext = func() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), transcodeLoopTimeout)
}

// // Transcoder / orchestrator RPC interface implementation
// type orchestrator struct {
// 	address ethcommon.Address
// 	node    *LivepeerNode
// 	rm      common.RoundsManager
// 	secret  []byte
// }

// func (orch *orchestrator) ServiceURI() *url.URL {
// 	return orch.node.GetServiceURI()
// }

// func (orch *orchestrator) Address() ethcommon.Address {
// 	return orch.address
// }

// func (orch *orchestrator) TranscoderSecret() string {
// 	return orch.node.OrchSecret
// }

// func (orch *orchestrator) CheckCapacity(mid ManifestID) error {
// 	orch.node.segmentMutex.RLock()
// 	defer orch.node.segmentMutex.RUnlock()
// 	if _, ok := orch.node.SegmentChans[mid]; ok {
// 		return nil
// 	}
// 	if len(orch.node.SegmentChans) >= MaxSessions {
// 		return ErrOrchCap
// 	}
// 	return nil
// }

// func (orch *orchestrator) ServeTranscoder(stream net.Transcoder_RegisterTranscoderServer, capacity int, capabilities *net.Capabilities) {
// 	orch.node.serveTranscoder(stream, capacity, capabilities)
// }

// func (orch *orchestrator) TranscoderResults(tcID int64, res *RemoteTranscoderResult) {
// 	orch.node.TranscoderManager.transcoderResults(tcID, res)
// }

// func (orch *orchestrator) Capabilities() *net.Capabilities {
// 	if orch.node == nil {
// 		return nil
// 	}
// 	return orch.node.Capabilities.ToNetCapabilities()
// }

// func (orch *orchestrator) AuthToken(sessionID string, expiration int64) *net.AuthToken {
// 	h := hmac.New(sha256.New, orch.secret)
// 	msg := append([]byte(sessionID), new(big.Int).SetInt64(expiration).Bytes()...)
// 	h.Write(msg)

// 	return &net.AuthToken{
// 		Token:      h.Sum(nil),
// 		SessionId:  sessionID,
// 		Expiration: expiration,
// 	}
// }

// func (orch *orchestrator) isActive(addr ethcommon.Address) (bool, error) {
// 	filter := &common.DBOrchFilter{
// 		CurrentRound: orch.rm.LastInitializedRound(),
// 		Addresses:    []ethcommon.Address{addr},
// 	}
// 	orchs, err := orch.node.Database.SelectOrchs(filter)
// 	if err != nil {
// 		return false, err
// 	}

// 	return len(orchs) > 0, nil
// }

// // LivepeerNode transcode methods

// var ErrOrchBusy = errors.New("OrchestratorBusy")
// var ErrOrchCap = errors.New("OrchestratorCapped")

type TranscodeResult struct {
	Err           error
	Sig           []byte
	TranscodeData *TranscodeData
	OS            drivers.OSSession
}

// TranscodeData contains the transcoding output for an input segment
type TranscodeData struct {
	Segments   []*TranscodedSegmentData
	Pixels     int64 // Decoded pixels
	Detections []ffmpeg.DetectData
}

// TranscodedSegmentData contains encoded data for a profile
type TranscodedSegmentData struct {
	Data   []byte
	PHash  []byte // Perceptual hash data (maybe nil)
	Pixels int64  // Encoded pixels
}

type SegChanData struct {
	ctx context.Context
	seg *stream.HLSSegment
	md  *SegTranscodingMetadata
	res chan *TranscodeResult
}

type RemoteTranscoderResult struct {
	TranscodeData *TranscodeData
	Err           error
}

type SegmentChan chan *SegChanData
type TranscoderChan chan *RemoteTranscoderResult

type transcodeConfig struct {
	OS      drivers.OSSession
	LocalOS drivers.OSSession
}

// func (rtm *RemoteTranscoderManager) getTaskChan(taskID int64) (TranscoderChan, error) {
// 	rtm.taskMutex.RLock()
// 	defer rtm.taskMutex.RUnlock()
// 	if tc, ok := rtm.taskChans[taskID]; ok {
// 		return tc, nil
// 	}
// 	return nil, fmt.Errorf("No transcoder channel")
// }

// func (rtm *RemoteTranscoderManager) addTaskChan() (int64, TranscoderChan) {
// 	rtm.taskMutex.Lock()
// 	defer rtm.taskMutex.Unlock()
// 	taskID := rtm.taskCount
// 	rtm.taskCount++
// 	if tc, ok := rtm.taskChans[taskID]; ok {
// 		// should really never happen
// 		glog.V(common.DEBUG).Info("Transcoder channel already exists for ", taskID)
// 		return taskID, tc
// 	}
// 	rtm.taskChans[taskID] = make(TranscoderChan, 1)
// 	return taskID, rtm.taskChans[taskID]
// }

// func (rtm *RemoteTranscoderManager) removeTaskChan(taskID int64) {
// 	rtm.taskMutex.Lock()
// 	defer rtm.taskMutex.Unlock()
// 	if _, ok := rtm.taskChans[taskID]; !ok {
// 		glog.V(common.DEBUG).Info("Transcoder channel nonexistent for job ", taskID)
// 		return
// 	}
// 	delete(rtm.taskChans, taskID)
// }

// func (n *LivepeerNode) endTranscodingSession(sessionId string, logCtx context.Context) {
// 	// timeout; clean up goroutine here
// 	var (
// 		exists  bool
// 		storage *transcodeConfig
// 		sess    *RemoteTranscoder
// 	)
// 	n.storageMutex.Lock()
// 	if storage, exists = n.StorageConfigs[sessionId]; exists {
// 		storage.OS.EndSession()
// 		storage.LocalOS.EndSession()
// 		delete(n.StorageConfigs, sessionId)
// 	}
// 	n.storageMutex.Unlock()
// 	// check to avoid nil pointer caused by garbage collection while this go routine is still running
// 	if n.TranscoderManager != nil {
// 		n.TranscoderManager.RTmutex.Lock()
// 		// send empty segment to signal transcoder internal session teardown if session exist
// 		if sess, exists = n.TranscoderManager.streamSessions[sessionId]; exists {
// 			segData := &net.SegData{
// 				AuthToken: &net.AuthToken{SessionId: sessionId},
// 			}
// 			msg := &net.NotifySegment{
// 				SegData: segData,
// 			}
// 			_ = sess.stream.Send(msg)
// 		}
// 		n.TranscoderManager.completeStreamSession(sessionId)
// 		n.TranscoderManager.RTmutex.Unlock()
// 	}
// 	n.segmentMutex.Lock()
// 	mid := ManifestID(sessionId)
// 	if _, exists = n.SegmentChans[mid]; exists {
// 		close(n.SegmentChans[mid])
// 		delete(n.SegmentChans, mid)
// 		if lpmon.Enabled {
// 			lpmon.CurrentSessions(len(n.SegmentChans))
// 		}
// 	}
// 	n.segmentMutex.Unlock()
// 	if exists {
// 		clog.V(common.DEBUG).Infof(logCtx, "Transcoding session ended by the Broadcaster for sessionID=%v", sessionId)
// 	}
// }

// func (n *LivepeerNode) serveTranscoder(stream net.Transcoder_RegisterTranscoderServer, capacity int, capabilities *net.Capabilities) {
// 	from := common.GetConnectionAddr(stream.Context())
// 	coreCaps := CapabilitiesFromNetCapabilities(capabilities)
// 	n.Capabilities.AddCapacity(coreCaps)
// 	defer n.Capabilities.RemoveCapacity(coreCaps)
// 	// Manage blocks while transcoder is connected
// 	n.TranscoderManager.Manage(stream, capacity, capabilities)
// 	glog.V(common.DEBUG).Infof("Closing transcoder=%s channel", from)
// }

// func (rtm *RemoteTranscoderManager) transcoderResults(tcID int64, res *RemoteTranscoderResult) {
// 	remoteChan, err := rtm.getTaskChan(tcID)
// 	if err != nil {
// 		return // do we need to return anything?
// 	}
// 	remoteChan <- res
// }

// type RemoteTranscoder struct {
// 	manager      *RemoteTranscoderManager
// 	stream       net.Transcoder_RegisterTranscoderServer
// 	capabilities *Capabilities
// 	eof          chan struct{}
// 	addr         string
// 	capacity     int
// 	load         int
// }

// RemoteTranscoderFatalError wraps error to indicate that error is fatal
type RemoteTranscoderFatalError struct {
	error
}

// NewRemoteTranscoderFatalError creates new RemoteTranscoderFatalError
// Exported here to be used in other packages
func NewRemoteTranscoderFatalError(err error) error {
	return RemoteTranscoderFatalError{err}
}

var ErrRemoteTranscoderTimeout = errors.New("Remote transcoder took too long")
var ErrNoTranscodersAvailable = errors.New("no transcoders available")
var ErrNoCompatibleTranscodersAvailable = errors.New("no transcoders can provide requested capabilities")

// func (rt *RemoteTranscoder) done() {
// 	// select so we don't block indefinitely if there's no listener
// 	select {
// 	case rt.eof <- struct{}{}:
// 	default:
// 	}
// }

// Transcode do actual transcoding by sending work to remote transcoder and waiting for the result
// func (rt *RemoteTranscoder) Transcode(logCtx context.Context, md *SegTranscodingMetadata) (*TranscodeData, error) {
// 	taskID, taskChan := rt.manager.addTaskChan()
// 	defer rt.manager.removeTaskChan(taskID)
// 	fname := md.Fname
// 	signalEOF := func(err error) (*TranscodeData, error) {
// 		rt.done()
// 		clog.Errorf(logCtx, "Fatal error with remote transcoder=%s taskId=%d fname=%s err=%q", rt.addr, taskID, fname, err)
// 		return nil, RemoteTranscoderFatalError{err}
// 	}

// 	// Copy and remove some fields to minimize unneeded transfer
// 	mdCopy := *md
// 	mdCopy.OS = nil // remote transcoders currently upload directly back to O
// 	mdCopy.Hash = ethcommon.Hash{}
// 	segData, err := NetSegData(&mdCopy)
// 	if err != nil {
// 		return nil, err
// 	}

// 	start := time.Now()
// 	msg := &net.NotifySegment{
// 		Url:     fname,
// 		TaskId:  taskID,
// 		SegData: segData,
// 		// Triggers failure on Os that don't know how to use SegData
// 		Profiles: []byte("invalid"),
// 	}
// 	err = rt.stream.Send(msg)

// 	if err != nil {
// 		return signalEOF(err)
// 	}

// 	// set a minimum timeout to accommodate transport / processing overhead
// 	dur := common.HTTPTimeout
// 	paddedDur := 4.0 * md.Duration // use a multiplier of 4 for now
// 	if paddedDur > dur {
// 		dur = paddedDur
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), dur)
// 	defer cancel()
// 	select {
// 	case <-ctx.Done():
// 		return signalEOF(ErrRemoteTranscoderTimeout)
// 	case chanData := <-taskChan:
// 		segmentLen := 0
// 		if chanData.TranscodeData != nil {
// 			segmentLen = len(chanData.TranscodeData.Segments)
// 		}
// 		clog.InfofErr(logCtx, "Successfully received results from remote transcoder=%s segments=%d taskId=%d fname=%s dur=%v",
// 			rt.addr, segmentLen, taskID, fname, time.Since(start), chanData.Err)
// 		return chanData.TranscodeData, chanData.Err
// 	}
// }
// func NewRemoteTranscoder(m *RemoteTranscoderManager, stream net.Transcoder_RegisterTranscoderServer, capacity int, caps *Capabilities) *RemoteTranscoder {
// 	return &RemoteTranscoder{
// 		manager:      m,
// 		stream:       stream,
// 		eof:          make(chan struct{}, 1),
// 		capacity:     capacity,
// 		addr:         common.GetConnectionAddr(stream.Context()),
// 		capabilities: caps,
// 	}
// }

// func NewRemoteTranscoderManager() *RemoteTranscoderManager {
// 	return &RemoteTranscoderManager{
// 		remoteTranscoders: []*RemoteTranscoder{},
// 		liveTranscoders:   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder{},
// 		RTmutex:           sync.Mutex{},

// 		taskMutex: &sync.RWMutex{},
// 		taskChans: make(map[int64]TranscoderChan),

// 		streamSessions: make(map[string]*RemoteTranscoder),
// 	}
// }

// type byLoadFactor []*RemoteTranscoder

// func loadFactor(r *RemoteTranscoder) float64 {
// 	return float64(r.load) / float64(r.capacity)
// }

// func (r byLoadFactor) Len() int      { return len(r) }
// func (r byLoadFactor) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
// func (r byLoadFactor) Less(i, j int) bool {
// 	return loadFactor(r[j]) < loadFactor(r[i]) // sort descending
// }

// type RemoteTranscoderManager struct {
// 	remoteTranscoders []*RemoteTranscoder
// 	liveTranscoders   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder
// 	RTmutex           sync.Mutex

// 	// For tracking tasks assigned to remote transcoders
// 	taskMutex *sync.RWMutex
// 	taskChans map[int64]TranscoderChan
// 	taskCount int64

// 	// Map for keeping track of sessions and their respective transcoders
// 	streamSessions map[string]*RemoteTranscoder
// }

// RegisteredTranscodersCount returns number of registered transcoders
// func (rtm *RemoteTranscoderManager) RegisteredTranscodersCount() int {
// 	rtm.RTmutex.Lock()
// 	defer rtm.RTmutex.Unlock()
// 	return len(rtm.liveTranscoders)
// }

// // RegisteredTranscodersInfo returns list of restered transcoder's information
// func (rtm *RemoteTranscoderManager) RegisteredTranscodersInfo() []common.RemoteTranscoderInfo {
// 	rtm.RTmutex.Lock()
// 	res := make([]common.RemoteTranscoderInfo, 0, len(rtm.liveTranscoders))
// 	for _, transcoder := range rtm.liveTranscoders {
// 		res = append(res, common.RemoteTranscoderInfo{Address: transcoder.addr, Capacity: transcoder.capacity})
// 	}
// 	rtm.RTmutex.Unlock()
// 	return res
// }

// // Manage adds transcoder to list of live transcoders. Doesn't return until transcoder disconnects
// func (rtm *RemoteTranscoderManager) Manage(stream net.Transcoder_RegisterTranscoderServer, capacity int, capabilities *net.Capabilities) {
// 	from := common.GetConnectionAddr(stream.Context())
// 	transcoder := NewRemoteTranscoder(rtm, stream, capacity, CapabilitiesFromNetCapabilities(capabilities))
// 	go func() {
// 		ctx := stream.Context()
// 		<-ctx.Done()
// 		err := ctx.Err()
// 		glog.Errorf("Stream closed for transcoder=%s, err=%q", from, err)
// 		transcoder.done()
// 	}()

// 	rtm.RTmutex.Lock()
// 	rtm.liveTranscoders[transcoder.stream] = transcoder
// 	rtm.remoteTranscoders = append(rtm.remoteTranscoders, transcoder)
// 	sort.Sort(byLoadFactor(rtm.remoteTranscoders))
// 	var totalLoad, totalCapacity, liveTranscodersNum int
// 	if monitor.Enabled {
// 		totalLoad, totalCapacity, liveTranscodersNum = rtm.totalLoadAndCapacity()
// 	}
// 	rtm.RTmutex.Unlock()
// 	if monitor.Enabled {
// 		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
// 	}

// 	<-transcoder.eof
// 	glog.Infof("Got transcoder=%s eof, removing from live transcoders map", from)

// 	rtm.RTmutex.Lock()
// 	delete(rtm.liveTranscoders, transcoder.stream)
// 	if monitor.Enabled {
// 		totalLoad, totalCapacity, liveTranscodersNum = rtm.totalLoadAndCapacity()
// 	}
// 	rtm.RTmutex.Unlock()
// 	if monitor.Enabled {
// 		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
// 	}
// }

// func removeFromRemoteTranscoders(rt *RemoteTranscoder, remoteTranscoders []*RemoteTranscoder) []*RemoteTranscoder {
// 	if len(remoteTranscoders) == 0 {
// 		// No transcoders to remove, return
// 		return remoteTranscoders
// 	}

// 	newRemoteTs := make([]*RemoteTranscoder, 0)
// 	for _, t := range remoteTranscoders {
// 		if t != rt {
// 			newRemoteTs = append(newRemoteTs, t)
// 		}
// 	}
// 	return newRemoteTs
// }

// func (rtm *RemoteTranscoderManager) selectTranscoder(sessionId string, caps *Capabilities) (*RemoteTranscoder, error) {
// 	rtm.RTmutex.Lock()
// 	defer rtm.RTmutex.Unlock()

// 	checkTranscoders := func(rtm *RemoteTranscoderManager) bool {
// 		return len(rtm.remoteTranscoders) > 0
// 	}

// 	findCompatibleTranscoder := func(rtm *RemoteTranscoderManager) int {
// 		for i := len(rtm.remoteTranscoders) - 1; i >= 0; i-- {
// 			// no capabilities = default capabilities, all transcoders must support them
// 			if caps == nil || caps.bitstring.CompatibleWith(rtm.remoteTranscoders[i].capabilities.bitstring) {
// 				return i
// 			}
// 		}
// 		return -1
// 	}

// 	for checkTranscoders(rtm) {
// 		currentTranscoder, sessionExists := rtm.streamSessions[sessionId]
// 		lastCompatibleTranscoder := findCompatibleTranscoder(rtm)
// 		if lastCompatibleTranscoder == -1 {
// 			return nil, ErrNoCompatibleTranscodersAvailable
// 		}
// 		if !sessionExists {
// 			currentTranscoder = rtm.remoteTranscoders[lastCompatibleTranscoder]
// 		}

// 		if _, ok := rtm.liveTranscoders[currentTranscoder.stream]; !ok {
// 			// Remove the stream session because the transcoder is no longer live
// 			if sessionExists {
// 				rtm.completeStreamSession(sessionId)
// 			}
// 			// transcoder does not exist in table; remove and retry
// 			rtm.remoteTranscoders = removeFromRemoteTranscoders(currentTranscoder, rtm.remoteTranscoders)
// 			continue
// 		}
// 		if !sessionExists {
// 			if currentTranscoder.load == currentTranscoder.capacity {
// 				// Head of queue is at capacity, so the rest must be too. Exit early
// 				return nil, ErrNoTranscodersAvailable
// 			}

// 			// Assinging transcoder to session for future use
// 			rtm.streamSessions[sessionId] = currentTranscoder
// 			currentTranscoder.load++
// 			sort.Sort(byLoadFactor(rtm.remoteTranscoders))
// 		}
// 		return currentTranscoder, nil
// 	}

// 	return nil, ErrNoTranscodersAvailable
// }

// ends transcoding session and releases resources
// func (node *LivepeerNode) EndTranscodingSession(sessionId string) {
// 	node.endTranscodingSession(sessionId, context.TODO())
// }

// func (node *RemoteTranscoderManager) EndTranscodingSession(sessionId string) {
// 	panic("shouldn't be called on RemoteTranscoderManager")
// }

// // completeStreamSessions end a stream session for a remote transcoder and decrements its load
// // caller should hold the mutex lock
// func (rtm *RemoteTranscoderManager) completeStreamSession(sessionId string) {
// 	t, ok := rtm.streamSessions[sessionId]
// 	if !ok {
// 		return
// 	}
// 	t.load--
// 	sort.Sort(byLoadFactor(rtm.remoteTranscoders))
// 	delete(rtm.streamSessions, sessionId)
// }

// // Caller of this function should hold RTmutex lock
// func (rtm *RemoteTranscoderManager) totalLoadAndCapacity() (int, int, int) {
// 	var load, capacity int
// 	for _, t := range rtm.liveTranscoders {
// 		load += t.load
// 		capacity += t.capacity
// 	}
// 	return load, capacity, len(rtm.liveTranscoders)
// }

// // Transcode does actual transcoding using remote transcoder from the pool
// func (rtm *RemoteTranscoderManager) Transcode(ctx context.Context, md *SegTranscodingMetadata) (*TranscodeData, error) {
// 	currentTranscoder, err := rtm.selectTranscoder(md.AuthToken.SessionId, md.Caps)
// 	if err != nil {
// 		return nil, err
// 	}
// 	res, err := currentTranscoder.Transcode(ctx, md)
// 	if err != nil {
// 		rtm.RTmutex.Lock()
// 		rtm.completeStreamSession(md.AuthToken.SessionId)
// 		rtm.RTmutex.Unlock()
// 	}
// 	_, fatal := err.(RemoteTranscoderFatalError)
// 	if fatal {
// 		// Don't retry if we've timed out; broadcaster likely to have moved on
// 		// XXX problematic for VOD when we *should* retry
// 		if err.(RemoteTranscoderFatalError).error == ErrRemoteTranscoderTimeout {
// 			return res, err
// 		}
// 		return rtm.Transcode(ctx, md)
// 	}
// 	return res, err
// }
