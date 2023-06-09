package core

import (
	"context"
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

// RemoteTranscoderFatalError wraps error to indicate that error is fatal
type RemoteTranscoderFatalError struct {
	error
}

// NewRemoteTranscoderFatalError creates new RemoteTranscoderFatalError
// Exported here to be used in other packages
func NewRemoteTranscoderFatalError(err error) error {
	return RemoteTranscoderFatalError{err}
}

// var ErrRemoteTranscoderTimeout = errors.New("Remote transcoder took too long")
// var ErrNoTranscodersAvailable = errors.New("no transcoders available")
// var ErrNoCompatibleTranscodersAvailable = errors.New("no transcoders can provide requested capabilities")
