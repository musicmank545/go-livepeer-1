package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	gonet "net"
	"net/http"
	"time"

	"github.com/livepeer/go-livepeer/clog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"

	ethcommon "github.com/ethereum/go-ethereum/common"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const paymentHeader = "Livepeer-Payment"
const segmentHeader = "Livepeer-Segment"

const pixelEstimateMultiplier = 1.02

var errSegEncoding = errors.New("ErrorSegEncoding")
var errSegSig = errors.New("ErrSegSig")
var errFormat = errors.New("unrecognized profile output format")
var errProfile = errors.New("unrecognized encoder profile")
var errEncoder = errors.New("unrecognized video codec")
var errDuration = errors.New("invalid duration")
var errCapCompat = errors.New("incompatible capabilities")

var tlsConfig = &tls.Config{InsecureSkipVerify: true}
var httpClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
		DialTLSContext: func(ctx context.Context, network, addr string) (gonet.Conn, error) {
			cctx, cancel := context.WithTimeout(ctx, common.HTTPDialTimeout)
			defer cancel()

			tlsDialer := &tls.Dialer{Config: tlsConfig}
			return tlsDialer.DialContext(cctx, network, addr)
		},
		// Required for the transport to try to upgrade to HTTP/2 if TLSClientConfig is non-nil or
		// if custom dialers (i.e. via DialTLSContext) are used. This allows us to by default
		// transparently support HTTP/2 while maintaining the flexibility to use HTTP/1 by running
		// with GODEBUG=http2client=0
		ForceAttemptHTTP2: true,
	},
	// Don't set a timeout here; pass a context to the request
}

func getPayment(header string) (net.Payment, error) {
	buf, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return net.Payment{}, errors.Wrap(err, "base64 decode error")
	}
	var payment net.Payment
	if err := proto.Unmarshal(buf, &payment); err != nil {
		return net.Payment{}, errors.Wrap(err, "protobuf unmarshal error")
	}

	return payment, nil
}

func getPaymentSender(payment net.Payment) ethcommon.Address {
	if payment.Sender == nil {
		return ethcommon.Address{}
	}
	return ethcommon.BytesToAddress(payment.Sender)
}

func makeFfmpegVideoProfiles(protoProfiles []*net.VideoProfile) ([]ffmpeg.VideoProfile, error) {
	profiles := make([]ffmpeg.VideoProfile, 0, len(protoProfiles))
	for _, profile := range protoProfiles {
		name := profile.Name
		if name == "" {
			name = "net_" + ffmpeg.DefaultProfileName(int(profile.Width), int(profile.Height), int(profile.Bitrate))
		}
		format := ffmpeg.FormatMPEGTS
		switch profile.Format {
		case net.VideoProfile_MPEGTS:
		case net.VideoProfile_MP4:
			format = ffmpeg.FormatMP4
		default:
			return nil, errFormat
		}
		encoderProf := ffmpeg.ProfileNone
		switch profile.Profile {
		case net.VideoProfile_ENCODER_DEFAULT:
		case net.VideoProfile_H264_BASELINE:
			encoderProf = ffmpeg.ProfileH264Baseline
		case net.VideoProfile_H264_MAIN:
			encoderProf = ffmpeg.ProfileH264Main
		case net.VideoProfile_H264_HIGH:
			encoderProf = ffmpeg.ProfileH264High
		case net.VideoProfile_H264_CONSTRAINED_HIGH:
			encoderProf = ffmpeg.ProfileH264ConstrainedHigh
		default:
			return nil, errProfile
		}
		encoder := ffmpeg.H264
		switch profile.Encoder {
		case net.VideoProfile_H264:
			encoder = ffmpeg.H264
		case net.VideoProfile_H265:
			encoder = ffmpeg.H265
		case net.VideoProfile_VP8:
			encoder = ffmpeg.VP8
		case net.VideoProfile_VP9:
			encoder = ffmpeg.VP9
		default:
			return nil, errEncoder
		}
		var gop time.Duration
		if profile.Gop < 0 {
			gop = time.Duration(profile.Gop)
		} else {
			gop = time.Duration(profile.Gop) * time.Millisecond
		}
		prof := ffmpeg.VideoProfile{
			Name:         name,
			Bitrate:      fmt.Sprint(profile.Bitrate),
			Framerate:    uint(profile.Fps),
			FramerateDen: uint(profile.FpsDen),
			Resolution:   fmt.Sprintf("%dx%d", profile.Width, profile.Height),
			Format:       format,
			Profile:      encoderProf,
			GOP:          gop,
			Encoder:      encoder,
		}
		profiles = append(profiles, prof)
	}
	return profiles, nil
}

func makeNetDetectData(ffmpegDetectData []ffmpeg.DetectData) []*net.DetectData {
	netDataList := []*net.DetectData{}
	for _, data := range ffmpegDetectData {
		var netData *net.DetectData
		switch data.Type() {
		case ffmpeg.SceneClassification:
			d := data.(ffmpeg.SceneClassificationData)
			netClasses := make(map[uint32]float64)
			for classID, prob := range d {
				netClasses[uint32(classID)] = prob
			}
			netData = &net.DetectData{Value: &net.DetectData_SceneClassification{
				SceneClassification: &net.SceneClassificationData{
					ClassProbs: netClasses,
				},
			}}
		}
		netDataList = append(netDataList, netData)
	}
	return netDataList
}

func verifySegCreds(ctx context.Context, orch Orchestrator, segCreds string, broadcaster ethcommon.Address) (*core.SegTranscodingMetadata, context.Context, error) {
	buf, err := base64.StdEncoding.DecodeString(segCreds)
	if err != nil {
		glog.Error("Unable to base64-decode ", err)
		return nil, ctx, errSegEncoding
	}
	var segData net.SegData
	err = proto.Unmarshal(buf, &segData)
	if err != nil {
		glog.Error("Unable to unmarshal ", err)
		return nil, ctx, err
	}

	md, err := coreSegMetadata(&segData)
	if err != nil {
		return nil, ctx, err
	}
	ctx = clog.AddManifestID(ctx, string(md.ManifestID))

	if !orch.VerifySig(broadcaster, string(md.Flatten()), segData.Sig) {
		clog.Errorf(ctx, "Sig check failed")
		return nil, ctx, errSegSig
	}

	if !md.Caps.CompatibleWith(orch.Capabilities()) {
		clog.Errorf(ctx, "Capability check failed")
		return nil, ctx, errCapCompat
	}

	// Check that auth token is valid and not expired
	if segData.AuthToken == nil {
		return nil, ctx, errors.New("missing auth token")
	}

	verifyToken := orch.AuthToken(segData.AuthToken.SessionId, segData.AuthToken.Expiration)
	if !bytes.Equal(verifyToken.Token, segData.AuthToken.Token) {
		return nil, ctx, errors.New("invalid auth token")
	}
	ctx = clog.AddOrchSessionID(ctx, segData.AuthToken.SessionId)

	expiration := time.Unix(segData.AuthToken.Expiration, 0)
	if time.Now().After(expiration) {
		return nil, ctx, errors.New("expired auth token")
	}

	if err := orch.CheckCapacity(core.ManifestID(segData.AuthToken.SessionId)); err != nil {
		clog.Errorf(ctx, "Cannot process manifest err=%q", err)
		return nil, ctx, err
	}

	return md, ctx, nil
}

func estimateFee(seg *stream.HLSSegment, profiles []ffmpeg.VideoProfile, priceInfo *big.Rat) (*big.Rat, error) {
	if priceInfo == nil {
		return nil, nil
	}

	// TODO: Estimate the number of input pixels
	// Estimate the number of output pixels
	var outPixels int64
	for _, p := range profiles {
		w, h, err := ffmpeg.VideoProfileResolution(p)
		if err != nil {
			return nil, err
		}
		framerate := p.Framerate
		if framerate == 0 {
			// FPS is being passed through (no fps adjustment)
			// TODO incorporate the actual number of frames from the input
			framerate = 120 // conservative estimate of input fps
		}
		framerateDen := p.FramerateDen
		if framerateDen == 0 {
			// Denominator not set, treat as 1
			framerateDen = 1
		}
		// Take ceilings, as it is better to overestimate
		fps := math.Ceil((float64(framerate) / float64(framerateDen)))
		outPixels += int64(w*h) * int64(fps) * int64(math.Ceil(seg.Duration))
	}

	// feeEstimate = pixels * pixelEstimateMultiplier * priceInfo
	fee := new(big.Rat).SetInt64(outPixels)
	// Multiply pixels by pixelEstimateMultiplier to ensure that we never underpay
	fee.Mul(fee, new(big.Rat).SetFloat64(pixelEstimateMultiplier))
	fee.Mul(fee, priceInfo)

	return fee, nil
}

func sendReqWithTimeout(req *http.Request, timeout time.Duration) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())
	timeouter := time.AfterFunc(timeout, cancel)

	req = req.WithContext(ctx)
	resp, err := httpClient.Do(req)
	if timeouter.Stop() {
		return resp, err
	}
	// timeout has already fired and cancelled the request
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return nil, context.DeadlineExceeded
}
