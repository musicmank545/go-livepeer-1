package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/net"
	ffmpeg "github.com/livepeer/lpms/ffmpeg"
	"github.com/patrickmn/go-cache"
	"google.golang.org/grpc"

	ethcommon "github.com/ethereum/go-ethereum/common"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

const GRPCConnectTimeout = 3 * time.Second
const GRPCTimeout = 8 * time.Second
const HTTPIdleTimeout = 10 * time.Minute

var authTokenValidPeriod = 30 * time.Minute
var discoveryAuthWebhookCacheCleanup = 5 * time.Minute

var discoveryAuthWebhookCache = cache.New(authTokenValidPeriod, discoveryAuthWebhookCacheCleanup)

type lphttp struct {
	//orchestrator Orchestrator
	orchRPC  *grpc.Server
	transRPC *http.ServeMux
	node     *core.LivepeerNode
}

// grpc methods
func (h *lphttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if r.ProtoMajor == 2 && strings.HasPrefix(ct, "application/grpc") {
		h.orchRPC.ServeHTTP(w, r)
	} else {
		h.transRPC.ServeHTTP(w, r)
	}
}

func coreSegMetadata(segData *net.SegData) (*core.SegTranscodingMetadata, error) {
	if segData == nil {
		glog.Error("Empty seg data")
		return nil, errors.New("empty seg data")
	}
	var err error
	profiles := []ffmpeg.VideoProfile{}
	if len(segData.FullProfiles3) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles3)
	} else if len(segData.FullProfiles2) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles2)
	} else if len(segData.FullProfiles) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles)
	} else if len(segData.Profiles) > 0 {
		profiles, err = common.BytesToVideoProfile(segData.Profiles)
	}
	if err != nil {
		glog.Error("Unable to deserialize profiles ", err)
		return nil, err
	}

	var os *net.OSInfo
	if len(segData.Storage) > 0 {
		os = segData.Storage[0]
	}

	dur := time.Duration(segData.Duration) * time.Millisecond
	if dur < 0 || dur > common.MaxDuration {
		glog.Error("Invalid duration")
		return nil, errDuration
	}
	if dur == 0 {
		dur = 2 * time.Second // assume 2sec default duration
	}

	caps := core.CapabilitiesFromNetCapabilities(segData.Capabilities)
	if caps == nil {
		// For older broadcasters. Note if there are any orchestrator
		// mandatory capabilities, seg creds verification will fail.
		caps = core.NewCapabilities(nil, nil)
	}

	detectorProfs := []ffmpeg.DetectorProfile{}
	for _, detector := range segData.DetectorProfiles {
		var detectorProfile ffmpeg.DetectorProfile
		// Refer to the following for type magic:
		// https://developers.google.com/protocol-buffers/docs/reference/go-generated#oneof
		switch x := detector.Value.(type) {
		case *net.DetectorProfile_SceneClassification:
			profile := x.SceneClassification
			classes := []ffmpeg.DetectorClass{}
			for _, class := range profile.Classes {
				classes = append(classes, ffmpeg.DetectorClass{
					ID:   int(class.ClassId),
					Name: class.ClassName,
				})
			}
			detectorProfile = &ffmpeg.SceneClassificationProfile{
				SampleRate: uint(profile.SampleRate),
				Classes:    classes,
			}
		}
		detectorProfs = append(detectorProfs, detectorProfile)
	}
	var segPar *core.SegmentParameters
	if segData.SegmentParameters != nil {
		segPar = &core.SegmentParameters{
			From: time.Duration(segData.SegmentParameters.From) * time.Millisecond,
			To:   time.Duration(segData.SegmentParameters.To) * time.Millisecond,
		}
	}

	return &core.SegTranscodingMetadata{
		ManifestID:         core.ManifestID(segData.ManifestId),
		Seq:                segData.Seq,
		Hash:               ethcommon.BytesToHash(segData.Hash),
		Profiles:           profiles,
		OS:                 os,
		Duration:           dur,
		Caps:               caps,
		AuthToken:          segData.AuthToken,
		DetectorEnabled:    segData.DetectorEnabled,
		DetectorProfiles:   detectorProfs,
		CalcPerceptualHash: segData.CalcPerceptualHash,
		SegmentParameters:  segPar,
	}, nil
}
