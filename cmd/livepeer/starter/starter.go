package starter

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	lpmon "github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/server"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/lpms/ffmpeg"
)

var (
	// The timeout for ETH RPC calls
	ethRPCTimeout = 20 * time.Second
	// The maximum blocks for the block watcher to retain
	blockWatcherRetentionLimit = 20

	// Estimate of the gas required to redeem a PM ticket on L1 Ethereum
	redeemGasL1 = 350000
	// Estimate of the gas required to redeem a PM ticket on L2 Arbitrum
	redeemGasL2 = 1200000
	// The multiplier on the transaction cost to use for PM ticket faceValue
	txCostMultiplier = 100

	// The interval at which to clean up cached max float values for PM senders and balances per stream
	cleanupInterval = 1 * time.Minute
	// The time to live for cached max float values for PM senders (else they will be cleaned up) in seconds
	smTTL = 60 // 1 minute
)

const RtmpPort = "1935"
const RpcPort = "8935"
const CliPort = "7935"

type LivepeerConfig struct {
	Network                      *string
	RtmpAddr                     *string
	CliAddr                      *string
	HttpAddr                     *string
	ServiceAddr                  *string
	OrchAddr                     *string
	VerifierURL                  *string
	EthController                *string
	VerifierPath                 *string
	LocalVerify                  *bool
	HttpIngest                   *bool
	Orchestrator                 *bool
	Transcoder                   *bool
	Broadcaster                  *bool
	OrchSecret                   *string
	TranscodingOptions           *string
	MaxAttempts                  *int
	SelectRandFreq               *float64
	MaxSessions                  *int
	CurrentManifest              *bool
	Nvidia                       *string
	Netint                       *string
	TestTranscoder               *bool
	SceneClassificationModelPath *string
	DetectContent                *bool
	DetectionSampleRate          *uint
	EthAcctAddr                  *string
	EthPassword                  *string
	EthKeystorePath              *string
	EthOrchAddr                  *string
	EthUrl                       *string
	TxTimeout                    *time.Duration
	MaxTxReplacements            *int
	GasLimit                     *int
	MinGasPrice                  *int64
	MaxGasPrice                  *int
	InitializeRound              *bool
	TicketEV                     *string
	MaxFaceValue                 *string
	MaxTicketEV                  *string
	DepositMultiplier            *int
	PricePerUnit                 *int
	MaxPricePerUnit              *int
	PixelsPerUnit                *int
	AutoAdjustPrice              *bool
	PricePerBroadcaster          *string
	BlockPollingInterval         *int
	Redeemer                     *bool
	RedeemerAddr                 *string
	Reward                       *bool
	Monitor                      *bool
	MetricsPerStream             *bool
	MetricsExposeClientIP        *bool
	MetadataQueueUri             *string
	MetadataAmqpExchange         *string
	MetadataPublishTimeout       *time.Duration
	Datadir                      *string
	Objectstore                  *string
	Recordstore                  *string
	FVfailGsBucket               *string
	FVfailGsKey                  *string
	AuthWebhookURL               *string
	OrchWebhookURL               *string
	DetectionWebhookURL          *string
}

// DefaultLivepeerConfig creates LivepeerConfig exactly the same as when no flags are passed to the livepeer process.
func DefaultLivepeerConfig() LivepeerConfig {
	// Network & Addresses:
	defaultNetwork := "offchain"
	defaultRtmpAddr := "127.0.0.1:" + RtmpPort
	defaultCliAddr := "127.0.0.1:" + CliPort
	defaultHttpAddr := ""
	defaultServiceAddr := ""
	defaultOrchAddr := ""
	defaultVerifierURL := ""
	defaultVerifierPath := ""

	// Transcoding:
	defaultOrchestrator := false
	defaultTranscoder := false
	defaultBroadcaster := false
	defaultOrchSecret := ""
	defaultTranscodingOptions := "P240p30fps16x9,P360p30fps16x9"
	defaultMaxAttempts := 3
	defaultSelectRandFreq := 0.3
	defaultMaxSessions := 10
	defaultCurrentManifest := false
	defaultNvidia := ""
	defaultNetint := ""
	defaultTestTranscoder := true
	defaultDetectContent := false
	defaultDetectionSampleRate := uint(math.MaxUint32)
	defaultSceneClassificationModelPath := "tasmodel.pb"

	// Onchain:
	defaultEthAcctAddr := ""
	defaultEthPassword := ""
	defaultEthKeystorePath := ""
	defaultEthOrchAddr := ""
	defaultEthUrl := ""
	defaultTxTimeout := 5 * time.Minute
	defaultMaxTxReplacements := 1
	defaultGasLimit := 0
	defaultMaxGasPrice := 0
	defaultEthController := ""
	defaultInitializeRound := false
	defaultTicketEV := "1000000000000"
	defaultMaxFaceValue := "0"
	defaultMaxTicketEV := "3000000000000"
	defaultDepositMultiplier := 1
	defaultMaxPricePerUnit := 0
	defaultPixelsPerUnit := 1
	defaultAutoAdjustPrice := true
	defaultpricePerBroadcaster := ""
	defaultBlockPollingInterval := 5
	defaultRedeemer := false
	defaultRedeemerAddr := ""
	defaultMonitor := false
	defaultMetricsPerStream := false
	defaultMetricsExposeClientIP := false
	defaultMetadataQueueUri := ""
	defaultMetadataAmqpExchange := "lp_golivepeer_metadata"
	defaultMetadataPublishTimeout := 1 * time.Second

	// Ingest:
	defaultHttpIngest := true

	// Verification:
	defaultLocalVerify := true

	// Storage:
	defaultDatadir := ""
	defaultObjectstore := ""
	defaultRecordstore := ""

	// Fast Verification GS bucket:
	defaultFVfailGsBucket := ""
	defaultFVfailGsKey := ""

	// API
	defaultAuthWebhookURL := ""
	defaultOrchWebhookURL := ""
	defaultDetectionWebhookURL := ""

	return LivepeerConfig{
		// Network & Addresses:
		Network:      &defaultNetwork,
		RtmpAddr:     &defaultRtmpAddr,
		CliAddr:      &defaultCliAddr,
		HttpAddr:     &defaultHttpAddr,
		ServiceAddr:  &defaultServiceAddr,
		OrchAddr:     &defaultOrchAddr,
		VerifierURL:  &defaultVerifierURL,
		VerifierPath: &defaultVerifierPath,

		// Transcoding:
		Orchestrator:                 &defaultOrchestrator,
		Transcoder:                   &defaultTranscoder,
		Broadcaster:                  &defaultBroadcaster,
		OrchSecret:                   &defaultOrchSecret,
		TranscodingOptions:           &defaultTranscodingOptions,
		MaxAttempts:                  &defaultMaxAttempts,
		SelectRandFreq:               &defaultSelectRandFreq,
		MaxSessions:                  &defaultMaxSessions,
		CurrentManifest:              &defaultCurrentManifest,
		Nvidia:                       &defaultNvidia,
		Netint:                       &defaultNetint,
		TestTranscoder:               &defaultTestTranscoder,
		SceneClassificationModelPath: &defaultSceneClassificationModelPath,
		DetectContent:                &defaultDetectContent,
		DetectionSampleRate:          &defaultDetectionSampleRate,

		// Onchain:
		EthAcctAddr:            &defaultEthAcctAddr,
		EthPassword:            &defaultEthPassword,
		EthKeystorePath:        &defaultEthKeystorePath,
		EthOrchAddr:            &defaultEthOrchAddr,
		EthUrl:                 &defaultEthUrl,
		TxTimeout:              &defaultTxTimeout,
		MaxTxReplacements:      &defaultMaxTxReplacements,
		GasLimit:               &defaultGasLimit,
		MaxGasPrice:            &defaultMaxGasPrice,
		EthController:          &defaultEthController,
		InitializeRound:        &defaultInitializeRound,
		TicketEV:               &defaultTicketEV,
		MaxFaceValue:           &defaultMaxFaceValue,
		MaxTicketEV:            &defaultMaxTicketEV,
		DepositMultiplier:      &defaultDepositMultiplier,
		MaxPricePerUnit:        &defaultMaxPricePerUnit,
		PixelsPerUnit:          &defaultPixelsPerUnit,
		AutoAdjustPrice:        &defaultAutoAdjustPrice,
		PricePerBroadcaster:    &defaultpricePerBroadcaster,
		BlockPollingInterval:   &defaultBlockPollingInterval,
		Redeemer:               &defaultRedeemer,
		RedeemerAddr:           &defaultRedeemerAddr,
		Monitor:                &defaultMonitor,
		MetricsPerStream:       &defaultMetricsPerStream,
		MetricsExposeClientIP:  &defaultMetricsExposeClientIP,
		MetadataQueueUri:       &defaultMetadataQueueUri,
		MetadataAmqpExchange:   &defaultMetadataAmqpExchange,
		MetadataPublishTimeout: &defaultMetadataPublishTimeout,

		// Ingest:
		HttpIngest: &defaultHttpIngest,

		// Verification:
		LocalVerify: &defaultLocalVerify,

		// Storage:
		Datadir:     &defaultDatadir,
		Objectstore: &defaultObjectstore,
		Recordstore: &defaultRecordstore,

		// Fast Verification GS bucket:
		FVfailGsBucket: &defaultFVfailGsBucket,
		FVfailGsKey:    &defaultFVfailGsKey,

		// API
		AuthWebhookURL:      &defaultAuthWebhookURL,
		OrchWebhookURL:      &defaultOrchWebhookURL,
		DetectionWebhookURL: &defaultDetectionWebhookURL,
	}
}

func StartLivepeer(ctx context.Context, cfg LivepeerConfig) {
	if *cfg.MaxSessions <= 0 {
		glog.Fatal("-maxSessions must be greater than zero")
		return
	}

	if *cfg.Netint != "" && *cfg.Nvidia != "" {
		glog.Fatal("both -netint and -nvidia arguments specified, this is not supported")
		return
	}

	if *cfg.DetectionSampleRate <= 0 {
		glog.Fatal("-detectionSampleRate must be greater than zero")
		return
	}

	// If multiple orchAddr specified, ensure other necessary flags present and clean up list
	orchURLs := parseOrchAddrs(*cfg.OrchAddr)

	if *cfg.Datadir == "" {
		homedir := os.Getenv("HOME")
		if homedir == "" {
			usr, err := user.Current()
			if err != nil {
				glog.Fatalf("Cannot find current user: %v", err)
			}
			homedir = usr.HomeDir
		}
		*cfg.Datadir = filepath.Join(homedir, ".lpData", *cfg.Network)
	}

	//Make sure datadir is present
	if _, err := os.Stat(*cfg.Datadir); os.IsNotExist(err) {
		glog.Infof("Creating data dir: %v", *cfg.Datadir)
		if err = os.MkdirAll(*cfg.Datadir, 0755); err != nil {
			glog.Errorf("Error creating datadir: %v", err)
		}
	}

	//Set Gs bucket for fast verification fail case
	if *cfg.FVfailGsBucket != "" && *cfg.FVfailGsKey != "" {
		drivers.SetCreds(*cfg.FVfailGsBucket, *cfg.FVfailGsKey)
	}

	//Set up DB
	dbh, err := common.InitDB(*cfg.Datadir + "/lpdb.sqlite3")
	if err != nil {
		glog.Errorf("Error opening DB: %v", err)
		return
	}
	defer dbh.Close()

	n, err := core.NewLivepeerNode(nil, *cfg.Datadir, dbh)
	if err != nil {
		glog.Errorf("Error creating livepeer node: %v", err)
	}

	if *cfg.OrchSecret != "" {
		n.OrchSecret, _ = common.ReadFromFile(*cfg.OrchSecret)
	}

	var transcoderCaps []core.Capability
	if *cfg.Transcoder {
		core.WorkDir = *cfg.Datadir
		accel := ffmpeg.Software
		var devicesStr string
		if *cfg.Nvidia != "" {
			accel = ffmpeg.Nvidia
			devicesStr = *cfg.Nvidia
		}
		if *cfg.Netint != "" {
			accel = ffmpeg.Netint
			devicesStr = *cfg.Netint
		}
		if accel != ffmpeg.Software {
			accelName := ffmpeg.AccelerationNameLookup[accel]
			tf, dtf, err := core.GetTranscoderFactoryByAccel(accel)
			if err != nil {
				glog.Fatalf("Error unsupported acceleration: %v", err)
			}
			// Get a list of device ids
			devices, err := common.ParseAccelDevices(devicesStr, accel)
			glog.Infof("%v devices: %v", accelName, devices)
			if err != nil {
				glog.Fatalf("Error while parsing '-%v %v' flag: %v", strings.ToLower(accelName), devices, err)
			}
			glog.Infof("Transcoding on these %v devices: %v", accelName, devices)
			// Test transcoding with specified device
			if *cfg.TestTranscoder {
				transcoderCaps, err = core.TestTranscoderCapabilities(devices, tf)
				if err != nil {
					glog.Fatal(err)
					return
				}
			} else {
				// no capability test was run, assume default capabilities
				transcoderCaps = append(transcoderCaps, core.DefaultCapabilities()...)
			}
			n.Transcoder = core.NewLoadBalancingTranscoder(devices, tf, dtf)
		} else {
			// for local software mode, enable all capabilities
			transcoderCaps = append(core.DefaultCapabilities(), core.OptionalCapabilities()...)
			n.Transcoder = core.NewLocalTranscoder(*cfg.Datadir)
		}
	}

	n.NodeType = core.TranscoderNode

	lpmon.NodeID = *cfg.EthAcctAddr
	if lpmon.NodeID != "" {
		lpmon.NodeID += "-"
	}
	hn, _ := os.Hostname()
	lpmon.NodeID += hn

	if *cfg.Monitor {
		if *cfg.MetricsExposeClientIP {
			*cfg.MetricsPerStream = true
		}
		lpmon.Enabled = true
		lpmon.PerStreamMetrics = *cfg.MetricsPerStream
		lpmon.ExposeClientIP = *cfg.MetricsExposeClientIP
		nodeType := lpmon.Default

		nodeType = lpmon.Transcoder

		lpmon.InitCensus(nodeType, core.LivepeerVersion)
	}

	serviceErr := make(chan error)

	core.MaxSessions = *cfg.MaxSessions
	if lpmon.Enabled {
		lpmon.MaxSessions(core.MaxSessions)
	}

	n.Capabilities = core.NewCapabilities(transcoderCaps, core.MandatoryOCapabilities())
	*cfg.CliAddr = defaultAddr(*cfg.CliAddr, "127.0.0.1", CliPort)

	if drivers.NodeStorage == nil {
		// base URI will be empty for broadcasters; that's OK
		drivers.NodeStorage = drivers.NewMemoryDriver(n.GetServiceURI())
	}

	if *cfg.MetadataPublishTimeout > 0 {
		server.MetadataPublishTimeout = *cfg.MetadataPublishTimeout
	}
	if *cfg.MetadataQueueUri != "" {
		uri, err := url.ParseRequestURI(*cfg.MetadataQueueUri)
		if err != nil {
			glog.Fatalf("Error parsing -metadataQueueUri: err=%q", err)
		}
		switch uri.Scheme {
		case "amqp", "amqps":
			uriStr, exchange, keyNs := *cfg.MetadataQueueUri, *cfg.MetadataAmqpExchange, n.NodeType.String()
			server.MetadataQueue, err = event.NewAMQPExchangeProducer(context.Background(), uriStr, exchange, keyNs)
			if err != nil {
				glog.Fatalf("Error establishing AMQP connection: err=%q", err)
			}
		default:
			glog.Fatalf("Unsupported scheme in -metadataUri: %s", uri.Scheme)
		}
	}

	srv := &http.Server{Addr: *cfg.CliAddr}

	if n.OrchSecret == "" {
		glog.Fatal("Missing -orchSecret")
	}
	if len(orchURLs) <= 0 {
		glog.Fatal("Missing -orchAddr")
	}

	go server.RunTranscoder(n, orchURLs[0].Host, *cfg.MaxSessions, transcoderCaps)

	glog.Infof("**Liveepeer Running in Transcoder Mode***")

	glog.Infof("Livepeer Node version: %v", core.LivepeerVersion)

	select {

	case err := <-serviceErr:
		if err != nil {
			glog.Fatalf("Error starting service: %v", err)
		}
	case <-ctx.Done():
		srv.Shutdown(ctx)
		return
	}
}

func parseOrchAddrs(addrs string) []*url.URL {
	var res []*url.URL
	if len(addrs) > 0 {
		for _, addr := range strings.Split(addrs, ",") {
			addr = strings.TrimSpace(addr)
			addr = defaultAddr(addr, "127.0.0.1", RpcPort)
			if !strings.HasPrefix(addr, "http") {
				addr = "https://" + addr
			}
			uri, err := url.ParseRequestURI(addr)
			if err != nil {
				glog.Error("Could not parse orchestrator URI: ", err)
				continue
			}
			res = append(res, uri)
		}
	}
	return res
}

func defaultAddr(addr, defaultHost, defaultPort string) string {
	if addr == "" {
		return defaultHost + ":" + defaultPort
	}
	if addr[0] == ':' {
		return defaultHost + addr
	}
	// not IPv6 safe
	if !strings.Contains(addr, ":") {
		return addr + ":" + defaultPort
	}
	return addr
}
