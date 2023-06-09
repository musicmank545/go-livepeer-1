package server

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-livepeer/eth/types"
	"github.com/livepeer/go-livepeer/pm"
	"github.com/livepeer/lpms/ffmpeg"
)

const MainnetChainId = 1
const RinkebyChainId = 4

func localStreamsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// XXX fetch local streams?
		ret := make([]map[string]string, 0)

		respondJson(w, ret)
	})
}

type ChainIdGetter interface {
	ChainID() (*big.Int, error)
}

func ethChainIdHandler(db ChainIdGetter) http.Handler {
	return mustHaveDb(db, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chainID, err := db.ChainID()
		if err != nil {
			respond500(w, fmt.Sprintf("Error getting eth network ID err=%q", err))
			return
		}
		respondOk(w, []byte(chainID.String()))
	}))
}

type BlockGetter interface {
	LastSeenBlock() (*big.Int, error)
}

func currentBlockHandler(db BlockGetter) http.Handler {
	return mustHaveDb(db, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blk, err := db.LastSeenBlock()
		if err != nil {
			respond500(w, fmt.Sprintf("could not query last seen block: %v", err))
			return
		}
		respondOk(w, blk.Bytes())
	}))
}

func getAvailableTranscodingOptionsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transcodingOptions := make([]string, 0, len(ffmpeg.VideoProfileLookup))
		for opt := range ffmpeg.VideoProfileLookup {
			transcodingOptions = append(transcodingOptions, opt)
		}

		respondJson(w, transcodingOptions)
	})
}

// Rounds
func currentRoundHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentRound, err := client.CurrentRound()
		if err != nil {
			respond500(w, fmt.Sprintf("could not query current round: %v", err))
			return
		}

		respondOk(w, currentRound.Bytes())
	}))
}

func initializeRoundHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tx, err := client.InitializeRound()
		if err != nil {
			glog.Error(err)
			respond500(w, fmt.Sprintf("could not initialize round: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			glog.Error(err)
			respond500(w, fmt.Sprintf("could not initialize round: %v", err))
			return
		}
		respondOk(w, nil)
	}))
}

// Protocol parameters
func protocolParametersHandler(client eth.LivepeerEthClient, db ChainIdGetter) http.Handler {
	return mustHaveDb(db, mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		numActiveOrchestrators, err := client.GetTranscoderPoolMaxSize()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		roundLength, err := client.RoundLength()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		roundLockAmount, err := client.RoundLockAmount()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		unbondingPeriod, err := client.UnbondingPeriod()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		inflation, err := client.Inflation()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		inflationChange, err := client.InflationChange()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		targetBondingRate, err := client.TargetBondingRate()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		totalBonded, err := client.GetTotalBonded()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		isL1Network, err := isL1Network(db)
		if err != nil {
			glog.Error(err)
			return
		}
		var totalSupply *big.Int
		if isL1Network {
			totalSupply, err = client.TotalSupply()
		} else {
			totalSupply, err = client.GetGlobalTotalSupply()
		}
		if err != nil {
			respond500(w, err.Error())
			return
		}

		paused, err := client.Paused()
		if err != nil {
			respond500(w, err.Error())
			return
		}

		params := &types.ProtocolParameters{
			NumActiveTranscoders: numActiveOrchestrators,
			RoundLength:          roundLength,
			RoundLockAmount:      roundLockAmount,
			UnbondingPeriod:      unbondingPeriod,
			Inflation:            inflation,
			InflationChange:      inflationChange,
			TargetBondingRate:    targetBondingRate,
			TotalBonded:          totalBonded,
			TotalSupply:          totalSupply,
			Paused:               paused,
		}

		respondJson(w, params)
	})))
}

// Eth
func contractAddressesHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addrMap := client.ContractAddresses()
		respondJson(w, addrMap)
	}))
}

func ethAddrHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondOk(w, []byte(client.Account().Address.Hex()))
	}))
}

func tokenBalanceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := client.BalanceOf(client.Account().Address)
		if err != nil {
			respond500(w, err.Error())
			return
		}
		respondOk(w, []byte(b.String()))
	}))
}

func ethBalanceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := client.Backend()
		balance, err := b.BalanceAt(r.Context(), client.Account().Address, nil)
		if err != nil {
			respond500(w, err.Error())
			return
		}
		respondOk(w, []byte(balance.String()))
	}))
}

func transferTokensHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		to := r.FormValue("to")
		amountStr := r.FormValue("amount")
		amount, err := common.ParseBigInt(amountStr)
		if err != nil {
			respond400(w, err.Error())
			return
		}

		tx, err := client.Transfer(ethcommon.HexToAddress(to), amount)
		if err != nil {
			respond500(w, err.Error())
			return
		}
		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, err.Error())
			return
		}

		glog.Infof("Transferred %v to %v", eth.FormatUnits(amount, "LPT"), to)
		respondOk(w, nil)
	}))
}

func requestTokensHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextValidRequest, err := client.NextValidRequest(client.Account().Address)
		if err != nil {
			respond500(w, fmt.Sprintf("Unable to get the time for the next valid request from faucet: %v", err))
			return
		}

		backend := client.Backend()
		h, err := backend.HeaderByNumber(r.Context(), nil)
		if err != nil {
			respond500(w, fmt.Sprintf("Unable to get latest block: %v", err))
			return
		}

		now := int64(h.Time)
		if nextValidRequest.Int64() != 0 && nextValidRequest.Int64() > now {
			respond500(w, fmt.Sprintf("Error requesting tokens from faucet: can only request tokens once every hour, please wait %v more minutes", (nextValidRequest.Int64()-now)/60))
			return
		}

		glog.Infof("Requesting tokens from faucet")

		tx, err := client.Request()
		if err != nil {
			respond500(w, fmt.Sprintf("Error requesting tokens from faucet: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("Error requesting tokens from faucet: %v", err))
			return
		}
		respondOk(w, nil)
	}))
}

func signMessageHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use EIP-191 (https://github.com/ethereum/EIPs/blob/master/EIPS/eip-191.md) signature versioning
		// The SigFormat terminology is taken from:
		// https://github.com/ethereum/go-ethereum/blob/dddf73abbddb297e61cee6a7e6aebfee87125e49/signer/core/apitypes/types.go#L171
		sigFormat := r.Header.Get("SigFormat")
		if sigFormat == "" {
			sigFormat = "text/plain"
		}

		message := r.FormValue("message")

		var signed []byte
		var err error

		// Only support text/plain and data/typed
		// By default use text/plain
		switch sigFormat {
		case "data/typed":
			var d apitypes.TypedData
			err = json.Unmarshal([]byte(message), &d)
			if err != nil {
				respond400(w, fmt.Sprintf("could not unmarshal typed data - err=%q", err))
				return
			}

			signed, err = client.SignTypedData(d)
			if err != nil {
				respond500(w, fmt.Sprintf("could not sign typed data - err=%q", err))
				return
			}
		default:
			// text/plain
			signed, err = client.Sign([]byte(message))
			if err != nil {
				respond500(w, fmt.Sprintf("could not sign message - err=%q", err))
				return
			}
		}

		respondOk(w, signed)
	}))
}

func voteHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		poll := r.FormValue("poll")
		if !ethcommon.IsHexAddress(poll) {
			respond500(w, "invalid poll contract address")
			return
		}

		choiceStr := r.FormValue("choiceID")
		choiceID, ok := new(big.Int).SetString(choiceStr, 10)
		if !ok {
			respond500(w, "choiceID is not a valid integer value")
			return
		}
		if !types.VoteChoice(int(choiceID.Int64())).IsValid() {
			respond500(w, "invalid choiceID")
			return
		}

		// submit tx
		tx, err := client.Vote(
			ethcommon.HexToAddress(poll),
			choiceID,
		)
		if err != nil {
			respond500(w, fmt.Sprintf("unable to submit vote transaction err=%q", err))
			return
		}

		if err := client.CheckTx(tx); err != nil {
			respond500(w, fmt.Sprintf("unable to mine vote transaction err=%q", err))
			return
		}

		respondOk(w, tx.Hash().Bytes())
	}))
}

// Gas Price
func setMaxGasPriceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		amount := r.FormValue("amount")
		gprice, err := common.ParseBigInt(amount)
		if err != nil {
			respond400(w, fmt.Sprintf("Parsing failed for price: %v", err))
			return
		}
		if amount == "0" {
			gprice = nil
		}
		client.Backend().GasPriceMonitor().SetMaxGasPrice(gprice)
		if err := client.SetMaxGasPrice(gprice); err != nil {
			glog.Errorf("Error setting max gas price: %v", err)
			respond500(w, err.Error())
		}
		respondOk(w, nil)
	}))
}

func setMinGasPriceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		minGasPrice, err := common.ParseBigInt(r.FormValue("minGasPrice"))
		if err != nil {
			respond400(w, fmt.Sprintf("invalid minGasPrice: %v", err))
			return
		}
		client.Backend().GasPriceMonitor().SetMinGasPrice(minGasPrice)

		respondOk(w, nil)
	}))
}

func maxGasPriceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := client.Backend()
		gprice := b.GasPriceMonitor().MaxGasPrice()
		if gprice == nil {
			respondOk(w, nil)
		} else {
			respondOk(w, []byte(gprice.String()))
		}
	}))
}

func minGasPriceHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondOk(w, []byte(client.Backend().GasPriceMonitor().MinGasPrice().String()))
	}))
}

// Tickets
func fundDepositAndReserveHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		depositAmount, err := common.ParseBigInt(r.FormValue("depositAmount"))
		if err != nil {
			respond400(w, fmt.Sprintf("invalid depositAmount: %v", err))
			return
		}

		reserveAmount, err := common.ParseBigInt(r.FormValue("reserveAmount"))
		if err != nil {
			respond400(w, fmt.Sprintf("invalid reserveAmount: %v", err))
			return
		}

		tx, err := client.FundDepositAndReserve(depositAmount, reserveAmount)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute fundDepositAndReserve: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute fundDepositAndReserve: %v", err))
			return
		}

		respondOk(w, []byte("fundDepositAndReserve success"))
	}))
}

func fundDepositHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		amount, err := common.ParseBigInt(r.FormValue("amount"))
		if err != nil {
			respond400(w, fmt.Sprintf("invalid amount: %v", err))
			return
		}

		tx, err := client.FundDeposit(amount)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute fundDeposit: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute fundDeposit: %v", err))
			return
		}

		respondOk(w, []byte("fundDeposit success"))
	}))
}

func unlockHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tx, err := client.Unlock()
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute unlock: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute unlock: %v", err))
			return
		}

		respondOk(w, []byte("unlock success"))
	}))
}

func cancelUnlockHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tx, err := client.CancelUnlock()
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute cancelUnlock: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute cancelUnlock: %v", err))
			return
		}

		respondOk(w, []byte("cancelUnlock success"))
	}))
}

func withdrawHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tx, err := client.Withdraw()
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute withdraw: %v", err))
			return
		}

		err = client.CheckTx(tx)
		if err != nil {
			respond500(w, fmt.Sprintf("could not execute withdraw: %v", err))
			return
		}

		respondOk(w, []byte("withdraw success"))
	}))
}

func senderInfoHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info, err := client.GetSenderInfo(client.Account().Address)
		if err != nil {
			if err.Error() == "ErrNoResult" {
				info = &pm.SenderInfo{
					Deposit:       big.NewInt(0),
					WithdrawRound: big.NewInt(0),
					Reserve: &pm.ReserveInfo{
						FundsRemaining:        big.NewInt(0),
						ClaimedInCurrentRound: big.NewInt(0),
					},
				}
			} else {
				respond500(w, fmt.Sprintf("could not query sender info: %v", err))
				return
			}
		}

		respondJson(w, info)
	}))
}

func ticketBrokerParamsHandler(client eth.LivepeerEthClient) http.Handler {
	return mustHaveClient(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unlockPeriod, err := client.UnlockPeriod()
		if err != nil {
			respond500(w, fmt.Sprintf("could not query TicketBroker unlockPeriod: %v", err))
			return
		}

		params := struct {
			UnlockPeriod *big.Int
		}{
			unlockPeriod,
		}

		respondJson(w, params)
	}))
}

func isL1Network(db ChainIdGetter) (bool, error) {
	chainId, err := db.ChainID()
	if err != nil {
		return false, err
	}
	return chainId.Int64() == MainnetChainId || chainId.Int64() == RinkebyChainId, err
}

// Helpers
func respondOk(w http.ResponseWriter, msg []byte) {
	w.WriteHeader(http.StatusOK)
	if msg != nil {
		if _, err := w.Write(msg); err != nil {
			glog.Error(err)
		}
	}
}

func respondJson(w http.ResponseWriter, v interface{}) {
	if v == nil {
		respond500(w, "unknown internal server error")
		return
	}
	data, err := json.Marshal(v)
	if err != nil {
		respond500(w, err.Error())
	}
	respondJsonOk(w, data)
}

func respondJsonOk(w http.ResponseWriter, msg []byte) {
	w.Header().Set("Content-Type", "application/json")
	respondOk(w, msg)
}

func respond500(w http.ResponseWriter, errMsg string) {
	respondWithError(w, errMsg, http.StatusInternalServerError)
}

func respond400(w http.ResponseWriter, errMsg string) {
	respondWithError(w, errMsg, http.StatusBadRequest)
}

func respondWithError(w http.ResponseWriter, errMsg string, code int) {
	glog.Errorf("HTTP Response Error %v: %v", code, errMsg)
	http.Error(w, errMsg, code)
}

func mustHaveFormParams(h http.Handler, params ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			respond500(w, fmt.Sprintf("parse form error: %v", err))
			return
		}

		for _, param := range params {
			if r.FormValue(param) == "" {
				respond400(w, fmt.Sprintf("missing form param: %s", param))
				return
			}
		}

		h.ServeHTTP(w, r)
	})
}

func mustHaveClient(client eth.LivepeerEthClient, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if client == nil {
			respond500(w, "missing ETH client")
			return
		}
		h.ServeHTTP(w, r)
	})
}

func mustHaveDb(db interface{}, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			respond500(w, "missing database")
			return
		}
		h.ServeHTTP(w, r)
	})
}
