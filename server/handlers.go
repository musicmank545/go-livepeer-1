package server

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/golang/glog"
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

func mustHaveDb(db interface{}, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			respond500(w, "missing database")
			return
		}
		h.ServeHTTP(w, r)
	})
}
