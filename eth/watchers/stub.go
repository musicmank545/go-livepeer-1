package watchers

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/livepeer/go-livepeer/eth/blockwatch"
)

var stubBondingManagerAddr = common.HexToAddress("0x511bc4556d823ae99630ae8de28b9b80df90ea2e")
var stubRoundsManagerAddr = common.HexToAddress("0xc1F9BB72216E5ecDc97e248F65E14df1fE46600a")

func newStubBaseLog() types.Log {
	return types.Log{
		BlockNumber: uint64(30),
		TxHash:      common.HexToHash("0xd9bb5f9e888ee6f74bedcda811c2461230f247c205849d6f83cb6c3925e54586"),
		TxIndex:     uint(0),
		BlockHash:   common.HexToHash("0x6bbf9b6e836207ab25379c20e517a89090cbbaf8877746f6ed7fb6820770816b"),
		Index:       uint(0),
		Removed:     false,
	}
}

func newStubUnbondLog() types.Log {
	log := newStubBaseLog()
	log.Address = stubBondingManagerAddr
	log.Topics = []common.Hash{
		common.HexToHash("0x2d5d98d189bee5496a08db2a5948cb7e5e786f09d17d0c3f228eb41776c24a06"),
		// delegate = 0x525419FF5707190389bfb5C87c375D710F5fCb0E
		common.HexToHash("0x000000000000000000000000525419ff5707190389bfb5c87c375d710f5fcb0e"),
		// delegator = 0xF75b78571F6563e8Acf1899F682Fb10A9248CCE8
		common.HexToHash("0x000000000000000000000000f75b78571f6563e8acf1899f682fb10a9248cce8"),
	}
	// unbondingLockId = 1
	// amount = 11111000000000000000
	// withdrawRound = 1457
	log.Data = common.Hex2Bytes("0x00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000009a3233a1a35d800000000000000000000000000000000000000000000000000000000000000005b1")

	return log
}

func newStubNewRoundLog() types.Log {
	log := newStubBaseLog()
	log.Address = stubRoundsManagerAddr
	topic := crypto.Keccak256Hash([]byte("NewRound(uint256,bytes32)"))
	round := big.NewInt(8)
	roundBytes := common.BytesToHash(common.LeftPadBytes(round.Bytes(), 32))
	log.Topics = []common.Hash{topic, roundBytes}
	log.Data, _ = hexutil.Decode("0x15063b24c3dfd390370cd13eaf27fd0b079c60f31bf1414c574f865e906a8964")
	return log
}

type stubSubscription struct {
	errCh        <-chan error
	unsubscribed bool
}

func (s *stubSubscription) Unsubscribe() {
	s.unsubscribed = true
}

func (s *stubSubscription) Err() <-chan error {
	return s.errCh
}

type stubBlockWatcher struct {
	sink chan<- []*blockwatch.Event
	sub  *stubSubscription
}

func (bw *stubBlockWatcher) Subscribe(sink chan<- []*blockwatch.Event) event.Subscription {
	bw.sink = sink
	bw.sub = &stubSubscription{errCh: make(<-chan error)}
	return bw.sub
}