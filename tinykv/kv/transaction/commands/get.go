package commands

import (
	"encoding/hex"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

// 读的时候看看有这样的锁存在吗，没锁就直接读最新的
// 有锁会发生两种情况 1。能读到最新 2。读不到最新的 读不读的到 依赖于最终的committed ts 和 start ts 的大小

func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	// g.request.Version or txn.StartTs log debug information
	log.Debug("[kv] get key", zap.String("key", hex.EncodeToString(key)), zap.Uint64("version", g.request.Version), zap.Uint64("start_ts", txn.StartTS))
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	response := new(kvrpcpb.GetResponse)
	// YOUR CODE HERE (lab1).
	// Check for locks and their visibilities.检查锁的可见性
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Error("get key failed", zap.Uint64("start_ts", txn.StartTS),
			zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return nil, nil, err
	}
	// 本次读请求事务的start ts 比 和锁的start ts 大 返回异常
	if lock != nil && g.request.Version >= lock.Ts {
		response.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         key,
				LockTtl:     lock.Ttl,
			}}
		return response, nil, nil
	}
	// YOUR CODE HERE (lab1).
	// Search writes for a committed value, set results in the response.搜索写入的值，设置结果
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	value, err := txn.GetValue(key)
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)), zap.String("value", string(value)))
	if err != nil {
		log.Error("get value failed", zap.Uint64("start_ts", txn.StartTS),
			zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return nil, nil, err
	}

	if value == nil {
		response.NotFound = true
	}
	response.Value = value

	return response, nil, nil
}
