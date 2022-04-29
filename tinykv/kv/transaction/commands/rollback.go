package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.BatchRollbackResponse)

	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	if lock == nil || lock.Ts != txn.StartTS {
		// There is no lock, check the write status.
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
		// the type. Also the command could be stale that the record is already rolled back or committed.
		// If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		// if the key has already been rolled back, so nothing to do.
		// If the key has already been committed. This should not happen since the client should never send both
		// commit and rollback requests.
		// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		//尝试插入回滚记录如果没有相应的记录，请使用`mvcc。WriteKindRollback`表示
		//类型。此外，如果记录已回滚或提交，则该命令可能已过时。
		//如果也没有写入，则可能预写入已丢失。无论如何，我们都要插入一个回滚写入。
		//如果密钥已回滚，则无需执行任何操作。
		//如果密钥已经提交。这种情况不应该发生，因为客户端永远不应该同时发送这两个消息
		//提交和回滚请求。
		//也没有写入，大概预写入丢失了。无论如何，我们都要插入一个回滚写入。

		//We insert a rollback write anyway.
		if existingWrite == nil {
			// YOUR CODE HERE (lab1).
			txn.Rollback(key, false)
			return nil, nil
		} else {
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				return nil, nil
			}

			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}

	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	txn.DeleteLock(key)

	return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
