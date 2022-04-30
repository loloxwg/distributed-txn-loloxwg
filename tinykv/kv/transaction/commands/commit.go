package commands

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"reflect"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	// YOUR CODE HERE (lab1).
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.
	log.Debug("Commit", zap.Uint64("startTs", c.startTs), zap.Uint64("commitTs", commitTs))
	if commitTs < c.startTs {
		log.Error("commitTs is invalid", zap.Uint64("startTs", c.startTs), zap.Uint64("commitTs", commitTs))
		return nil, fmt.Errorf("commitTs %d is less than startTs %d", commitTs, c.startTs)
	}

	response := new(kvrpcpb.CommitResponse)

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Error("GetLock", zap.Error(err))
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab1).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			log.Error("CurrentWrite", zap.Error(err))
			return nil, err
		}
		if write != nil {
			log.Info("write is not nil", zap.Uint64("startTS", txn.StartTS),
				zap.Uint64("commitTs", commitTs),
				zap.String("key", hex.EncodeToString(key)),
				zap.Uint64("write_kind", uint64(write.Kind)))
			if write.Kind == mvcc.WriteKindRollback {
				respValue := reflect.ValueOf(response)
				keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("false")}
				reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
				return response, nil
			}
			// 如果 有写 但是写并不是 mvcc.WriteKindRollback 那么什么都不返回 bug fix
			return nil, nil
		}

		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	log.Debug("Commit a Write object to the DB", zap.Uint64("txn.startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
