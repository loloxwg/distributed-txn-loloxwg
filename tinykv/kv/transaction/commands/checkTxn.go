package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.LockTs,
		},
		request: request,
	}
}

func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	key := c.request.PrimaryKey
	response := new(kvrpcpb.CheckTxnStatusResponse)

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts == txn.StartTS {
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {
			// YOUR CODE HERE (lab1).
			// You should check the lock's TTL and return the corresponding response.
			/* rollback operations
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			log.Info("checkTxnStatus", zap.Uint64("startTs", c.startTs), zap.Uint64("currentTs", c.request.CurrentTs), zap.Uint64("lockTs", lock.Ts), zap.Uint64("ttl", lock.Ttl))
			txn.PutWrite(key, lock.Ts, &mvcc.Write{
				StartTS: lock.Ts,
				Kind:    mvcc.WriteKindRollback,
			})
			*/
			txn.Rollback(c.request.PrimaryKey, true)
			//三种 txn 状态：
			//locked: lock_ttl > 0
			//commit: commit_version > 0
			//roll back: lock_ttl == 0 && commit_version == 0
			response.LockTtl = 0
			response.Action = kvrpcpb.Action_TTLExpireRollback

			// Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
			// represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
			log.Info("checkTxnStatus rollback the primary lock as it's expired",
				zap.Uint64("lock.TS", lock.Ts),
				zap.Uint64("physical(lock.TS)", physical(lock.Ts)),
				zap.Uint64("txn.StartTS", txn.StartTS),
				zap.Uint64("currentTS", c.request.CurrentTs),
				zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))
			return response, nil
		} else {
			// Lock has not expired, leave it alone.
			response.Action = kvrpcpb.Action_NoAction
			response.LockTtl = lock.Ttl
		}

		return response, nil
	}

	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}

	if existingWrite == nil {
		// YOUR CODE HERE (lab1).
		// The lock never existed, it's still needed to put a rollback record on it so that
		// the stale transaction commands such as prewrite on the key will fail.
		// Note try to set correct `response.Action`,
		// the action types could be found in kvrpcpb.Action_xxx.
		txn.PutWrite(key, c.request.LockTs, &mvcc.Write{
			StartTS: c.request.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		response.Action = kvrpcpb.Action_LockNotExistRollback

		return response, nil
	}

	if existingWrite.Kind == mvcc.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}

	// The key has already been committed.
	response.CommitVersion = commitTs
	response.Action = kvrpcpb.Action_NoAction
	return response, nil
}

func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite() [][]byte {
	return [][]byte{c.request.PrimaryKey}
}
