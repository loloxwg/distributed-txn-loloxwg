package commands

import (
	"encoding/hex"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Prewrite represents the prewrite stage of a transaction. A prewrite contains all writes (but not reads) in a transaction,
// if the whole transaction can be written to underlying storage atomically and without conflicting with other
// transactions (complete or in-progress) then success is returned to the client. If all a client's prewrites succeed,
// then it will send a commit message. I.e., prewrite is the first phase in a two phase commit.
//预写表示事务的预写阶段。预写包含事务中的所有写入（但不是读取），如果整个事务可以原子地写入底层存储，
//并且不会与其他事务（已完成或正在进行）发生冲突，则会将成功返回给客户端。如果客户端的所有预写都成功，
//那么它将发送一条提交消息。也就是说，预写是两阶段提交的第一阶段。两阶段提交中的第一阶段。
type Prewrite struct {
	CommandBase
	request *kvrpcpb.PrewriteRequest
}

func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// PrepareWrites prepares the data to be written to the raftstore. The data flow is as follows.
// The tinysql part:
// 		user client -> insert/delete query -> tinysql server
//      query -> parser -> planner -> executor -> the transaction memory buffer
//		memory buffer -> kv muations -> kv client 2pc committer
//		committer -> prewrite all the keys
//		committer -> commit all the keys
//		tinysql server -> respond to the user client
// The tinykv part:
//		prewrite requests -> transaction mutations -> raft request
//		raft req -> raft router -> raft worker -> peer propose raft req
//		raft worker -> peer receive majority response for the propose raft req  -> peer raft committed entries
//  	raft worker -> process committed entries -> send apply req to apply worker
//		apply worker -> apply the correspond requests to storage(the state machine) -> callback
//		callback -> signal the response action -> response to kv client
func (p *Prewrite) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.PrewriteResponse)

	// Prewrite all mutations in the request.
	//预写请求中的所有突变。
	for _, m := range p.request.Mutations {
		keyError, err := p.prewriteMutation(txn, m)
		if keyError != nil {
			response.Errors = append(response.Errors, keyError)
		} else if err != nil {
			return nil, err
		}
	}

	return response, nil
}

// prewriteMutation prewrites mut to txn. It returns (nil, nil) on success, (err, nil) if the key in mut is already
// locked or there is any other key error, and (nil, err) if an internal error occurs.
func (p *Prewrite) prewriteMutation(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.KeyError, error) {
	key := mut.Key
	value := mut.Value
	log.Debug("prewrite key and value", zap.Uint64("start_ts", txn.StartTS), zap.String("value", hex.EncodeToString(value)), zap.String("key", hex.EncodeToString(key)))
	// YOUR CODE HERE (lab1).
	// Check for write conflicts.
	// Hint: Check the interafaces provided by `mvcc.MvccTxn`. The error type `kvrpcpb.WriteConflict` is used
	//		 denote to write conflict error, try to set error information properly in the `kvrpcpb.KeyError`
	//		 response.
	write, ts, err := txn.MostRecentWrite(key)
	if err != nil {
		return nil, err
	}
	// key already written
	if write != nil && p.request.StartVersion <= ts {
		keyError := &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    p.request.StartVersion,
				ConflictTs: ts,
				Key:        key,
				Primary:    p.request.PrimaryLock,
			}}
		return keyError, nil
	}
	// YOUR CODE HERE (lab1).
	// Check if key is locked. Report key is locked error if lock does exist, note the key could be locked
	// by this transaction already and the current prewrite request is stale.
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	// key already locked
	if lock != nil && lock.Ts != p.request.StartVersion {
		keyError := &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         key,
				LockTtl:     lock.Ttl,
			}}
		return keyError, nil
	}
	// YOUR CODE HERE (lab1).
	// Write a lock and value.
	// Hint: Check the interfaces provided by `mvccTxn.Txn`.
	switch mut.Op {
	case kvrpcpb.Op_Put:
		log.Debug("prewrite put", zap.Uint64("start_ts", txn.StartTS),
			zap.String("key", hex.EncodeToString(key)))
		txn.PutValue(key, value)
		txn.PutLock(key, &mvcc.Lock{
			Primary: p.request.PrimaryLock,
			Ts:      p.request.StartVersion,
			Ttl:     p.request.LockTtl,
			Kind:    mvcc.WriteKindPut})
	case kvrpcpb.Op_Del:
		log.Debug("prewrite del", zap.Uint64("start_ts", txn.StartTS),
			zap.String("key", hex.EncodeToString(key)))
		txn.DeleteValue(key)
		txn.PutLock(key, &mvcc.Lock{
			Primary: p.request.PrimaryLock,
			Ts:      p.request.StartVersion,
			Ttl:     p.request.LockTtl,
			Kind:    mvcc.WriteKindDelete})
	}

	return nil, nil
}

func (p *Prewrite) WillWrite() [][]byte {
	result := [][]byte{}
	for _, m := range p.request.Mutations {
		result = append(result, m.Key)
	}
	return result
}
