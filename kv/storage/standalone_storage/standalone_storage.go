package standalone_storage

import (
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	transactions []*badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := StandAloneStorage{}
	os.MkdirAll(conf.DBPath, os.ModePerm)
	storage.db = engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	s.transactions = append(s.transactions, txn)
	r := NewStandAloneStorageReader(txn)
	return &r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := engine_util.WriteBatch{}
	for _, m := range batch {
		wb.SetCF(m.Cf(), m.Key(), m.Value())
	}
	return wb.WriteToDB(s.db)
}

type StandAloneStorageReader struct {
	txn *badger.Txn
	iterators []engine_util.DBIterator
}

func NewStandAloneStorageReader(txn *badger.Txn) StandAloneStorageReader {
	return StandAloneStorageReader{txn: txn, iterators: make([]engine_util.DBIterator, 0)}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if val == nil {
		return nil, nil
	}
	return val, err
}
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf, r.txn)
	r.iterators = append(r.iterators, it)
	return it
}
func (r *StandAloneStorageReader) Close() {
	for _, it := range r.iterators {
		if it.Valid() {
			it.Close()
		}
	}
	r.txn.Discard()
}