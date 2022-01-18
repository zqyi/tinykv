package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, err
	}

	return &kvrpcpb.RawGetResponse{Value: val}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Cf:    req.GetCf(),
		Value: req.GetValue(),
	}
	batch := []storage.Modify{{Data: put}}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	batch := []storage.Modify{{Data: del}}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var pairs []*kvrpcpb.KvPair
	i := 0
	for iter.Seek(req.StartKey); iter.Valid() && i < int(req.Limit); iter.Next() {
		log.Debugf("iter")
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		i++
	}

	return &kvrpcpb.RawScanResponse{Kvs: pairs}, nil
}
