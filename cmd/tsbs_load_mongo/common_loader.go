package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type fileDataSource struct {
	lenBuf []byte
	r      *bufio.Reader
}
func parseTag(x map[string]interface{}, t *mongo.MongoTag) (bool) {
	unionTable := new(flatbuffers.Table)
	if(!t.Value(unionTable)) { return false }
	if(t.ValueType() == mongo.MongoTagValueMongoStringTag) {
		stringTag := new(mongo.MongoStringTag)
		stringTag.Init(unionTable.Bytes, unionTable.Pos)
		x[string(t.Key())] = string(stringTag.Value())
	} else if(t.ValueType() == mongo.MongoTagValueMongoFloat32Tag) {
		floatTag := new(mongo.MongoFloat32Tag)
		floatTag.Init(unionTable.Bytes, unionTable.Pos)
		x[string(t.Key())] = float32(floatTag.Value())
	} else {
		return false
	}
	return true
}

func parseTagAsBson(t *mongo.MongoTag) (bson.E, bool) {
	unionTable := new(flatbuffers.Table)
	if(!t.Value(unionTable)) { return bson.E{}, false }
	if(t.ValueType() == mongo.MongoTagValueMongoStringTag) {
		stringTag := new(mongo.MongoStringTag)
		stringTag.Init(unionTable.Bytes, unionTable.Pos)
		return bson.E{string(t.Key()), string(stringTag.Value())}, true
	} else if(t.ValueType() == mongo.MongoTagValueMongoFloat32Tag) {
		floatTag := new(mongo.MongoFloat32Tag)
		floatTag.Init(unionTable.Bytes, unionTable.Pos)
		return bson.E{string(t.Key()), float32(floatTag.Value())}, true
	} else {
		return bson.E{}, false
	}
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	item := &mongo.MongoPoint{}

	_, err := d.r.Read(d.lenBuf)
	if err == io.EOF {
		return data.LoadedPoint{}
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	// ensure correct len of receiving buffer
	l := int(binary.LittleEndian.Uint64(d.lenBuf))
	itemBuf := make([]byte, l)

	// read the bytes and init the flatbuffer object
	totRead := 0
	for totRead < l {
		m, err := d.r.Read(itemBuf[totRead:])
		// (EOF is also fatal)
		if err != nil {
			log.Fatal(err.Error())
		}
		totRead += m
	}
	if totRead != len(itemBuf) {
		panic(fmt.Sprintf("reader/writer logic error, %d != %d", totRead, len(itemBuf)))
	}
	n := flatbuffers.GetUOffsetT(itemBuf)
	item.Init(itemBuf, n)

	return data.NewLoadedPoint(item)
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

type batch struct {
	arr []*mongo.MongoPoint
}

func (b *batch) Len() uint {
	return uint(len(b.arr))
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.(*mongo.MongoPoint)
	b.arr = append(b.arr, that)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{arr: []*mongo.MongoPoint{}}
}

type mongoBenchmark struct {
	loaderFileName string
	l              load.BenchmarkRunner
	dbc            *dbCreator
}

func (b *mongoBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{lenBuf: make([]byte, 8), r: load.GetBufferedReader(b.loaderFileName)}
}

func (b *mongoBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *mongoBenchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}
