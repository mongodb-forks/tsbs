package main

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	tsbsMongo "github.com/timescale/tsbs/pkg/targets/mongo"
)

// naiveBenchmark allows you to run a benchmark using the naive, one document per
// event Mongo approach
type naiveBenchmark struct {
	mongoBenchmark
}

func newNaiveBenchmark(l load.BenchmarkRunner, loaderConf *load.BenchmarkRunnerConfig) *naiveBenchmark {
	return &naiveBenchmark{mongoBenchmark{loaderConf.FileName, l, &dbCreator{}}}
}

func (b *naiveBenchmark) GetProcessor() targets.Processor {
	return &naiveProcessor{dbc: b.dbc}
}

func (b *naiveBenchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

type singlePoint map[string]interface{}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

type naiveProcessor struct {
	dbc        *dbCreator
	collection *mongo.Collection

	pvs []interface{}
}

func (p *naiveProcessor) Init(_ int, doLoad, _ bool) {
	if doLoad {
		p.collection = p.dbc.client.Database(loader.DatabaseName()).Collection(collectionName)
	}
	p.pvs = []interface{}{}
}

func withRetry(ctx context.Context, maxAttempts int, op func(context.Context) error) error {
    backoff := 100 * time.Millisecond

    for attempt := 1; ; attempt++ {
        err := op(ctx)
        if err == nil {
            return nil
        }

        // Stop on non‑retryable errors (optional but recommended)
        //var le mongo.LabeledError
        //if !errors.As(err, &le) || !le.HasErrorLabel("RetryableError") && !le.HasErrorLabel("RetryableWriteError") {
        //    return err
        //}

        // Give up if context is done or attempts exhausted
        if ctx.Err() != nil || (maxAttempts > 0 && attempt >= maxAttempts) {
            if ctx.Err() != nil {
                return ctx.Err()
            }
            return err
        }

        // Simple exponential backoff
        time.Sleep(backoff)
        backoff *= 2
    }
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch).arr
	if cap(p.pvs) < len(batch) {
		p.pvs = make([]interface{}, len(batch))
	}
	p.pvs = p.pvs[:len(batch)]
	var metricCnt uint64

	if randomFieldOrder {
		for i, event := range batch {
			x := spPool.Get().(*singlePoint)
			(*x)["measurement"] = string(event.MeasurementName())
			(*x)[timestampField] = time.Unix(0, event.Timestamp())
			(*x)["tags"] = map[string]string{}
			f := &tsbsMongo.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				(*x)[string(f.Key())] = f.Value()
			}
			t := &tsbsMongo.MongoTag{}
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				(*x)["tags"].(map[string]string)[string(t.Key())] = string(t.Value())
			}
			p.pvs[i] = x
			metricCnt += uint64(event.FieldsLength())
		}
	} else {
		for i, event := range batch {
			x := bson.D{}
			x = append(x, bson.E{"measurement", string(event.MeasurementName())})
			x = append(x, bson.E{timestampField, time.Unix(0, event.Timestamp())})
			f := &tsbsMongo.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				x = append(x, bson.E{string(f.Key()), f.Value()})
			}
			t := &tsbsMongo.MongoTag{}
			tags := bson.D{}
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				tags = append(tags, bson.E{string(t.Key()), string(t.Value())})
			}
			x = append(x, bson.E{"tags", tags})
			p.pvs[i] = x
			metricCnt += uint64(event.FieldsLength())
		}
	}

	if doLoad {
		opts := options.InsertMany().SetOrdered(orderedInserts)
        ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
        defer cancel()
		//_, err := p.collection.InsertMany(ctx, p.pvs, opts)
        err := withRetry(ctx, 0, func(c context.Context) error {
		    _, err := p.collection.InsertMany(ctx, p.pvs, opts)
            return err
        })
		if err != nil {
			log.Fatalf("Bulk insert docs err: %s\n", err.Error())
		}
	}
	for _, p := range p.pvs {
		spPool.Put(p)
	}

	return metricCnt, 0
}
