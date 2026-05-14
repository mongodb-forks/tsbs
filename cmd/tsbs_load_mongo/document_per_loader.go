package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

// makeDeterministicID builds a 12-byte _id from the identity fields of a
// generated event: (host, measurement, timestamp_ns). This matches the wire
// size of the default Mongo ObjectID (12 bytes) so the primary-key index
// shape is unchanged vs. server-generated ids, while making inserts
// idempotent: a retry after a partial success surfaces each already-persisted
// document as an E11000 dup-key, which remainingDocsAfterPartialFailure then
// silently filters out.
//
// All TSBS generators (devops, iot) produce events where (host, measurement,
// timestamp_ns) is unique by construction, so SHA-256 truncated to 96 bits is
// vastly below any practical collision risk for workloads of this size.
func makeDeterministicID(host, measurement string, timestampNS int64) primitive.Binary {
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(timestampNS))
	h := sha256.New()
	h.Write([]byte(host))
	h.Write([]byte{0})
	h.Write([]byte(measurement))
	h.Write([]byte{0})
	h.Write(tsBuf[:])
	sum := h.Sum(nil)
	return primitive.Binary{Subtype: 0x00, Data: sum[:12]}
}

// extractIdentityTag returns the value of the identity tag for the event
// (hostname for devops, name for iot). Empty string if neither is present,
// which is survivable because the _id still encodes measurement+timestamp.
func extractIdentityTag(event *tsbsMongo.MongoPoint) string {
	t := &tsbsMongo.MongoTag{}
	for j := 0; j < event.TagsLength(); j++ {
		event.Tags(t, j)
		key := string(t.Key())
		if key == "hostname" || key == "name" {
			return string(t.Value())
		}
	}
	return ""
}

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


// retryableStepdownCodes lists MongoDB server error codes that signal a
// transient replica-set state change (primary failover, stepdown, shutdown,
// etc.) or other retryable server condition. These are the classic codes that
// appear when a primary steps down and a secondary is promoted mid-write.
// See: https://www.mongodb.com/docs/manual/core/retryable-writes/
var retryableStepdownCodes = map[int]struct{}{
	6:     {}, // HostUnreachable
	7:     {}, // HostNotFound
	89:    {}, // NetworkTimeout
	91:    {}, // ShutdownInProgress
	189:   {}, // PrimarySteppedDown
	262:   {}, // ExceededTimeLimit
	9001:  {}, // SocketException
	10107: {}, // NotWritablePrimary (formerly NotMaster)
	11600: {}, // InterruptedAtShutdown
	11602: {}, // InterruptedDueToReplStateChange
	13435: {}, // NotPrimaryNoSecondaryOk
	13436: {}, // NotPrimaryOrSecondary
	63:    {}, // StaleShardVersion
	150:   {}, // StaleEpoch
	13388: {}, // StaleConfig
	234:   {}, // RetryChangeStream
	133:   {}, // FailedToSatisfyReadPreference
}

// isRetryable reports whether err is a transient error that can plausibly
// succeed on a later attempt, e.g. a network blip or a replica-set failover.
// Non-retryable errors (validation, auth, duplicate key, etc.) return false
// so the caller fails fast rather than burning the entire retry budget.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Respect caller cancellation / deadline explicitly.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A recovered driver panic is only retryable when (a) we've opted into
	// deterministic _ids (so retries are idempotent) AND (b) the panic matches
	// the specific known result[:idIndex] bug signature.
	if pe, ok := err.(*driverPanicError); ok {
		if deterministicIDs && isKnownInsertManyResultSlicePanic(pe.value) {
			return true
		}
		return false
	}
	// context.DeadlineExceeded on the *per-attempt* context is retryable;
	// on the *parent* context the caller will bail out via ctx.Err() anyway.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Driver-level helpers: network errors and driver timeouts are retryable.
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}
	// Server-selection errors surface during a failover window when the driver
	// couldn't find a primary within serverSelectionTimeout. The replica set
	// will have a primary again shortly; the outer maxPerWriteRetryTime budget
	// bounds how long we keep retrying.
	if strings.Contains(err.Error(), "server selection error") {
		return true
	}
	// Duplicate key is never retryable on its own (and on a retry after a
	// partially-succeeded batch we handle this explicitly via
	// BulkWriteException inspection below).
	if mongo.IsDuplicateKeyError(err) {
		return false
	}

	// The driver attaches a "RetryableWriteError" label to any server error
	// it considers retryable.
	type labeled interface {
		HasErrorLabel(string) bool
	}
	if le, ok := err.(labeled); ok && le.HasErrorLabel("RetryableWriteError") {
		return true
	}

	switch e := err.(type) {
	case mongo.CommandError:
		if _, ok := retryableStepdownCodes[int(e.Code)]; ok {
			return true
		}
	case mongo.WriteException:
		if e.WriteConcernError != nil {
			if _, ok := retryableStepdownCodes[e.WriteConcernError.Code]; ok {
				return true
			}
		}
		for _, we := range e.WriteErrors {
			if _, ok := retryableStepdownCodes[we.Code]; ok {
				return true
			}
		}
	case mongo.BulkWriteException:
		if e.WriteConcernError != nil {
			if _, ok := retryableStepdownCodes[e.WriteConcernError.Code]; ok {
				return true
			}
		}
		for _, we := range e.WriteErrors {
			if _, ok := retryableStepdownCodes[we.WriteError.Code]; ok {
				return true
			}
		}
	}

	return false
}

// remainingDocsAfterPartialFailure implements the partial-success recovery
// logic for InsertMany. The MongoDB wire protocol never tells us explicitly
// which documents from the input slice were persisted, so we rely on the
// documented server behaviour:
//
//   - With ordered=true, the server stops at the first WriteError. All docs
//     at indices < firstErrIdx were persisted; the doc at firstErrIdx failed;
//     docs at indices > firstErrIdx were never attempted.
//   - With ordered=false, the server attempts every doc. Every index listed
//     in WriteErrors failed; every other index was persisted.
//
// In both cases we return only the documents that still need to be (re)tried,
// and we drop docs whose per-write error is non-retryable (e.g. duplicate key
// from a prior partially-successful attempt).
func remainingDocsAfterPartialFailure(docs []interface{}, bwe mongo.BulkWriteException, ordered bool) []interface{} {
	if len(bwe.WriteErrors) == 0 {
		return docs
	}
	failedIdx := make(map[int]mongo.WriteError, len(bwe.WriteErrors))
	minIdx := len(docs)
	for _, we := range bwe.WriteErrors {
		failedIdx[we.Index] = we.WriteError
		if we.Index < minIdx {
			minIdx = we.Index
		}
	}

	isNonRetryableWriteErr := func(we mongo.WriteError) bool {
		// Duplicate keys almost always mean the doc was already inserted on a
		// prior attempt - don't keep retrying it.
		if we.Code == 11000 || we.Code == 11001 || we.Code == 12582 {
			return true
		}
		// If the per-write code isn't in our retryable set, treat as terminal.
		_, ok := retryableStepdownCodes[we.Code]
		return !ok
	}

	var remaining []interface{}
	if ordered {
		// Everything before the first failure is persisted; skip it.
		// The doc at minIdx failed - include it only if it's retryable.
		if we, ok := failedIdx[minIdx]; ok && !isNonRetryableWriteErr(we) {
			remaining = append(remaining, docs[minIdx])
		}
		// Everything after the first failure was never attempted; retry all.
		if minIdx+1 < len(docs) {
			remaining = append(remaining, docs[minIdx+1:]...)
		}
	} else {
		for i, d := range docs {
			we, failed := failedIdx[i]
			if !failed {
				// Persisted on this attempt; do not retry.
				continue
			}
			if isNonRetryableWriteErr(we) {
				continue
			}
			remaining = append(remaining, d)
		}
	}
	return remaining
}

// driverPanicError wraps a recovered panic from inside the mongo driver so
// callers (and isRetryable) can reason about it as a typed error.
type driverPanicError struct {
	value interface{}
}

func (e *driverPanicError) Error() string {
	return fmt.Sprintf("panic in mongo InsertMany: %v", e.value)
}

// isKnownInsertManyResultSlicePanic reports whether the recovered panic
// matches the signature of the mongo-go-driver issue in
// (*Collection).insert's WriteCommandError cleanup path, where
// `result = result[:idIndex]` (or the sibling `append(result[:idIndex], ...)`)
// can go out of range when server-reported WriteErrors indices don't line up
// with the post-retry result slice. This panic is still present in every
// released v1.x and v2.x driver version; recovering and retrying it is only
// safe when writes are idempotent (deterministic _ids) - otherwise retrying
// risks silent duplicates because some docs in the batch may already have
// been persisted server-side.
//
// We match on the runtime panic message specifically so that unrelated
// panics (nil-pointer in user code, OOM, etc.) are not treated as retryable.
func isKnownInsertManyResultSlicePanic(v interface{}) bool {
	msg := fmt.Sprintf("%v", v)
	if !strings.Contains(msg, "slice bounds out of range") {
		return false
	}
	// Only the result[:idIndex] / append(result[:idIndex], ...) form uses the
	// `[:N]` shape; other out-of-range panics in the same family (e.g.
	// `[N:M]`) are not this bug and shouldn't be swept in.
	return strings.Contains(msg, "[:-") || strings.Contains(msg, "[:")
}

// safeInsertMany wraps (*mongo.Collection).InsertMany so that any panic
// raised inside the driver is converted into a regular error. The returned
// error is a *driverPanicError when a panic was recovered, so callers can
// distinguish driver panics from ordinary server-returned errors.
func safeInsertMany(ctx context.Context, coll *mongo.Collection, docs []interface{}, opts *options.InsertManyOptions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &driverPanicError{value: r}
		}
	}()
	_, err = coll.InsertMany(ctx, docs, opts)
	return err
}

// insertManyWithRetry runs InsertMany with retry semantics tailored for a
// replica-set failover. It:
//   - bounds each attempt by attemptTimeout so a hung call cannot eat the
//     total budget,
//   - uses exponential backoff (starting at 100ms, capped at 5s) between
//     attempts,
//   - gives up once the parent context is done,
//   - only retries errors classified as transient by isRetryable, and
//   - on BulkWriteException inspects the per-document WriteErrors and retries
//     only the documents that still need to be inserted, respecting the
//     ordered/unordered semantics.
func insertManyWithRetry(
	ctx context.Context,
	coll *mongo.Collection,
	docs []interface{},
	opts *options.InsertManyOptions,
	attemptTimeout time.Duration,
) error {
	ordered := true
	if opts != nil && opts.Ordered != nil {
		ordered = *opts.Ordered
	}

	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	pending := docs
	for attempt := 1; ; attempt++ {
		if len(pending) == 0 {
			return nil
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := safeInsertMany(attemptCtx, coll, pending, opts)
		cancel()
		if err == nil {
			return nil
		}

		// If the error is a partial-failure BulkWriteException, try to narrow
		// `pending` down to the docs that still need to be inserted before
		// deciding whether to retry. This way a single dup-key among 1000
		// otherwise-successful docs on a post-failover retry doesn't torpedo
		// the whole batch.
		if bwe, ok := err.(mongo.BulkWriteException); ok {
			pending = remainingDocsAfterPartialFailure(pending, bwe, ordered)
			if len(pending) == 0 {
				// Every remaining doc either succeeded or failed with a
				// non-retryable error that we're explicitly choosing to drop.
				return nil
			}
		}

		if !isRetryable(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("Transient mongo write error (attempt %d, %d docs pending): %s", attempt, len(pending), err.Error())

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	// If a prior batch had a terminal write error, short-circuit so workers drain fast
	// and RunBenchmark can reach postRun/summary. We still consume the batch so
	// the scanner's flow-control doesn't deadlock.
	// if loadFailed() {
	// 	return 0, 0
	// }

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
			if deterministicIDs {
				(*x)["_id"] = makeDeterministicID(
					extractIdentityTag(event),
					string(event.MeasurementName()),
					event.Timestamp(),
				)
			}
			p.pvs[i] = x
			metricCnt += uint64(event.FieldsLength())
		}
	} else {
		for i, event := range batch {
			x := bson.D{}
			if deterministicIDs {
				x = append(x, bson.E{"_id", makeDeterministicID(
					extractIdentityTag(event),
					string(event.MeasurementName()),
					event.Timestamp(),
				)})
			}
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
		// Each individual attempt is bounded by the configurable TSBS parameter writeTimeout (defaults to 30s). 
		attemptTimeout := writeTimeout
		if attemptTimeout <= 0 {
			attemptTimeout = 30 * time.Second
		}

		// If maxPerWriteRetryTime is set, we use it to bound the total time spent retrying. Otherwise, we use the attemptTimeout.
		var (
			ctx    = context.Background()
			cancel context.CancelFunc
		)
		if maxPerWriteRetryTime > 0 {
			ctx, cancel = context.WithTimeout(ctx, maxPerWriteRetryTime)
		} else {
			ctx, cancel = context.WithTimeout(ctx, attemptTimeout)
		}
		var err error
		if maxPerWriteRetryTime > 0 {
			err = insertManyWithRetry(ctx, p.collection, p.pvs, opts, attemptTimeout)
		} else {
			err = safeInsertMany(ctx, p.collection, p.pvs, opts)
		}
		cancel()
		if err != nil {

			log.Fatalf("Bulk insert docs err: %s\n", err.Error())

			// code that tests not fatally logging during workload. 
			// if maxPerWriteRetryTime > 0 { 
			// 	// Record the failure and stop counting metrics for this batch,
			// 	// but do NOT kill the process: RunBenchmark must still reach
			// 	// postRun so the "N workers (mean rate X metrics/sec)" summary
			// 	// line is printed. main() will exit non-zero after that.
			// 	log.Printf("Bulk insert docs err: %s\n", err.Error())
			// 	setLoadFailed(err)
			// 	for _, pv := range p.pvs {
			// 		spPool.Put(pv)
			// 	}
			// 	return 0, 0

			// } else {
			// 	log.Fatalf("Bulk insert docs err: %s\n", err.Error())
			// }
			

			
		}
	}
	for _, p := range p.pvs {
		spPool.Put(p)
	}

	return metricCnt, 0
}
