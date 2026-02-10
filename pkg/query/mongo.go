package query

import (
	"encoding/json"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Mongo encodes a Mongo request. This will be serialized for use
// by the tsbs_run_queries_mongo program.
type Mongo struct {
	HumanLabel       []byte
	HumanDescription []byte
	CollectionName   []byte
	Pipeline         mongo.Pipeline
	id               uint64
}

// MongoPool is a sync.Pool of Mongo Query types
var MongoPool = sync.Pool{
	New: func() interface{} {
		return &Mongo{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			CollectionName:   []byte{},
			Pipeline:         mongo.Pipeline{},
		}
	},
}

// NewMongo returns a new Mongo Query instance
func NewMongo() *Mongo {
	return MongoPool.Get().(*Mongo)
}

// GetID returns the ID of this Query
func (q *Mongo) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *Mongo) SetID(id uint64) {
	q.id = id
}

// String produces a debug-ready description of a Query.
func (q *Mongo) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s", q.HumanLabel, q.HumanDescription)
}

// HumanLabelName returns the human readable name of this Query
func (q *Mongo) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *Mongo) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *Mongo) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.CollectionName = q.CollectionName[:0]
	q.Pipeline = nil

	MongoPool.Put(q)
}

// mongoQueryJSON is the JSON-serializable representation of a Mongo query.
type mongoQueryJSON struct {
	Label      string            `json:"label"`
	Collection string            `json:"collection"`
	Pipeline   []json.RawMessage `json:"pipeline"`
}

// ToExtJSON serializes the Mongo query as a JSON object using MongoDB Extended JSON
// for the pipeline stages. The output is suitable for consumption by pymongo's
// bson.json_util.loads().
func (q *Mongo) ToExtJSON() ([]byte, error) {
	stages := make([]json.RawMessage, len(q.Pipeline))
	for i, stage := range q.Pipeline {
		// Each stage is a bson.D. MarshalExtJSON produces Extended JSON
		// which preserves types like $date for time.Time values.
		extJSON, err := bson.MarshalExtJSON(stage, false, false)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal pipeline stage %d: %w", i, err)
		}
		stages[i] = extJSON
	}

	out := mongoQueryJSON{
		Label:      string(q.HumanLabel),
		Collection: string(q.CollectionName),
		Pipeline:   stages,
	}
	return json.Marshal(out)
}
