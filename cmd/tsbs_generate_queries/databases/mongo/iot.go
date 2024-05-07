package mongo

import (
	"fmt"
//	"strings"
	"time"
	"encoding/gob"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register(bson.D{})
	gob.Register([]bson.M{})
}

func (i *IoT) getTrucksFilterArray(nTrucks int) []string {
	names, err := i.GetRandomTrucks(nTrucks)
	panicIfErr(err)
	return names
}

// IoT produces Mongo-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	trucks := i.getTrucksFilterArray(nTrucks)
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"tags.name": bson.M{
					"$in": trucks,
				},
			},

		}},
		{{
			"$group", bson.M{
				"_id": "$tags.name",
				"output": bson.M{
					"$top": bson.M{
						"sortBy": bson.M{ "time" : -1},
						"output": bson.M{
							"longitude": "$longitude",
							"latitude": "$latitude",
							"time": "$time",
						},
					},
				},
			},
		}},
	}

	humanLabel := "MongoDB last location by specific truck(s)"
	humanDesc := fmt.Sprintf("%s: random %4d trucks (%v)", humanLabel, nTrucks, trucks)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	fleet := i.GetRandomFleet()
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"$and": []bson.M{
					bson.M{ "tags.fleet": fleet },
					bson.M{ "tags.name": bson.M{ "$ne": nil } },
					bson.M{ "measurement": "readings" },
				},
			},
		}},
		{{
			"$group", bson.M{
				"_id": "$tags.name",
				"output": bson.M{
					"$top": bson.M{
						"sortBy": bson.M{ "time" : -1},
						"output": bson.M{
							"longitude": "$longitude",
							"latitude": "$latitude",
							"time": "$time",
							"driver": "$tags.driver",
						},
					},
				},
			},
		}},
	}

	humanLabel := "MongoDB last location for each truck"
	humanDesc := fmt.Sprintf("%s: fleet: (%s)", humanLabel, fleet)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	fleet := i.GetRandomFleet()
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"$and": []bson.M{
					bson.M{ "tags.fleet": fleet },
					bson.M{ "tags.name": bson.M{ "$ne": nil } },
					bson.M{ "measurement": "diagnostics" },
				},
			},
		}},
		{{
			"$group", bson.M{
				"_id": "$tags.name",
				"output": bson.M{
					"$top": bson.M{
						"sortBy": bson.M{ "time" : -1},
						"output": bson.M{
							"driver": "$tags.driver",
							"time": "$time",
							"fleet": "$tags.fleet",
							"fuel": "$fuel_state",
						},
					},
				},
			},
		}},
		{{
			"$match", bson.M{
				"output.fuel": bson.M{
					"$lte": 0.1,
				},
			},
		}},
	}
	humanLabel := "MongoDB trucks with low fuel in a fleet"
	humanDesc := fmt.Sprintf("%s: fleet: (%s)", humanLabel, fleet)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}
