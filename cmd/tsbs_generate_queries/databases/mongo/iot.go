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

func (i *IoT) TrucksWithHighLoad(qi query.Query) {
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
							"current_load": "$current_load",
							"load_capacity": "$tags.load_capacity",
							"current_load_ratio": bson.M{
								"$divide": bson.A{"$current_load", "$tags.load_capacity"},
							},
						},
					},
				},
			},
		}},
		{{
			"$match", bson.M{
				"output.current_load_ratio": bson.M{
					"$gte": 0.9,
				},
			},
		}},
	}
	humanLabel := "MongoDB trucks with high load in a fleet"
	humanDesc := fmt.Sprintf("%s: fleet: (%s)", humanLabel, fleet)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	start := interval.Start()
	// start := interval.Start().Format(goTimeFmt)
	// end := interval.End().Format(goTimeFmt)
	end := interval.End()
	fleet := i.GetRandomFleet()

	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "readings",
				"tags.fleet": fleet,
				"time": bson.M{"$gte": start, "$lt": end },
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$tags.name",
					"driver": "$tags.driver",
					"fleet": "$tags.fleet",
					"bucket": bson.M{
						"$dateTrunc": bson.M{
							"date": "$time",
							"unit": "minute",
							"binSize": 10,
						},
					},
				},
				"avg_velocity": bson.M{
					"$avg": "$velocity",
				},
			},
		}},
		{{
			"$match", bson.M{
				"avg_velocity": bson.M{"$lt": 1.0},
			},
		}},
	}

	humanLabel := "MongoDB stationary trucks (trucks with low velocity)"
	humanDesc := fmt.Sprintf("%s: (%s) in [%v, %v]", humanLabel, fleet, start, end)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	fleet := i.GetRandomFleet()
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	start := interval.Start()
	end := interval.End()
	// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
	numIntervals := tenMinutePeriods(5, iot.LongDrivingSessionDuration)

	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "readings",
				"tags.fleet": fleet,
				"tags.name": bson.M{ "$ne": nil },
				"time": bson.M{"$gte": start, "$lt": end },
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$tags.name",
					"driver": "$tags.driver",
					"fleet": "$tags.fleet",
					"bucket": bson.M{
						"$dateTrunc": bson.M{
							"date": "$time",
							"unit": "minute",
							"binSize": 10,
						},
					},
				},
				"avg_velocity": bson.M{
					"$avg": "$velocity",
				},
			},
		}},
		{{
			"$match", bson.M{
				"avg_velocity": bson.M{"$gte": 1.0},
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$_id.name",
					"driver": "$_id.driver",
				 },
				"active_10_min_sessions": bson.M{"$count": bson.M{}},
			},
		}},
		{{
			"$match", bson.M{
				"active_10_min_sessions": bson.M{"$gt": numIntervals},
			},
		}},
	}

	humanLabel := "MongoDB trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: (%s) stopped less than 20 mins in 4 hour period [%v, %v]", humanLabel, fleet, start, end)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
