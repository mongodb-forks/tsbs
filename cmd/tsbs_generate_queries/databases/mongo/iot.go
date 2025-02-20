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

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
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

func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	fleet := i.GetRandomFleet()
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	start := interval.Start()
	end := interval.End()
	// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
	numIntervals := tenMinutePeriods(35, iot.DailyDrivingDuration)

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

	humanLabel := "MongoDB trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: in fleet (%s) drove more than 10hours in the last 24 hours [%v, %v]", humanLabel, fleet, start, end)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}


// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	fleet := i.GetRandomFleet()

	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "readings",
				"tags.name": bson.M{ "$ne": nil },
				"tags.nominal_fuel_consumption": bson.M{ "$ne": nil },
				"velocity": bson.M{"$gt": 1.0},
			},
		}},
		{{
			"$group", bson.M{
				"_id": "$tags.fleet",
				"mean_fuel_consumption": bson.M{
					"$avg": "$fuel_consumption",
				},
				"nominal_fuel_consumption": bson.M{
					"$avg": "$tags.nominal_fuel_consumption",
				},
			},
		}},
	}

	humanLabel := "MongoDB average vs projected fuel consumption per fleet"
	humanDesc := fmt.Sprintf("%s: in fleet (%s)", humanLabel, fleet)

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "readings",
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"fleet": "$tags.fleet",
					"name": "$tags.name",
					"driver": "$tags.driver",
					"bucket": bson.M{
						"$dateTrunc": bson.M{
							"date": "$time",
							"unit": "minute",
							"binSize": 10,
						},
					},
				},
				"mv": bson.M{ "$avg": "$velocity" },
			},
		}},
		{{
			"$match", bson.M{
				"mv": bson.M{ "$gt": 1 },
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"fleet": "$_id.fleet",
					"name": "$_id.name",
					"driver": "$_id.driver",
					"day": bson.M{
						"$dateTrunc": bson.M{
							"date": "$_id.bucket",
							"unit": "day",
							"binSize": 1,
						},
					},
				},
				"ten_min_per_day": bson.M{
					"$count": bson.M{},
				},
			},
		}},
		{{
			"$addFields", bson.M{
				"hours_per_day": bson.M{
					"$divide": bson.A{ "$ten_min_per_day", 6 },
				},
			},
		}},
	}

	humanLabel := "MongoDB average driver driving duration per day"
	humanDesc := humanLabel

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)	
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "readings",
				"tags.name": bson.M{"$ne": nil },
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$tags.name",
					"fleet": "$tags.fleet",
					"bucket": bson.M{
						"$dateTrunc": bson.M{
							"date": "$time",
							"unit": "minute",
							"binSize": 10,
						},
					},
				},
				"avg_velocity": bson.M{ "$avg" : "$velocity" },
			},
		}},
		{{
			"$addFields", bson.M{
				"isDriving": bson.M{
					"$cond": bson.A{
						bson.M{"$gte": bson.A{"$avg_velocity", 1.0}},
						1.0,
						0.0,
					},
				},
			},
		}},
		{{
			"$setWindowFields", bson.M{
				"partitionBy": "$_id.name",
				"sortBy": bson.M{"_id.bucket": 1},
				"output": bson.M{
					"summedBack": bson.M{
						"$sum": "$isDriving",
						"window": bson.M{
							"documents": bson.A{-1, "current"},
						},
					},
					"summedFront": bson.M{
						"$sum": "$isDriving",
						"window": bson.M{
							"documents": bson.A{"current", 1},
						},
					},
				},
			},
		}},
		{{
			"$match", bson.M{
				"isDriving" : 1,
				"$or": []bson.M{
					bson.M{"summedBack": bson.M{"$eq": 1}},
					bson.M{"summedFront" : bson.M{"$eq": 1}},
				},
			},
		}},
		{{
			"$setWindowFields", bson.M{
				"partitionBy": "$_id.name",
				"sortBy": bson.M{"_id.bucket": 1},
				"output": bson.M{
					"times": bson.M{
						"$push": "$_id.bucket",
						"window": bson.M{
							"documents": bson.A{-1, "current"},
						},
					},
				},
			},
		}},
		{{
			"$match", bson.M{
				"summedFront": 1,
			},
		}},
		{{
			"$addFields", bson.M{
				"interval": bson.M{
					"$cond": bson.A{
						bson.M{"$eq": bson.A{"$summedBack", 1} },
						10,
						bson.M{"$add": bson.A{
							bson.M{
								"$dateDiff": bson.M{
									"startDate": bson.M{
										"$arrayElemAt": bson.A{ "$times", 0 },
									},
									"endDate": bson.M{
										"$arrayElemAt": bson.A{ "$times", 1 },
									},
									"unit": "minute",
								},
							},
							10,
						}},
					},
				},
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$_id.name",
					"day": bson.M{
						"$dateTrunc": bson.M{
							"date": "$_id.bucket", 
							"unit": "hour",
							"binSize": 1,
						 },
					},
				},
				"avgSession": bson.M{
					"$avg": "$interval",
				},
			},
		}},
	}
	humanLabel := "MongoDB average driver driving session without stopping per day"
	humanDesc := humanLabel

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)	
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "diagnostics",
				"tags.model": bson.M{ "$ne" : nil},
				"tags.fleet": bson.M{ "$ne" : nil},
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"fleet": "$tags.fleet",
					"model": "$tags.model",
				},
				"avg_load": bson.M{ "$avg": "$current_load"},
				"capacity": bson.M{ "$first": "$tags.load_capacity"},
			},
		}},
		{{
			"$addFields", bson.M{
				"avg_load_ratio": bson.M{
					"$divide": bson.A{"$avg_load", "$capacity"},
				},
			},
		}},
	}
	humanLabel := "MongoDB average load per truck model per fleet"
	humanDesc := humanLabel

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)	
}

func (i *IoT) DailyTruckActivity(qi query.Query) {
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "diagnostics",
				"tags.model": bson.M{"$ne": nil},
				"tags.fleet": bson.M{"$ne": nil},
				"tags.name": bson.M{"$ne": nil},
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"name": "$tags.name",
					"fleet": "$tags.fleet",
					"model": "$tags.model",
					"ten_min_bucket": bson.M{
						"$dateTrunc": bson.M{
							"date": "$time",
							"unit": "minute",
							"binSize": 10,
						},
					},
				},
				"mean_status": bson.M{ "$avg": "$status"},
			},
		}},
		{{
			"$match", bson.M{
				"mean_status": bson.M{ "$lt": 1.0 },
			},
		}},
		{{
			"$group", bson.M{
				"_id": bson.M{
					"fleet": "$_id.fleet",
					"model": "$_id.model",
                                         "day": bson.M{
						"$dateTrunc": bson.M{
							"date": "$_id.ten_min_bucket",
							"unit": "day",
							"binSize": 1,
						},
					},
				},
				"active_slots_per_day": bson.M{"$count": bson.M{}},
			},
		}},
		{{
			"$addFields", bson.M{
				"daily_activity": bson.M{
					// in total, there are 144 10 minute slots per day
					"$divide": bson.A{"$active_slots_per_day", 144 },
				},
			},
		}},
	}

	humanLabel := "MongoDB daily truck activity per fleet per model"
	humanDesc := humanLabel

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)	
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	pipelineQuery := mongo.Pipeline{
		{{
			"$match", bson.M{
				"measurement": "diagnostics",
				"tags.name": bson.M{"$ne": nil},
				"tags.model": bson.M{"$ne": nil},
			},
		}},
		{{
			"$setWindowFields", bson.M{
				"partitionBy": "$tags.name",
				"sortBy": bson.M{ "time": 1 },
				"output": bson.M{
					"summed": bson.M{
						"$sum": "$status",
						"window": bson.M{
							"documents": bson.A{-1, "current"},
						},
					},
				},
			},
		}},
		{{
			"$match", bson.M{
				"status": bson.M{ "$ne": 0 },
				"$expr": bson.M{
					"$eq": bson.A{ "$status", "$summed" },
				},
			},
		}},
		{{
			"$group", bson.M{
				"_id": "$tags.model",
				"breakdowns": bson.M{"$count": bson.M{}},
			},
		}},
	}

	humanLabel := "MongoDB truck breakdown frequency per model"
	humanDesc := humanLabel

	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.Pipeline = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(humanDesc)	
}
