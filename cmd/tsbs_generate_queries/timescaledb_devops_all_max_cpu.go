package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsAllMaxCPU contains info for TimescaleDB-devops test 'cpu-max-all-*'
type TimescaleDBDevopsAllMaxCPU struct {
	TimescaleDBDevops
	hosts int
}

// NewTimescaleDBDevopsAllMaxCPU produces a new function that produces a new TimescaleDBDevopsAllMaxCPU
func NewTimescaleDBDevopsAllMaxCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end)
		return &TimescaleDBDevopsAllMaxCPU{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsAllMaxCPU) Dispatch(scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}