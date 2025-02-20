package mongo

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/finance"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

// BaseGenerator contains settings specific for Mongo database.
type BaseGenerator struct {
	UseNaive bool
}

// GenerateEmptyQuery returns an empty query.Mongo.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewMongo()
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	var devops utils.QueryGenerator = &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	if g.UseNaive {
		devops = &NaiveDevops{
			BaseGenerator: g,
			Core:          core,
		}

	}

	return devops, nil
}

func (g *BaseGenerator) NewFinance(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := finance.NewCore(start, end, scale)
	if err != nil {
		return nil, err
	}

	return &Finance{
		BaseGenerator: g,
		Core: core,
	}, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	return &IoT{
		BaseGenerator: g,
		Core:          core,
	}, nil
}
