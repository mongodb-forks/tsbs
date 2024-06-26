package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type StochasticOscillator struct {
	core     utils.QueryGenerator
	span     time.Duration
	interval time.Duration
	points   int
}

func NewStochasticOscillator(span, interval time.Duration, points int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &StochasticOscillator{
			core, span, interval, points,
		}
	}
}

func (d *StochasticOscillator) Fill(q query.Query) query.Query {
	fc, ok := d.core.(StochasticOscillatorFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.StochasticOscillator(q, d.span, d.interval, d.points)
	return q
}
