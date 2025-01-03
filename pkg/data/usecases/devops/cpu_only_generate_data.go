package devops

import (
	"fmt"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
	*commonDevopsSimulator
}

// Fields returns a map of subsystems to metrics collected
func (d *CPUOnlySimulator) Fields() map[string][]string {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

func (d *CPUOnlySimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *data.Point) bool {
	fmt.Println("batchHostPoints: ", d.batchHostPoints)
	// Switch to the next metric if needed
	if (d.batchHostPoints) {
		// Max points is across all the hosts.
		// TODO: account for the extra points that might be not included when we round down
		if d.madePoints == uint64(d.maxPoints/uint64(len(d.hosts))) {
			d.hostIndex++
			d.madePoints = 0
			// TODO: figure out if this is the appropriate place to adjust num hosts for epoch...
			d.adjustNumHostsForEpoch()
		}
		// We have generated all of our points and we want to signal we are finished 
		if d.hostIndex == uint64(len(d.hosts)) {
			return false 
		} 
		// We only tick the points for the current host index.
		d.hosts[d.hostIndex].TickAll(d.interval)
	} else {
		if d.hostIndex == uint64(len(d.hosts)) {
			d.hostIndex = 0

			for i := 0; i < len(d.hosts); i++ {
				d.hosts[i].TickAll(d.interval)
			}
			d.adjustNumHostsForEpoch()
		}
	}
	return d.populatePoint(p, 0)
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig commonDevopsSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *CPUOnlySimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, c.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = c.HostConstructor(NewHostCtx(i, c.Start))
	}

	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c), interval)
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &CPUOnlySimulator{&commonDevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		epoch:          0,
		epochs:         epochs,
		epochHosts:     c.InitHostCount,
		initHosts:      c.InitHostCount,
		timestampStart: c.Start,
		timestampEnd:   c.End,
		interval:       interval,
		batchHostPoints: c.BatchHostPoints,
	}}

	return sim
}
