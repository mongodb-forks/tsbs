//go:build mongo_only
// +build mongo_only

package initializers

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/mongo"
)

func GetTarget(format string) targets.ImplementedTarget {
	if format == constants.FormatMongo {
		return mongo.NewTarget()
	}

	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, constants.FormatMongo))
}
