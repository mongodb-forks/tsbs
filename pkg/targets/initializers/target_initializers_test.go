package initializers

import (
	"testing"

	"github.com/timescale/tsbs/pkg/targets/constants"
)

func TestGetTargetMongo(t *testing.T) {
	target := GetTarget(constants.FormatMongo)
	if target == nil {
		t.Fatal("expected non-nil target for FormatMongo")
	}
}
