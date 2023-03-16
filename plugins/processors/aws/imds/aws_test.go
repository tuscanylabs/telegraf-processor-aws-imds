package aws

import (
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

func TestBasicStartup(t *testing.T) {
	p := newAwsIMDSProcessor()
	p.Log = &testutil.Logger{}
	p.ImdsTags = []string{"region", "accountId"}
	acc := &testutil.Accumulator{}
	require.NoError(t, p.Init())

	require.Len(t, acc.GetTelegrafMetrics(), 0)
	require.Len(t, acc.Errors, 0)
}
