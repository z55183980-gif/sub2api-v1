package service

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapperCapacitySnapshotDistinguishesToolWaitFromHTTP(t *testing.T) {
	snapshot := decodeWrapperCapacity(strings.NewReader(`{"cliProcesses":{"total":5,"generating":0,"waitingForTools":5,"limit":5},"sessions":{"queued":0},"availableProcesses":0}`))
	require.True(t, snapshot.Valid)
	require.Equal(t, 0, *snapshot.Generating)
	require.Equal(t, 5, *snapshot.WaitingTools)
	require.Equal(t, 5, *snapshot.TotalProcesses)
	require.Equal(t, 0, *snapshot.IdleRetained)
	require.False(t, snapshot.ObservedAt.IsZero())
}

func TestWrapperCapacitySnapshotMissingOrInconsistentIsUnknown(t *testing.T) {
	for _, body := range []string{`{}`, `broken`, `{"cliProcesses":{"total":5,"limit":5}}`,
		`{"cliProcesses":{"total":5,"generating":0,"waitingForTools":0,"limit":5}}`,
		`{"cliProcesses":{"total":-1,"generating":0,"waitingForTools":-1,"limit":5}}`} {
		snapshot := decodeWrapperCapacity(strings.NewReader(body))
		require.False(t, snapshot.Valid, body)
		require.Nil(t, snapshot.TotalProcesses)
	}
}
