package rstream

import (
	"time"

	"github.com/fuserobotics/statestream"
)

// A window is a snapshot of a period of time.
// NOTE: Before disposing a window, must reach final state.
// Final states are: [Error, Committed]
type Window interface {
	Data() stream.StorageBackend

	State() WindowState
	StateChanges(chan<- WindowState)
	Error() error

	Meta() *WindowMeta
	MetaChanges(chan<- *WindowMeta)

	InitLive()
	InitWithMidTimestamp(midTimestamp time.Time)
	InitWithMetadata(meta *WindowMeta)

	ContainsTimestamp(time.Time) bool

	Activate()

	Dispose()
	IsDisposed() bool
	OnDisposed(func())
}
