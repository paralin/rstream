package rstream

import (
	"time"
)

// Metadata describing a window.
type WindowMeta struct {
	StartBound *time.Time
	EndBound   *time.Time
}
