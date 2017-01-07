package rstream

import (
	"errors"
)

var WindowOutOfRangeErr = errors.New("Window out of range.")
var WindowDisposedErr = errors.New("Window was disposed.")
var WindowNoDataErr = errors.New("Window returned no data.")
