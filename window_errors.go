package rstream

import (
	"errors"
)

var WindowOutOfRangeErr = errors.New("Window out of range.")
var WindowDisposedErr = errors.New("Window was disposed.")
