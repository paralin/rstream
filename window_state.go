package rstream

type WindowState int

const (
	WindowState_Pending WindowState = iota
	WindowState_Waiting
	WindowState_Pulling
	WindowState_Live
	WindowState_Committed
	WindowState_Error
)
