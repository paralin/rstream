package test

import (
	"github.com/fuserobotics/rstream"
	"github.com/fuserobotics/rstream/mock"
	"golang.org/x/net/context"
	"testing"
	"time"
)

type multiplexerTestContext struct {
	WindowFactory rstream.WindowFactory
	Multiplexer   *rstream.WindowMultiplexer
}

func newMultiplexerTestContext() *multiplexerTestContext {
	ref1 := &rstream.WindowFactoryReference{
		Identifier: "1",
		Factory:    newMockWindow,
	}
	return &multiplexerTestContext{
		WindowFactory: newMockWindow,
		Multiplexer:   rstream.NewWindowMultiplexer(context.Background(), ref1),
	}
}

func TestMultiplexer(t *testing.T) {
	testCtx := newMultiplexerTestContext()
	multi := testCtx.Multiplexer
	ref2 := &rstream.WindowFactoryReference{
		Identifier: "2",
		Factory:    newMockWindow,
	}
	multi.AddFactory(ref2)

	windowStateChan := make(chan rstream.WindowState, 2)
	multi.StateChanges(windowStateChan)
	stateSequence := []rstream.WindowState{
		rstream.WindowState_Pending,
		rstream.WindowState_Waiting,
		rstream.WindowState_Pulling,
		rstream.WindowState_Committed,
	}
	multi.Activate()
	multi.InitWithMidTimestamp(mock.RootMockTime.Add(time.Duration(-3) * time.Second))
	states := []rstream.WindowState{}
	for i := 0; i < len(stateSequence); i++ {
		state := <-windowStateChan
		states = append(states, state)
		if state != stateSequence[i] {
			if state == rstream.WindowState_Error {
				t.Errorf("Error: %v", multi.Error())
			}
			t.Fatalf("Expected state at %d to be %v, instead was %v, %v", i, stateSequence[i], state, states)
		}
	}
	t.Log("Success")
}

var compileTimeAssert rstream.Window = &rstream.WindowMultiplexer{}
