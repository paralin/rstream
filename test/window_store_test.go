package test

import (
	"github.com/fuserobotics/rstream"
	"github.com/fuserobotics/rstream/mock"
	"testing"
	"time"
)

type testContext struct {
	WindowFactory rstream.WindowFactory
	Store         *rstream.WindowStore
}

func newMockWindow(ws *rstream.WindowStore) rstream.Window {
	return mock.NewMockWindow()
}

func newTestContext() *testContext {
	return &testContext{
		WindowFactory: newMockWindow,
		Store:         rstream.NewWindowStore(newMockWindow),
	}
}

func TestFetchBeginning(t *testing.T) {
	now := mock.RootMockTime
	tc := newTestContext()
	tts := now.Add(time.Duration(-7) * time.Second)
	resChan := tc.Store.BuildWindow(&tts)
	result := <-resChan
	if result.Error != nil {
		t.Fatal(result.Error.Error())
	}
	wind := result.Window
	windowStateChan := make(chan rstream.WindowState, 2)
	wind.StateChanges(windowStateChan)
	stateSequence := []rstream.WindowState{
		rstream.WindowState_Waiting,
		rstream.WindowState_Pulling,
		rstream.WindowState_Committed,
	}
	wind.Activate()
	for i := 0; i < len(stateSequence); i++ {
		if state := <-windowStateChan; state != stateSequence[i] {
			t.Fatalf("Expected state at %d to be %v, instead was %v", i, stateSequence[i], state)
		}
	}
}
