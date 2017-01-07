package test

import (
	"github.com/fuserobotics/rstream"
	"github.com/fuserobotics/rstream/mock"
	"golang.org/x/net/context"
	"testing"
	"time"
)

type storeTestContext struct {
	WindowFactory rstream.WindowFactory
	Store         *rstream.WindowStore
}

func newMockWindow(ctx context.Context) rstream.Window {
	return mock.NewMockWindow(ctx)
}

func newStoreTestContext() *storeTestContext {
	return &storeTestContext{
		WindowFactory: newMockWindow,
		Store:         rstream.NewWindowStore(context.Background(), newMockWindow),
	}
}

func TestFetchBeginning(t *testing.T) {
	now := mock.RootMockTime
	tc := newStoreTestContext()
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
	wind.Dispose()
	if wind.State() != rstream.WindowState_Committed {
		t.Fatalf("Error should not be set after committed when disposing.")
	}
}
