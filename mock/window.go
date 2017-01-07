package mock

import (
	"errors"
	"sync"
	"time"

	"github.com/fuserobotics/rstream"
	sstream "github.com/fuserobotics/statestream"
	"golang.org/x/net/context"
)

const mockDelay time.Duration = time.Duration(100) * time.Millisecond

var OutOfBoundsErr error = errors.New("Window is out of bounds.")

type MockWindow struct {
	*rstream.StandardWindow
	data         *sstream.MemoryBackend
	initedLive   bool
	activateOnce sync.Once
	windowCtx    context.Context
}

func NewMockWindow(ctx context.Context) *MockWindow {
	return &MockWindow{
		StandardWindow: rstream.NewStandardWindow(),
		data:           &sstream.MemoryBackend{Entries: MockDataset()},
		windowCtx:      ctx,
	}
}

func (w *MockWindow) Data() sstream.StorageBackend {
	return w.data
}

func (w *MockWindow) InitLive() {
	w.initedLive = true
	w.InitWithMidTimestamp(time.Now())
}

func (w *MockWindow) InitWithMidTimestamp(midTimestamp time.Time) {
	snap, _ := w.data.GetSnapshotBefore(midTimestamp)
	if snap == nil {
		w.StandardWindow.SetError(OutOfBoundsErr)
		return
	}

	var endSnap *sstream.StreamEntry
	if !w.initedLive {
		endSnap, _ = w.data.GetEntryAfter(midTimestamp, sstream.StreamEntrySnapshot)
	}

	meta := &rstream.WindowMeta{
		StartBound: &snap.Timestamp,
	}

	if endSnap != nil {
		meta.EndBound = &endSnap.Timestamp
	}

	go func(meta *rstream.WindowMeta) {
		time.Sleep(mockDelay)
		w.NextMeta(meta)
		w.NextState(rstream.WindowState_Waiting)
	}(meta)
}

func (w *MockWindow) Activate() {
	w.activateOnce.Do(w.doActivate)
}

func (w *MockWindow) doActivate() {
	w.NextState(rstream.WindowState_Pulling)
	go func() {
		time.Sleep(mockDelay)

		entries := MockDataset()

		for _, ent := range entries {
			w.data.SaveEntry(ent)
			time.Sleep(time.Duration(50) * time.Millisecond)
		}

		w.NextState(rstream.WindowState_Committed)
	}()
}

var mockWindowTypeAssertion rstream.Window = NewMockWindow(context.Background())
