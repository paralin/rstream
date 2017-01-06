package rstream

import (
	"sync"
	"time"
)

// Implementation of core window behaviors, implementations can subclass this.
type StandardWindow struct {
	stateMtx  sync.Mutex
	state     WindowState
	stateSubs []chan<- WindowState

	metaMtx  sync.Mutex
	meta     *WindowMeta
	metaSubs []chan<- *WindowMeta

	disposed    bool
	disposeMtx  sync.Mutex
	disposeCbs  []func()
	disposeWait sync.WaitGroup
	err         error
}

func NewStandardWindow() *StandardWindow {
	sw := &StandardWindow{state: WindowState_Pending}
	sw.disposeWait.Add(1)
	return sw
}

func (w *StandardWindow) State() WindowState {
	w.stateMtx.Lock()
	defer w.stateMtx.Unlock()

	return w.state
}

func (w *StandardWindow) StateChanges(ch chan<- WindowState) {
	w.stateMtx.Lock()
	defer w.stateMtx.Unlock()

	w.stateSubs = append(w.stateSubs, ch)
	ch <- w.state
}

func (w *StandardWindow) Meta() *WindowMeta {
	w.metaMtx.Lock()
	defer w.metaMtx.Unlock()

	return w.meta
}

func (w *StandardWindow) MetaChanges(ch chan<- *WindowMeta) {
	w.metaMtx.Lock()
	defer w.metaMtx.Unlock()

	w.metaSubs = append(w.metaSubs, ch)
}

func (w *StandardWindow) InitWithMetadata(meta *WindowMeta) {
	w.metaMtx.Lock()

	if w.meta != nil {
		w.metaMtx.Unlock()
		return
	}

	w.stateMtx.Lock()
	currState := w.state
	w.stateMtx.Unlock()

	if currState != WindowState_Pending {
		w.metaMtx.Unlock()
		return
	}

	w.NextState(WindowState_Waiting)
	w.metaMtx.Unlock()
	w.NextMeta(meta)
}

func (w *StandardWindow) ContainsTimestamp(t time.Time) bool {
	meta := w.Meta()
	if meta == nil || meta.StartBound == nil {
		return false
	}

	return meta.StartBound.Before(t) &&
		(meta.EndBound == nil || meta.EndBound.After(t))
}

func (w *StandardWindow) Dispose() {
	w.disposeMtx.Lock()
	defer w.disposeMtx.Unlock()

	if w.disposed {
		return
	}

	for _, cb := range w.disposeCbs {
		go cb()
	}
	w.disposeCbs = nil
	w.disposed = true
	w.disposeWait.Done()
}

// Sets an error, enters error state, disposes window.
func (w *StandardWindow) SetError(err error) {
	if w.err != nil {
		return
	}

	w.err = err
	w.NextState(WindowState_Error)
	w.Dispose()
}

func (w *StandardWindow) IsDisposed() bool {
	w.disposeMtx.Lock()
	defer w.disposeMtx.Unlock()

	return w.disposed
}

func (w *StandardWindow) Error() error {
	w.stateMtx.Lock()
	defer w.stateMtx.Unlock()

	return w.err
}

func (w *StandardWindow) OnDisposed(cb func()) {
	w.disposeMtx.Lock()
	defer w.disposeMtx.Unlock()

	if w.disposed {
		go cb()
		return
	}

	w.disposeCbs = append(w.disposeCbs, cb)
}

func (w *StandardWindow) NextState(state WindowState) {
	w.stateMtx.Lock()
	defer w.stateMtx.Unlock()

	if state == w.state {
		return
	}

	w.state = state
	for _, ch := range w.stateSubs {
		select {
		case ch <- state:
		default:
		}
	}
}

func (w *StandardWindow) NextMeta(meta *WindowMeta) {
	w.metaMtx.Lock()
	defer w.metaMtx.Unlock()

	if meta == w.meta {
		return
	}

	w.meta = meta
	for _, ch := range w.metaSubs {
		select {
		case ch <- meta:
		default:
		}
	}
}
