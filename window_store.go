package rstream

import (
	"errors"
	"sync"
	"time"

	"github.com/oleiade/lane"
)

type pendingWindowInterest struct {
	Timestamp *time.Time
	Chan      chan<- *BuildWindowResult
}

type BuildWindowResult struct {
	Error  error
	Window Window
}

type WindowFactory func(ws *WindowStore) Window

// A time linear storage of windows.
type WindowStore struct {
	// List of windows ordered over time.
	windows       []Window
	windowsMtx    sync.RWMutex
	windowFactory WindowFactory

	// List of pending window interests.
	pendingInterests    *lane.Queue
	pendingInterestWake chan bool

	// Disposed
	disposed     bool
	disposedMtx  sync.Mutex
	disposedChan chan bool
}

func NewWindowStore(windowFactory WindowFactory) *WindowStore {
	if windowFactory == nil {
		return nil
	}

	res := &WindowStore{
		pendingInterests:    lane.NewQueue(),
		pendingInterestWake: make(chan bool, 1),
		windowFactory:       windowFactory,
		disposedChan:        make(chan bool, 1),
	}
	go res.update()
	return res
}

// Build a window around a mid timestamp, or live if nil.
func (ws *WindowStore) BuildWindow(midTime *time.Time) <-chan *BuildWindowResult {
	ws.disposedMtx.Lock()
	defer ws.disposedMtx.Unlock()

	if ws.disposed {
		return nil
	}

	ch := make(chan *BuildWindowResult, 1)
	existingWindow := ws.getExistingWindow(midTime)
	if existingWindow != nil {
		ch <- &BuildWindowResult{
			Error:  nil,
			Window: existingWindow,
		}
		return ch
	}

	pwi := &pendingWindowInterest{
		Timestamp: midTime,
		Chan:      ch,
	}
	ws.pendingInterests.Enqueue(pwi)
	ws.wakeNewPendingInterest()
	return ch
}

// Update logic loop
func (ws *WindowStore) update() {
	defer ws.disposeCleanup()
	for {
		select {
		case <-ws.pendingInterestWake:
		case <-ws.disposedChan:
			return
		}

		var pendingInterest *pendingWindowInterest
		pendingInterestInter := ws.pendingInterests.Dequeue()
		if pendingInterestInter == nil {
			continue
		} else {
			pendingInterest = pendingInterestInter.(*pendingWindowInterest)
			existingWindow := ws.getExistingWindow(pendingInterest.Timestamp)
			if existingWindow != nil {
				pendingInterest.Chan <- &BuildWindowResult{Window: existingWindow}
				continue
			}
		}

		pendingWindow := ws.windowFactory(ws)
		if pendingWindow == nil {
			continue
		}
		if pendingInterest.Timestamp == nil {
			pendingWindow.InitLive()
		} else {
			pendingWindow.InitWithMidTimestamp(*pendingInterest.Timestamp)
		}

		wsChan := make(chan WindowState, 3)
		pendingWindow.StateChanges(wsChan)

	WaitForWindowLoop:
		for {
			select {
			case state := <-wsChan:
				if pendingWindow.IsDisposed() {
					if err := pendingWindow.Error(); err != nil {
						pendingInterest.Chan <- &BuildWindowResult{Error: err}
					} else {
						ws.pendingInterests.Enqueue(pendingInterest)
						ws.wakeNewPendingInterest()
						pendingInterest = nil
					}
					break WaitForWindowLoop
				}
				if state != WindowState_Pending {
					break WaitForWindowLoop
				}
			case <-ws.disposedChan:
				pendingWindow.Dispose()
				return
			}
		}

		if pendingInterest == nil {
			continue
		}

		windowMeta := pendingWindow.Meta()
		if windowMeta == nil || windowMeta.StartBound == nil {
			ws.pendingInterests.Enqueue(pendingInterest)
			ws.wakeNewPendingInterest()
			continue
		}

		ws.insertNewWindow(pendingWindow)
		pendingInterest.Chan <- &BuildWindowResult{Window: pendingWindow}
	}
}

// Wake logic loop when a new pending interest comes in.
func (ws *WindowStore) wakeNewPendingInterest() {
	select {
	case ws.pendingInterestWake <- true:
	default:
	}
}

func (ws *WindowStore) insertNewWindow(wind Window) {
	ws.windowsMtx.Lock()
	defer ws.windowsMtx.Unlock()

	windMeta := wind.Meta()
	if windMeta == nil || windMeta.StartBound == nil {
		return
	}

	defer func() {
		lwind := wind
		wind.OnDisposed(func() {
			ws.disposedMtx.Lock()
			defer ws.disposedMtx.Unlock()

			if ws.disposed {
				return
			}

			ws.windowsMtx.Lock()
			defer ws.windowsMtx.Unlock()

			for i, wind := range ws.windows {
				if wind == lwind {
					a := ws.windows
					copy(a[i:], a[i+1:])
					a[len(a)-1] = nil
					a = a[:len(a)-1]
					ws.windows = a
					return
				}
			}
		})
	}()

	if windMeta.EndBound == nil {
		ws.windows = append(ws.windows, wind)
		return
	}
	for idx, ewind := range ws.windows {
		// Shouldn't happen.. but just in case.
		if ewind == wind {
			return
		}
		ewindMeta := ewind.Meta()
		if ewindMeta == nil {
			continue
		}
		// If this existing window is AFTER our window we are inserting,
		// insert at this index (i.e. shift the rest of the array over)
		if ewindMeta.StartBound != nil &&
			ewindMeta.StartBound.After(*windMeta.StartBound) {
			s := ws.windows
			ws.windows = append(s[:idx], append([]Window{wind}, s[idx:]...)...)
			return
		}
	}

	// This happens when the array is empty.
	ws.windows = append(ws.windows, wind)
}

func (ws *WindowStore) getExistingWindow(midTime *time.Time) Window {
	ws.windowsMtx.RLock()
	defer ws.windowsMtx.RUnlock()
	windowCount := len(ws.windows)
	if windowCount == 0 {
		return nil
	}

	if midTime == nil || midTime.Unix() == 0 {
		wind := ws.windows[windowCount-1]
		windMeta := wind.Meta()
		if windMeta != nil && windMeta.EndBound == nil {
			return wind
		}
		return nil
	}

	for _, wind := range ws.windows {
		if wind.ContainsTimestamp(*midTime) {
			return wind
		}
	}
	return nil
}

func (ws *WindowStore) Dispose() {
	ws.disposedMtx.Lock()
	defer ws.disposedMtx.Unlock()

	if ws.disposed {
		return
	}

	ws.disposed = true
	ws.disposedChan <- true
}

func (ws *WindowStore) disposeCleanup() {
	// In case this hasn't been called yet.
	ws.Dispose()

	for _, wind := range ws.windows {
		wind.Dispose()
	}
	ws.windows = nil

	for !ws.pendingInterests.Empty() {
		interestInter := ws.pendingInterests.Dequeue()
		if interestInter == nil {
			continue
		}
		interest := interestInter.(*pendingWindowInterest)
		interest.Chan <- &BuildWindowResult{
			Error: errors.New("Window store is being disposed."),
		}
	}
}
