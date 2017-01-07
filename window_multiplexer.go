package rstream

import (
	sstream "github.com/fuserobotics/statestream"
	"golang.org/x/net/context"
	"sync"
	"time"
)

// TODO: handle if we init the multiplexer with metadata.

type WindowFactoryReference struct {
	Identifier string
	Factory    WindowFactory
}

func (w *WindowFactoryReference) IsNil() bool {
	return w == nil || w.Factory == nil || w.Identifier == ""
}

type multiplexedWindow struct {
	window            Window
	factoryIdentifier string

	windowContext       context.Context
	windowContextCancel context.CancelFunc
	initChan            chan *windowMultiplexerInit
	activateChan        chan bool
}

type windowMultiplexerInit struct {
	useLive      bool
	midTime      *time.Time
	existingMeta *WindowMeta
}

type WindowMultiplexer struct {
	*StandardWindow

	factoryMtx        sync.RWMutex
	factoryReferences map[string]*WindowFactoryReference
	factoryInstances  map[string]*multiplexedWindow
	factoryFailures   map[string]error

	initMtx           sync.RWMutex
	initData          *windowMultiplexerInit
	activated         bool
	activateAfterInit bool

	isolatedMtx sync.Mutex
	isolated    bool

	context       context.Context
	contextCancel context.CancelFunc

	data *sstream.MemoryBackend
}

func NewWindowMultiplexer(ctx context.Context, factories ...*WindowFactoryReference) *WindowMultiplexer {
	res := &WindowMultiplexer{
		StandardWindow:    NewStandardWindow(),
		factoryReferences: make(map[string]*WindowFactoryReference),
		factoryInstances:  make(map[string]*multiplexedWindow),
		factoryFailures:   make(map[string]error),
		data:              &sstream.MemoryBackend{},
	}
	res.context, res.contextCancel = context.WithCancel(ctx)
	for _, fact := range factories {
		res.AddFactory(fact)
	}
	return res
}

func (w *WindowMultiplexer) Activate() {
	w.initMtx.Lock()
	defer w.initMtx.Unlock()

	w.activateAfterInit = true
	if w.activated || w.state != WindowState_Waiting {
		return
	}

	w.factoryMtx.Lock()
	defer w.factoryMtx.Unlock()

	w.NextState(WindowState_Pulling)
	if w.meta != nil {
		w.activated = true
		for _, wind := range w.factoryInstances {
			select {
			case wind.activateChan <- true:
			default:
			}
		}
	}
}

func (w *WindowMultiplexer) InitLive() {
	w.applyInitData(&windowMultiplexerInit{useLive: true})
}

func (w *WindowMultiplexer) InitWithMetadata(meta *WindowMeta) {
	w.applyInitData(&windowMultiplexerInit{existingMeta: meta})
}

func (w *WindowMultiplexer) InitWithMidTimestamp(ts time.Time) {
	tsi := ts
	w.applyInitData(&windowMultiplexerInit{midTime: &tsi})
}

func (w *WindowMultiplexer) applyInitData(dat *windowMultiplexerInit) {
	w.initMtx.Lock()
	defer w.initMtx.Unlock()

	if w.initData != nil {
		return
	}
	w.initData = dat

	if w.state == WindowState_Pending && dat.existingMeta != nil {
		w.NextState(WindowState_Waiting)
	}

	for _, wind := range w.factoryInstances {
		select {
		case wind.initChan <- w.initData:
		default:
		}
	}
}

func (w *WindowMultiplexer) Data() sstream.StorageBackend {
	return w.data
}

func (w *WindowMultiplexer) AddFactory(fact *WindowFactoryReference) {
	if fact.IsNil() {
		return
	}

	w.factoryMtx.Lock()
	defer w.factoryMtx.Unlock()

	if _, ok := w.factoryInstances[fact.Identifier]; ok {
		return
	}

	w.factoryReferences[fact.Identifier] = fact
	w.instanceFactory(fact)
}

func (w *WindowMultiplexer) DeleteFactory(fact *WindowFactoryReference) {
	if fact.IsNil() {
		return
	}

	w.factoryMtx.Lock()
	defer w.factoryMtx.Unlock()

	_, ok := w.factoryReferences[fact.Identifier]
	if !ok {
		return
	}
	delete(w.factoryReferences, fact.Identifier)
	w.deleteFactoryInstances(fact.Identifier)
}

// Note: lock factoryMtx first.
func (w *WindowMultiplexer) instanceFactory(fact *WindowFactoryReference) {
	if w.State() == WindowState_Committed || w.isolated {
		return
	}
	_, ok := w.factoryInstances[fact.Identifier]
	if ok {
		return
	}
	wind := &multiplexedWindow{
		factoryIdentifier: fact.Identifier,
		initChan:          make(chan *windowMultiplexerInit, 1),
		activateChan:      make(chan bool, 1),
	}
	wind.windowContext, wind.windowContextCancel = context.WithCancel(w.context)
	wind.window = fact.Factory(wind.windowContext)

	w.initMtx.RLock()
	go w.handleWindow(wind)
	w.factoryInstances[fact.Identifier] = wind
	if w.initData != nil {
		wind.initChan <- w.initData
	}
	if w.activated {
		wind.activateChan <- true
	}
	w.initMtx.RUnlock()
}

func (w *WindowMultiplexer) deleteFactoryInstances(id string) {
	inst, ok := w.factoryInstances[id]
	if !ok {
		return
	}
	delete(w.factoryInstances, id)
	inst.windowContextCancel()
	if inst.window != nil {
		go inst.window.Dispose()
	}
}

func (w *WindowMultiplexer) windowGotMeta(meta *WindowMeta) bool {
	w.metaMtx.Lock()
	currMeta := w.meta
	w.metaMtx.Unlock()

	if currMeta != nil {
		return false
	}

	w.NextMeta(meta)
	w.NextState(WindowState_Waiting)
	if w.activateAfterInit {
		go w.Activate()
	}
	return true
}

func (w *WindowMultiplexer) isolateWindow(id string) bool {
	w.isolatedMtx.Lock()
	defer w.isolatedMtx.Unlock()
	if w.isolated {
		return false
	}
	w.isolated = true
	w.factoryMtx.Lock()
	defer w.factoryMtx.Unlock()
	for factId := range w.factoryInstances {
		if factId != id {
			w.deleteFactoryInstances(factId)
		}
	}
	return true
}

// Monitor a window.
func (w *WindowMultiplexer) handleWindow(wind *multiplexedWindow) (windowFailError error) {
	done := wind.windowContext.Done()
	reinstance := false
	defer func() {
		w.factoryMtx.Lock()
		defer w.factoryMtx.Unlock()

		w.deleteFactoryInstances(wind.factoryIdentifier)
		if windowFailError != nil {
			w.factoryFailures[wind.factoryIdentifier] = windowFailError
			if len(w.factoryInstances) == 0 {
				w.SetError(windowFailError)
			}
		} else if reinstance {
			factory, ok := w.factoryReferences[wind.factoryIdentifier]
			if ok {
				go w.instanceFactory(factory)
			}
		}
	}()

	window := wind.window
	windowStateChan := make(chan WindowState, 5)
	window.StateChanges(windowStateChan)
	windowMetaChan := make(chan *WindowMeta, 5)
	window.MetaChanges(windowMetaChan)
	multiplexerMetaChan := make(chan *WindowMeta, 5)
	w.MetaChanges(multiplexerMetaChan)

	// Wait for the window to begin initing.
	initedWithMetadata := false
	select {
	case <-done:
		return nil
	case initData := <-wind.initChan:
		if initData.existingMeta != nil {
			initedWithMetadata = true
			go window.InitWithMetadata(initData.existingMeta)
		} else if initData.useLive || initData.midTime == nil {
			go window.InitLive()
		} else {
			go window.InitWithMidTimestamp(*initData.midTime)
		}
	}

	// Wait for the window to reach a final state OR Waiting
WaitInitLoop:
	for {
		select {
		case <-done:
			return nil
		case state := <-windowStateChan:
			if state == WindowState_Error {
				return wind.window.Error()
			}
			if initedWithMetadata && state == WindowState_Waiting {
				break WaitInitLoop
			}
		case meta := <-windowMetaChan:
			if meta != nil && !initedWithMetadata {
				if w.windowGotMeta(meta) {
					break WaitInitLoop
				} else {
					reinstance = true
					return nil
				}
			}
		case meta := <-multiplexerMetaChan:
			if meta != nil && !initedWithMetadata {
				w.disposeMtx.Lock()
				reinstance = !w.disposed
				w.disposeMtx.Unlock()
			}
			return nil
		}
	}

	// Wait for activation
	select {
	case <-done:
		return nil
	case <-wind.activateChan:
	}

	var entriesCh chan *sstream.StreamEntry
	windData := window.Data()
	windDataStreaming, isStreamable := windData.(sstream.StreamingStorageBackend)
	if isStreamable {
		entriesCh = make(chan *sstream.StreamEntry, 20)
		windDataStreaming.EntryAdded(entriesCh)
	}

	// Issue the activation command
	go window.Activate()

	if isStreamable {
		// Race to first entry!
		isIsolated := false
		for {
			select {
			case <-done:
				return nil
			case state := <-windowStateChan:
				if state == WindowState_Error {
					return window.Error()
				}
				if state == WindowState_Committed {
					if len(w.data.Entries) == 0 {
						return WindowNoDataErr
					} else {
						w.NextState(WindowState_Committed)
						return nil
					}
				}
				if state == WindowState_Live && w.state != WindowState_Live {
					w.NextState(WindowState_Live)
				}
			case entry := <-entriesCh:
				if !isIsolated {
					if !w.isolateWindow(wind.factoryIdentifier) {
						return nil
					}
					isIsolated = true
				}
				w.data.SaveEntry(entry)
			}
		}
	} else {
		for {
			select {
			case <-done:
				return nil
			case state := <-windowStateChan:
				if state == WindowState_Error {
					return window.Error()
				}
				if state == WindowState_Committed {
					if !w.isolateWindow(wind.factoryIdentifier) {
						return nil
					}
					// Basically SaveEntry on all of them.
					return windData.ForEachEntry(func(ent *sstream.StreamEntry) error {
						return w.data.SaveEntry(ent)
					})
				}
			}
		}
	}
}
