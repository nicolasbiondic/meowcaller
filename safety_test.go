package meowcaller

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func capturePanics(t *testing.T) (func() []PanicInfo, Option) {
	t.Helper()
	var mu sync.Mutex
	var got []PanicInfo
	prev := panicHandler.Load()
	t.Cleanup(func() { panicHandler.Store(prev) })
	return func() []PanicInfo {
			mu.Lock()
			defer mu.Unlock()
			return append([]PanicInfo(nil), got...)
		}, WithPanicHandler(func(p PanicInfo) {
			mu.Lock()
			got = append(got, p)
			mu.Unlock()
		})
}

func testOffer(id string) *events.CallOffer {
	peer := types.NewJID("51999888777", types.DefaultUserServer)
	return &events.CallOffer{
		BasicCallMeta: types.BasicCallMeta{From: peer, CallCreator: peer, CallID: id, Timestamp: time.Now()},
		Data:          &waBinary.Node{Tag: "offer", Attrs: waBinary.Attrs{"call-id": id}},
	}
}

func TestOfferFilterLeavesCallUntouched(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)
	var seen atomic.Int32
	c := NewClient(wa, WithOfferFilter(func(ev *events.CallOffer) bool {
		seen.Add(1)
		return false
	}))
	var incoming atomic.Int32
	c.OnIncomingCall(func(*Call) { incoming.Add(1) })
	if err := c.RawHookErr(); err != nil {
		t.Fatalf("raw hook: %v", err)
	}
	wa.DangerousInternals().DispatchEvent(testOffer("filtered-1"))
	if seen.Load() != 1 {
		t.Fatalf("filter calls = %d, want 1", seen.Load())
	}
	if incoming.Load() != 0 {
		t.Fatal("incoming listener fired for a filtered offer")
	}
	if c.eng.engaged("filtered-1") {
		t.Fatal("engine tracks a filtered offer")
	}
	node := &waBinary.Node{Tag: "call", Content: []waBinary.Node{{Tag: "group_update", Attrs: waBinary.Attrs{"call-id": "filtered-1"}}}}
	if c.eng.onCallRawFiltered(node) {
		t.Fatal("raw hook claimed a stanza of a filtered call")
	}
}

func TestEnginePanicDoesNotStopLaterHandlers(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)
	panics, opt := capturePanics(t)
	NewClient(wa, opt, WithOfferFilter(func(*events.CallOffer) bool { panic("filter bug") }))
	var later atomic.Int32
	wa.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.CallOffer); ok {
			later.Add(1)
		}
	})
	if failed := wa.DangerousInternals().DispatchEvent(testOffer("panic-1")); failed {
		t.Fatal("dispatch reported a failed handler")
	}
	if later.Load() != 1 {
		t.Fatalf("handler registered after the engine ran %d times, want 1", later.Load())
	}
	got := panics()
	if len(got) != 1 || got[0].Where != "offer filter" || len(got[0].Stack) == 0 {
		t.Fatalf("panics = %+v, want one from the offer filter with a stack", got)
	}
}

func TestSafeGoRecoversAndReports(t *testing.T) {
	panics, opt := capturePanics(t)
	resolveConfig([]Option{opt})
	done := make(chan struct{})
	safeGo("test loop", func() {
		defer close(done)
		panic("boom")
	})
	<-done
	deadline := time.Now().Add(2 * time.Second)
	for len(panics()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := panics()
	if len(got) != 1 || got[0].Where != "test loop" || got[0].Value != "boom" {
		t.Fatalf("panics = %+v", got)
	}
	if !safeCall("ok", func() {}) {
		t.Fatal("safeCall reported a panic for a clean call")
	}
}
