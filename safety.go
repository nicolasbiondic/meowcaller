package meowcaller

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync/atomic"

	"go.mau.fi/whatsmeow/types/events"
)

// Embedding the engine in a process that also serves messages (a sync daemon on
// the same whatsmeow client) means a bug in the call stack must never take the
// session down. Every goroutine the engine starts and every hook it installs on
// whatsmeow runs under safeGo/safeCall: a panic is recovered, reported to the
// panic handler and the rest of the process keeps running.

// PanicInfo describes a recovered panic inside the call engine.
type PanicInfo struct {
	// Where names the goroutine or hook that panicked.
	Where string
	// Value is the recovered value.
	Value any
	// Stack is the goroutine stack at the panic.
	Stack []byte
}

var panicHandler atomic.Pointer[func(PanicInfo)]

// WithPanicHandler sets the function called after the engine recovers a panic
// in one of its goroutines or whatsmeow hooks. It is process-wide (the engine's
// helpers are shared by every Client); the last one set wins. Without it the
// panic is written to stderr.
func WithPanicHandler(fn func(PanicInfo)) Option {
	return func(c *config) {
		if fn != nil {
			panicHandler.Store(&fn)
		}
	}
}

// WithOfferFilter decides, before anything goes on the wire, whether an inbound
// 1:1 or group offer is engaged at all. When fn returns false the engine does
// not preaccept, does not reach the relays, does not call the incoming-call
// listener and leaves every later stanza of that call to whatsmeow, exactly as
// if the engine were not installed. Use it to keep a shared session untouched
// for callers the integrator will never answer.
func WithOfferFilter(fn func(*events.CallOffer) bool) Option {
	return func(c *config) { c.offerFilter = fn }
}

func reportPanic(where string, v any) {
	info := PanicInfo{Where: where, Value: v, Stack: debug.Stack()}
	if fn := panicHandler.Load(); fn != nil && *fn != nil {
		func() {
			defer func() { _ = recover() }()
			(*fn)(info)
		}()
		return
	}
	fmt.Fprintf(os.Stderr, "meowcaller: recovered panic in %s: %v\n%s", where, v, info.Stack)
}

// safeGo starts fn on its own goroutine with panic recovery.
func safeGo(where string, fn func()) {
	go safeCall(where, fn)
}

// safeCall runs fn and recovers a panic. It reports whether fn returned
// normally.
func safeCall(where string, fn func()) (ok bool) {
	defer func() {
		if v := recover(); v != nil {
			ok = false
			reportPanic(where, v)
		}
	}()
	fn()
	return true
}
