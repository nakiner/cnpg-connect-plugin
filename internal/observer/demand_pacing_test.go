package observer

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestReconnectDemandIsPacedButMetadataIsUrgent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o, _, _ := fakeObserver(t)
		_, cancel := o.store.Subscribe("test", "db")
		key, shutdown := o.queue.Get()
		if shutdown {
			t.Fatal("queue shut down")
		}
		o.queue.Done(key)
		cancel()
		// Simulate first collection finishing, then interleaved reconnects.
		var stop func()
		for range 1000 {
			_, stop = o.store.Subscribe("test", "db")
			stop()
		}
		_, stop = o.store.Subscribe("test", "db")
		defer stop()
		synctest.Wait()
		if o.queue.Len() != 0 {
			t.Fatal("reconnect bypassed refresh pacing")
		}
		o.Notify("test", "db")
		if o.queue.Len() != 1 {
			t.Fatal("pacing delayed an urgent metadata event")
		}
		key, _ = o.queue.Get()
		o.queue.Done(key)
		time.Sleep(o.opts.PollInterval)
		synctest.Wait()
		if o.queue.Len() != 1 {
			t.Fatal("paced demand was dropped instead of scheduled")
		}
	})
}
