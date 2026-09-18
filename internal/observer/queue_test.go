package observer

import (
	"fmt"
	"testing"
	"testing/synctest"
)

func TestPriorityQueueCoalescesThousandsOfEventsAndPreservesFairness(t *testing.T) {
	q, priority := newObservationQueue()
	defer q.ShutDown()
	for i := range 10000 {
		q.Add(clusterKey{"test", fmt.Sprint(i)})
	}
	changed := clusterKey{"test", "9999"}
	for range 10000 {
		priority.promote(changed)
		q.Add(changed)
	}
	if q.Len() != 10000 {
		t.Fatalf("events multiplied queue: %d", q.Len())
	}
	key, _ := q.Get()
	if key != changed {
		t.Fatalf("event waited behind routine checks: %v", key)
	}
	// Events during collection request exactly one subsequent observation.
	for range 10000 {
		priority.promote(changed)
		q.Add(changed)
	}
	q.Done(key)
	key, _ = q.Get()
	if key != changed {
		t.Fatal("lost dirty event during observation")
	}
	q.Done(key)
	for i := range 20 {
		k := clusterKey{"event", fmt.Sprint(i)}
		priority.promote(k)
		q.Add(k)
	}
	backgroundSeen := false
	for range 9 {
		k, _ := q.Get()
		if k.namespace == "test" {
			backgroundSeen = true
		}
		q.Done(k)
	}
	if !backgroundSeen {
		t.Fatal("continuous events starve background checks")
	}
	for q.Len() > 0 {
		k, _ := q.Get()
		q.Done(k)
	}
	if len(priority.entries) != 0 || len(priority.promoted) != 0 {
		t.Fatal("priority state leaked after drain")
	}
}

func TestPriorityQueueUrgentWorkerLeavesBackgroundForRegularWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, priority := newObservationQueue()
		defer q.ShutDown()
		background := clusterKey{"test", "background"}
		q.Add(background)
		got := make(chan clusterKey, 1)
		go func() {
			key, shutdown := priority.GetUrgent()
			if !shutdown {
				got <- key
			}
		}()
		synctest.Wait()
		select {
		case key := <-got:
			t.Fatalf("urgent worker consumed background work: %v", key)
		default:
		}
		if q.Len() != 1 {
			t.Fatalf("background work was removed: queue length %d", q.Len())
		}

		// A promotion must wake an urgent-only waiter even when Add coalesces.
		priority.promote(background)
		q.Add(background)
		synctest.Wait()
		select {
		case key := <-got:
			if key != background || !priority.IsUrgent(key) {
				t.Fatalf("wrong promoted observation: key=%v urgent=%v", key, priority.IsUrgent(key))
			}
			q.Done(key)
		default:
			t.Fatal("promotion did not wake urgent worker")
		}
	})
}

func TestPriorityQueueWorkerClassesShareCoalescing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, priority := newObservationQueue()
		defer q.ShutDown()
		key := clusterKey{"test", "shared"}
		priority.promote(key)
		q.Add(key)
		active, shutdown := priority.GetUrgent()
		if shutdown || active != key {
			t.Fatalf("initial urgent observation: %v shutdown=%v", active, shutdown)
		}
		for range 10000 {
			priority.promote(key)
			q.Add(key)
		}

		type result struct {
			key      clusterKey
			shutdown bool
		}
		got := make(chan result, 2)
		go func() {
			key, shutdown := q.Get()
			got <- result{key, shutdown}
		}()
		go func() {
			key, shutdown := priority.GetUrgent()
			got <- result{key, shutdown}
		}()
		synctest.Wait()
		if len(got) != 0 || q.Len() != 0 {
			t.Fatal("active key was dispatched concurrently")
		}
		q.Done(active)
		synctest.Wait()
		if len(got) != 1 {
			t.Fatalf("follow-up dispatched %d times, want once", len(got))
		}
		followup := <-got
		if followup.shutdown || followup.key != key || !priority.IsUrgent(key) {
			t.Fatalf("lost urgent follow-up: %+v", followup)
		}
		q.Done(key)
		synctest.Wait()
		if len(got) != 0 || q.Len() != 0 {
			t.Fatal("coalesced events created additional follow-ups")
		}
		q.ShutDown()
		synctest.Wait()
		if final := <-got; !final.shutdown {
			t.Fatalf("remaining waiter received unexpected work: %+v", final)
		}
	})
}

func TestPriorityQueueActiveUrgencyDoesNotChangeOnPromotion(t *testing.T) {
	q, priority := newObservationQueue()
	defer q.ShutDown()
	key := clusterKey{"test", "changing"}
	q.Add(key)
	active, _ := q.Get()
	if priority.IsUrgent(active) {
		t.Fatal("background observation classified urgent")
	}
	priority.promote(key)
	q.Add(key)
	if priority.IsUrgent(active) {
		t.Fatal("promotion changed urgency of the active observation")
	}
	q.Done(active)
	followup, _ := q.Get()
	if followup != key || !priority.IsUrgent(followup) {
		t.Fatal("regular worker did not retain follow-up urgency")
	}
	q.Done(followup)
	if priority.IsUrgent(key) {
		t.Fatal("completed observation retained active classification")
	}
}

func TestPriorityQueueShutdownWakesBothWorkerClasses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, priority := newObservationQueue()
		defer q.ShutDown()
		stopped := make(chan bool, 2)
		go func() {
			_, shutdown := q.Get()
			stopped <- shutdown
		}()
		go func() {
			_, shutdown := priority.GetUrgent()
			stopped <- shutdown
		}()
		synctest.Wait()
		q.ShutDown()
		synctest.Wait()
		if len(stopped) != 2 || !<-stopped || !<-stopped {
			t.Fatal("shutdown did not unblock both worker classes")
		}
		key := clusterKey{"test", "after-shutdown"}
		priority.promote(key)
		q.Add(key)
		if !q.ShuttingDown() || q.Len() != 0 || len(priority.promoted) != 0 {
			t.Fatal("shutdown accepted new work")
		}
	})
}

func TestPriorityQueueUrgentShutdownDoesNotConsumeBackground(t *testing.T) {
	q, priority := newObservationQueue()
	defer q.ShutDown()
	key := clusterKey{"test", "background"}
	q.Add(key)
	q.ShutDown()
	if got, shutdown := priority.GetUrgent(); !shutdown {
		t.Fatalf("urgent shutdown consumed background item %v", got)
	}
	if got, shutdown := q.Get(); shutdown || got != key {
		t.Fatalf("accepted background item lost during shutdown: %v shutdown=%v", got, shutdown)
	}
	q.Done(key)
	if _, shutdown := q.Get(); !shutdown {
		t.Fatal("empty stopped queue did not terminate")
	}
}

func TestPriorityQueueDrainIncludesQueuedAndDirtyWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, priority := newObservationQueue()
		defer q.ShutDown()
		key := clusterKey{"test", "active"}
		q.Add(key)
		_, _ = q.Get()
		priority.promote(key)
		q.Add(key)
		q.Add(clusterKey{"test", "background"})
		drained := make(chan struct{})
		go func() {
			q.ShutDownWithDrain()
			close(drained)
		}()
		synctest.Wait()
		if !q.ShuttingDown() {
			t.Fatal("drain did not stop admission")
		}
		select {
		case <-drained:
			t.Fatal("drain returned with active work")
		default:
		}
		q.Done(key)
		synctest.Wait()
		select {
		case <-drained:
			t.Fatal("drain returned with queued follow-up work")
		default:
		}
		for range 2 {
			key, shutdown := q.Get()
			if shutdown {
				t.Fatal("queued work lost during drain")
			}
			q.Done(key)
		}
		synctest.Wait()
		select {
		case <-drained:
		default:
			t.Fatal("completed work did not release drain")
		}
	})
}

func TestPriorityQueueShutdownInterruptsDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := newObservationQueue()
		defer q.ShutDown()
		key := clusterKey{"test", "active"}
		q.Add(key)
		_, _ = q.Get()
		drained := make(chan struct{})
		go func() {
			q.ShutDownWithDrain()
			close(drained)
		}()
		synctest.Wait()
		q.ShutDown()
		synctest.Wait()
		select {
		case <-drained:
		default:
			t.Fatal("shutdown did not interrupt drain")
		}
		q.Done(key)
	})
}

func TestPriorityQueueBackgroundWakesRegularWorkerPastUrgentWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, priority := newObservationQueue()
		defer q.ShutDown()
		urgentStopped := make(chan bool, 1)
		go func() {
			_, shutdown := priority.GetUrgent()
			urgentStopped <- shutdown
		}()
		synctest.Wait()
		regular := make(chan clusterKey, 1)
		go func() {
			key, shutdown := q.Get()
			if !shutdown {
				regular <- key
			}
		}()
		synctest.Wait()
		background := clusterKey{"test", "background"}
		q.Add(background)
		synctest.Wait()
		select {
		case key := <-regular:
			if key != background || priority.IsUrgent(key) {
				t.Fatalf("wrong background observation: %v", key)
			}
			q.Done(key)
		default:
			t.Fatal("background Add left regular worker asleep")
		}
		if len(urgentStopped) != 0 {
			t.Fatal("urgent worker consumed background observation")
		}
		q.ShutDown()
		synctest.Wait()
		if !<-urgentStopped {
			t.Fatal("urgent worker did not stop at shutdown")
		}
	})
}
