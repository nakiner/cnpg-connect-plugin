package observer

import (
	"fmt"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

// Defer delivery before deferring stateMu.Unlock: Go runs the unlock first.
// Metadata events and observations commit while ordered by stateMu; subscriber
// fanout and log output run after it is released. Flush every notification before
// logging so a blocked log cannot hold back other clusters in a shared-CA update.
type pendingDeliveries struct {
	notifications []func()
	logs          []func()
}

func (pending *pendingDeliveries) add(deliver, log func()) {
	pending.notifications = append(pending.notifications, deliver)
	if log != nil {
		pending.logs = append(pending.logs, log)
	}
}

func (pending *pendingDeliveries) deliver() {
	for _, deliver := range pending.notifications {
		deliver()
	}
	for _, log := range pending.logs {
		log()
	}
}

// commitPublication requires stateMu. Delivery and optional logging callbacks
// must run outside it, with all deliveries in a batch preceding its logs.
func (o *Observer) commitPublication(snapshot v1.Snapshot, started time.Time) (deliver func(), log func()) {
	changed, deliver := o.store.PutDeferred(snapshot)
	// Avoid flooding logs with dormant inventory placeholders.
	if !changed || snapshot.Reason == "awaiting_observation" {
		return deliver, nil
	}
	elapsed := time.Duration(0)
	if !started.IsZero() {
		elapsed = time.Since(started)
	}
	return deliver, func() {
		members := make([]string, 0, len(snapshot.Members))
		primary := ""
		for _, m := range snapshot.Members {
			members = append(members, fmt.Sprintf("%s:%s:%s:%t", m.Name, m.Role, m.SyncState, m.Ready))
			if m.ID == snapshot.PrimaryID {
				primary = m.Name
			}
		}
		o.log.Info("topology changed",
			"namespace", snapshot.Cluster.Namespace,
			"cluster", snapshot.Cluster.Name,
			"primary", primary,
			"available", snapshot.Available,
			"reason", snapshot.Reason,
			"members", members,
			"observation_duration", elapsed,
		)
	}
}
