package server

import (
	"fmt"
	"io"

	"github.com/VictoriaMetrics/metrics"
)

func (a *admission) WriteMetrics(w io.Writer) {
	set := metrics.NewSet()
	for _, metric := range []struct {
		name        string
		used, limit int
	}{{"rpcs", len(a.rpcs), a.limits.MaxRPCs}, {"watches", len(a.watches), a.limits.MaxWatches}, {"connections", int(a.connections.Load()), a.limits.MaxConnections}} {
		set.NewGauge(fmt.Sprintf("cnpg_connect_admission_in_use{kind=%q}", metric.name), nil).Set(float64(metric.used))
		set.NewGauge(fmt.Sprintf("cnpg_connect_admission_limit{kind=%q}", metric.name), nil).Set(float64(metric.limit))
	}
	for _, metric := range []struct {
		name  string
		value uint64
	}{{"auth", a.rejectedAuth.Load()}, {"capacity", a.rejectedCapacity.Load()}} {
		set.NewCounter(fmt.Sprintf("cnpg_connect_admission_rejections_total{reason=%q}", metric.name)).Set(metric.value)
	}
	set.NewCounter("cnpg_connect_admission_initial_timeouts_total").Set(a.timedout.Load())
	set.WritePrometheus(w)
}
