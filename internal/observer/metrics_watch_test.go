package observer

import (
	"bytes"
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"strings"
	"testing"
)

func TestMetadataWatchMetrics(t *testing.T) {
	o, _, _ := fakeObserver(t)
	source := watch.NewRaceFreeFake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracked := o.trackWatch(ctx, source, &o.clusterWatch, cancel)
	source.Error(&metav1.Status{Message: "private-error"})
	<-tracked.ResultChan()
	tracked.Stop()
	for range tracked.ResultChan() {
	}
	var b bytes.Buffer
	o.WriteMetrics(&b)
	for _, want := range []string{"cnpg_connect_metadata_watch_starts_total 1\n", "cnpg_connect_metadata_watch_ends_total 1\n", "cnpg_connect_metadata_watch_errors_total 1\n", "cnpg_connect_metadata_watch_up{resource=\"clusters\"} 0\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
	if strings.Contains(b.String(), "private-error") {
		t.Fatal("error content leaked")
	}
}
