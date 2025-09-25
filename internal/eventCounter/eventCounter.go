package eventCounter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var EventsCount = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "logistic_pack_retranslator",
	Name:      "events_count",
	Help:      "Number of events that process in retranslatior right now",
})
