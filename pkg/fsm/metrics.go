package fsm

import "github.com/prometheus/client_golang/prometheus"

var (
	// requestCounter *prometheus.CounterVec = prometheus.NewCounterVec(
	// 	prometheus.CounterOpts{
	// 		Name: "dm_req_total",
	// 		Help: "How many requests processed, partitioned by status code and method.",
	// 	},
	// 	[]string{"url", "http_method", "status_code"},
	// )

	transitionCounter *prometheus.CounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dm_state_transition_total",
			Help: "How many state transitions take place partitioned by previous state (from), current state (to) and status",
		},
		[]string{"from", "to", "status"},
	)

	requestDuration *prometheus.SummaryVec = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "dm_req_duration_seconds",
		Help: "Response time by method/http status code.",
	}, []string{"url", "http_method", "status_code"})

	rpcRequestDuration *prometheus.SummaryVec = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "dm_rpc_req_duration_seconds",
		Help: "Response time by rpc method/http status code.",
	}, []string{"url", "rpc_method", "status_code"})

	zpoolCapacity *prometheus.GaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dm_zpool_usage_percentage",
		Help: "Percentage of zpool capacity used.",
	}, []string{"node_name", "pool_name"})
)
