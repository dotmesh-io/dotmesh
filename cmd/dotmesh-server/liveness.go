package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"golang.org/x/net/context"

	"github.com/dotmesh-io/dotmesh/pkg/client"
)

func (state *InMemoryState) runLivenessServer() {
	router := mux.NewRouter()

	/*
		Check that the system is in a healthy state.

		This does NOT mean it's all up to date with etcd - because if the underlying system is healthy, we should EVENTUALLY
		catch up with etcd.

		Here, we need to check for the basic availability of things we require from the platform, such as etcd and zfs.

		This is run as a dedicated service on its own port, because the
		main listener is only started once we've caught up with the latest
		in etcd, and that can take a while. For Kubernetes liveness checks,
		we need a separate listener that's started before anything
		non-constant-time happens, or the daemon gets killed as it's trying
		to start up with lots of dots/commits to process :'-(
	*/

	router.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {
		// Check we can connect to etcd

		_, err := state.etcdClient.Get(
			context.Background(),
			ETCD_PREFIX,
			nil,
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading from etcd: %v\n", err), http.StatusInternalServerError)
			return
		}

		// Check the zpool exists
		_, err = state.zfs.GetZPoolCapacity()
		if err != nil {
			http.Error(w, fmt.Sprintf("Zpool error: %v\n", err), http.StatusInternalServerError)
			return
		}

		// Nothing failed, so let's return OK with the default 200 status code.
		fmt.Fprintf(w, "OK")
	},
	)

	err := http.ListenAndServe(fmt.Sprintf(":%s", client.LIVENESS_PORT), router)
	if err != nil {
		log.Fatalf("Unable to listen for liveness probes: %+v", err)
	}
}
