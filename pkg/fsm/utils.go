package fsm

import (
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/observer"

	log "github.com/sirupsen/logrus"
)

var deathObserver = observer.NewObserver("deathObserver")

func runWhileFilesystemLives(f func() error, label string, filesystemId string, errorBackoff, successBackoff time.Duration) {
	deathChan := make(chan interface{})
	deathObserver.Subscribe(filesystemId, deathChan)
	stillAlive := true
	for stillAlive {
		select {
		case _ = <-deathChan:
			stillAlive = false
		default:
			err := f()
			if err != nil {
				log.Printf(
					"Error in runWhileFilesystemLives(%s@%s), retrying in %s: %s",
					label, filesystemId, errorBackoff, err)
				time.Sleep(errorBackoff)
			} else {
				time.Sleep(successBackoff)
			}
		}
	}
	deathObserver.Unsubscribe(filesystemId, deathChan)
}

func terminateRunnersWhileFilesystemLived(filesystemId string) {
	deathObserver.Publish(filesystemId, struct{ reason string }{"runWhileFilesystemLives"})
}


func fq(fs string) string {
	// from filesystem id to a fully qualified ZFS filesystem
	return fmt.Sprintf("%s/%s/%s", POOL, ROOT_FS, fs)
}
func unfq(fqfs string) string {
	// from fully qualified ZFS name to filesystem id, strip off prefix
	return fqfs[len(POOL+"/"+ROOT_FS+"/"):]
}