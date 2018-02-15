package commands

import (
	"fmt"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
	"io"
	"os"
)

var clientVersion string
var dockerTag string

func SetVersion(v, t string) {
	clientVersion = v
	dockerTag = t
}

func NewCmdVersion(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the Dotmesh version information",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#comparing-client-and-remote-versions-dm-version",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
				if !scriptingMode {
					fmt.Fprintf(
						out,
						"Current remote: %s (use 'dm remote -v' to list and 'dm remote switch' to switch)\n\n",
						dm.Configuration.CurrentRemote,
					)
				}
				fmt.Fprintf(out, "Client:\n\tVersion: %s\n", clientVersion)
				if err != nil {
					return err
				}
				serverVersion, err := dm.GetVersion()
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "Server:\n\tVersion: %s\n", string(serverVersion.InstalledVersion))
				if serverVersion.Outdated {
					fmt.Fprintf(out, "\tUpgrade available\n\tLatest server version %s available\n", serverVersion.CurrentVersion)
					fmt.Fprintf(out, "\tLearn how to upgrade at %s\n", "https://docs.dotmesh.com/install-setup/upgrading/")
				}
				return err
			}()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	return cmd
}
