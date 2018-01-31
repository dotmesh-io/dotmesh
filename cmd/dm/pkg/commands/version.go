package commands

import (
	"fmt"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
	"io"
	"os"
)

var clientVersion string

func SetVersion(v string) {
	clientVersion = v
}

func NewCmdVersion(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the Dotmesh version information",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				fmt.Printf("Client:\n\tVersion: %s\n", clientVersion)
				dm, err := remotes.NewDotmeshAPI(configPath)
				if err != nil {
					return err
				}
				serverVersion, err := dm.GetVersion()
				if err != nil {
					return err
				}
				fmt.Printf("Server:\n\tVersion: %s\n", string(serverVersion.InstalledVersion))
				if serverVersion.Outdated {
					fmt.Printf("\tUpgrade available\n\tLatest server version %s available\n", serverVersion.CurrentVersion)
					fmt.Printf("\tLearn how to upgrade at %s\n", "https://docs.dotmesh.com/install-setup/upgrading/")
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
