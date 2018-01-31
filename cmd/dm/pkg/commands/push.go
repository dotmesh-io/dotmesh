package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

var pushRemoteVolume string

func NewCmdPush(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push <remote>",
		Short: `Push new commits from the current dot and branch to a remote dot (creating it if necessary)`,
		Long: `Pushes new commits to a <remote> from the currently active
branch of the currently active dot on the currently active cluster.
If the remote dot does not exist, it will be created
on-demand.

Example: to make a new backup and push new commits from the master branch of
dot 'postgres' to cluster 'backups':

    dm switch postgres && dm commit -m "friday backup"
    dm push backups
`,
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
				if err != nil {
					return err
				}
				peer, filesystemName, branchName, err := resolveTransferArgs(args)
				if err != nil {
					return err
				}
				transferId, err := dm.RequestTransfer(
					"push", peer, filesystemName, branchName, pushRemoteVolume, "",
				)
				if err != nil {
					return err
				}
				err = dm.PollTransfer(transferId, out)
				if err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&pushRemoteVolume, "remote-name", "", "",
		"Remote dot name to push to, including remote namespace e.g. alice/apples")
	return cmd
}
