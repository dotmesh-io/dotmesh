package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

var pullRemoteVolume string

func NewCmdPull(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull <remote> <dot> <branch>",
		Short: `Pull new commits from a remote dot to a local copy of that dot`,
		Long: `Pulls commits from a <remote> <dot>'s given <branch> to the
currently active branch of the currently active dot on the currently active
cluster. If <branch> is not specified, try to pull all branches. If <dot> is
not specified, try to pull a dot with the same name on the remote cluster.

Use 'dm clone' to make an initial copy, 'pull' only updates an existing one.

Example: to pull any new commits from the master branch of dot 'postgres' on
cluster 'backups':

    dm pull backups postgres master
`,
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
				if err != nil {
					return err
				}
				// TODO check that filesystem exists on toRemote

				peer, filesystemName, branchName, err := resolveTransferArgs(args)
				if err != nil {
					return err
				}
				transferId, err := dm.RequestTransfer(
					"pull", peer,
					filesystemName, branchName,
					pullRemoteVolume, branchName,
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

	cmd.PersistentFlags().StringVarP(&pullRemoteVolume, "remote-name", "", "",
		"Remote dot name to pull from")

	return cmd
}
