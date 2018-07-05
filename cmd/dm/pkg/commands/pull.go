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
		Use:   "pull <remote> [<dot> [<branch>]] [--remote-name=<dot>]",
		Short: `Pull new commits from a remote dot to a local copy of that dot`,
		Long: `Pulls commits from a remote dot to <dot>'s given <branch>.
If <branch> is not specified, try to pull all branches. If <dot> is
not specified, try to pull a dot with the same name on the remote cluster.

'pull' will attempt to use the remote dot the local <dot> was originally
cloned from on the specified <remote>, but any remote dot can be named with
'--remote-name'.

Use 'dm clone' to make an initial copy, 'pull' only updates an existing one.

Example: to pull any new commits from the master branch of dot 'postgres' on
cluster 'backups':

    dm pull backups postgres master

Online help: https://docs.dotmesh.com/references/cli/#pull-dm-pull-remote-dot-branch-remote-name-remote-dot
`,
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
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
