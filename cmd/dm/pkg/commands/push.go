package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/spf13/cobra"
)

var pushRemoteVolume string

func NewCmdPush(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push <remote> [<dot> [<branch>]] [--remote-name=<dot>]",
		Short: `Push new commits from the specified dot and branch to a remote dot (creating it if necessary)`,
		Long: `Pushes new commits to a <remote> from the branch <branch> of <dot>.
If <branch> is not specified, try to pull all branches. If <dot> is
not specified, try to pull a dot with the same name on the remote cluster.

'push' will attempt to use the remote dot the local <dot> was originally
cloned from on the specified <remote>, but any remote dot can be named with
'--remote-name'.

If the remote dot does not exist, it will be created on-demand.

Example: to make a new backup and push new commits from the master branch of
dot 'postgres' to cluster 'backups':

    dm switch postgres && dm commit -m "friday backup"
    dm push backups

Online help: https://docs.dotmesh.com/references/cli/#push-dm-push-remote-remote-name-dot
`,
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := client.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				peer, filesystemName, branchName, err := resolveTransferArgs(args)
				if err != nil {
					return err
				}
				transferId, err := dm.RequestTransfer(
					"push", peer, filesystemName, branchName, pushRemoteVolume, "", nil, stash,
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
	cmd.PersistentFlags().BoolVarP(&stash, "stash-on-divergence", "", false, "stash any divergence on a branch and continue")
	return cmd
}
