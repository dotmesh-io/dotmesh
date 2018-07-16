package commands

import (
	"io"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

var cloneLocalVolume string

func NewCmdClone(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clone <remote> [<dot> [<branch>]] [--local-name=<dot>]",
		Short: `Make a complete copy of a remote dot`,
		// XXX should this specify a branch?
		Long: `Make a complete copy on the current active cluster of the given
<branch> of the given <dot> on the given <remote>. By default, name the
dot the same here as it's named there, but that can be overriden with '--local-name'.

Example: to clone the 'repro_bug_1131' branch from dot 'billing_postgres' on
cluster 'devdata' to your currently active local dotmesh instance which has no
copy of 'app_billing_postgres' at all yet:

    dm clone devdata billing_postgres repro_bug_1131

Online help: https://docs.dotmesh.com/references/cli/#clone-dm-clone-local-name-local-dot-remote-dot-branch
`,
		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				// TODO check that filesystem does _not_ exist on toRemote

				peer, filesystemName, branchName, err := resolveTransferArgs(args)
				if err != nil {
					return err
				}
				transferId, err := dm.RequestTransfer(
					"pull", peer,
					cloneLocalVolume, branchName,
					filesystemName, branchName,
					// TODO also switch to the remote?
				)
				if err != nil {
					return err
				}
				err = dm.PollTransfer(transferId, out)
				if err != nil {
					return err
				}

				return nil
			})
		},
	}

	cmd.PersistentFlags().StringVarP(&cloneLocalVolume, "local-name", "", "",
		"Local dot name to create")

	return cmd
}
