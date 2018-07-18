package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

func NewCmdReset(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset [--hard] <ref>",
		Short: "Reset current HEAD to the specified state",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#roll-back-commits-dm-reset-hard-commit",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				if len(args) != 1 {
					return fmt.Errorf("Please specify one ref only.")
				}
				commit := args[0]
				if err := dm.ResetCurrentVolume(commit); err != nil {
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
	cmd.Flags().BoolVarP(
		&resetHard, "hard", "", false,
		"Any changes to tracked files in the current "+
			"dot since <ref> are discarded.",
	)
	return cmd
}
