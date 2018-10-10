package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdBranch(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "List branches",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-the-branches-dm-branch",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := client.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				v, err := dm.StrictCurrentVolume()
				if err != nil {
					return err
				}
				b, err := dm.CurrentBranch(v)
				if err != nil {
					return err
				}
				bs, err := dm.AllBranches(v)
				if err != nil {
					return err
				}
				for _, branch := range bs {
					if branch == b {
						branch = "* " + branch
					} else {
						branch = "  " + branch
					}
					fmt.Fprintf(out, "%s\n", branch)
				}
				return nil
			}()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	return cmd
}
