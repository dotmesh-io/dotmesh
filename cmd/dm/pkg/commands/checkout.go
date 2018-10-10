package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdCheckout(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checkout",
		Short: "Switch or make branches",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#switch-branches-dm-checkout-branch",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				if len(args) != 1 {
					return fmt.Errorf("Please give me a branch name.")
				}
				branch := args[0]
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
				if err := dm.CheckoutBranch(v, b, branch, makeBranch); err != nil {
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
	cmd.Flags().BoolVarP(&makeBranch, "branch", "b", false, "Make branch")
	return cmd
}
