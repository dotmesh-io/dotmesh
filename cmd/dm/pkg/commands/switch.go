package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

func NewCmdSwitch(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch",
		Short: "Change which dot is active",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#select-a-different-current-dot-dm-switch-dot",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				if len(args) > 1 {
					return fmt.Errorf("Too many arguments specified (more than 1).")
				}
				if len(args) == 0 {
					return fmt.Errorf("No dot name specified.")
				}
				volumeName := args[0]
				if !remotes.CheckName(volumeName) {
					return fmt.Errorf("Error: %v is an invalid name", volumeName)
				}
				exists, err := dm.VolumeExists(volumeName)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("Error: %v doesn't exist", volumeName)
				}
				err = dm.SwitchVolume(volumeName)
				if err != nil {
					return fmt.Errorf("Error: %v", err)
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
