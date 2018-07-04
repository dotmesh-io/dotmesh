package commands

import (
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

var createDot bool

func NewCmdMount(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount <dot> <mountpoint>",
		Short: "Mount a dots to the local filesystem.",
		Long: `Mounts a dot's filesystem to a given location on your host.

Run 'dm mount <dot> <mountpoint>' to expose the dot's filesystem at <mountpoint> on your host.

NOTE: this currently only works for Linux.`,

		Run: func(cmd *cobra.Command, args []string) {
			err := mountDot(cmd, args, out)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}

	cmd.PersistentFlags().BoolVarP(&createDot, "create", "r", false,
		"Create the dot if it does not exist.")

	return cmd
}

func mountDot(cmd *cobra.Command, args []string, out io.Writer) error {
	dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
	if err != nil {
		return err
	}

	var localDot, mountpoint string
	if len(args) == 2 {
		localDot = args[0]
		mountpoint = args[1]
	} else {
		return fmt.Errorf("Please specify <dot> <mountpoint> as arguments.")
	}

	namespace, dot, err := remotes.ParseNamespacedVolume(localDot)
	if err != nil {
		return err
	}

	exists, err := dm.VolumeExists(localDot)
	if err != nil {
		return err
	}

	if !exists && !createDot {
		return fmt.Errorf(`Dot %s does not exist.

If you want to create it - use the '--create' flag`, localDot)
	}

	localDotPath, err := dm.ProcureVolume(localDot)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "procured dot: %s/%s into %s\n", namespace, dot, localDotPath)

	err = os.Symlink(localDotPath, mountpoint)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "symlinked dot: %s/%s from %s to %s\n", namespace, dot, localDotPath, mountpoint)

	return nil
}
