package commands

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/spf13/cobra"
)

func NewCmdLog(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log",
		Short: "Show commit logs",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-commits-dm-log",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := client.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				activeVolume, err := dm.StrictCurrentVolume()
				if err != nil {
					return err
				}
				if activeVolume == "" {
					return fmt.Errorf(
						"No current dot. Try 'dm list' and " +
							"'dm switch' to switch to a dot.",
					)
				}

				activeBranch, err := dm.CurrentBranch(activeVolume)
				if err != nil {
					return err
				}

				commits, err := dm.ListCommits(activeVolume, activeBranch)
				if err != nil {
					return err
				}
				for _, commit := range commits {
					fmt.Fprintf(out, "commit %s\n", commit.Id)
					fmt.Fprintf(out, "author: %s\n", commit.Metadata["author"])
					fmt.Fprintf(out, "date: %s\n", commit.Metadata["timestamp"])

					sortedNames := []string{}
					for name, _ := range commit.Metadata {
						sortedNames = append(sortedNames, name)
					}
					sort.Strings(sortedNames)

					for _, name := range sortedNames {
						value := commit.Metadata[name]
						if name != "author" && name != "message" && name != "timestamp" {
							fmt.Fprintf(out, "%s: %s\n", name, value)
						}
					}

					fmt.Fprintf(out, "\n")
					fmt.Fprintf(out, "    %s\n\n", commit.Metadata["message"])
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
