package commands

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
	"text/tabwriter"
)

func NewCmdList(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "Enumerate dots on the current remote",
		Long:    "Online help: https://docs.dotmesh.com/references/cli/#list-the-available-dots-dm-list-h-scripting",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				if len(args) > 0 {
					return fmt.Errorf("Please specify no arguments.")
				}

				if !scriptingMode {
					fmt.Fprintf(
						out,
						"Current remote: %s (use 'dm remote -v' to list and 'dm remote switch' to switch)\n\n",
						dm.Configuration.CurrentRemote,
					)
				}

				columnNames := []string{"  DOT", "BRANCH", "SERVER", "CONTAINERS", "SIZE", "COMMITS", "DIRTY"}

				var target io.Writer
				if scriptingMode {
					target = out
				} else {
					target = tabwriter.NewWriter(out, 3, 8, 2, ' ', 0)
					fmt.Fprintf(
						target,
						strings.Join(columnNames, "\t")+"\n",
					)
				}

				vcs, err := dm.AllVolumesWithContainers()
				if err != nil {
					return err
				}

				for _, vc := range vcs {
					v := vc.Volume
					containerInfo := vc.Containers
					activeQualified, err := dm.CurrentVolume()
					if err != nil {
						return err
					}
					activeNamespace, activeVolume, err := remotes.ParseNamespacedVolume(activeQualified)
					if err != nil {
						return err
					}
					active := remotes.VolumeName{activeNamespace, activeVolume}

					start := "  "
					if active == v.Name {
						start = "* "
					}

					// disabled prefixes in scripting mode
					if scriptingMode {
						start = ""
					}

					// TODO maybe show all branches
					b, err := dm.CurrentBranch(v.Name.String())
					if err != nil {
						return err
					}
					containerNames := []string{}
					for _, container := range containerInfo {
						containerNames = append(containerNames, container.Name)
					}

					var dirtyString, sizeString string
					if scriptingMode {
						dirtyString = fmt.Sprintf("%d", v.DirtyBytes)
						sizeString = fmt.Sprintf("%d", v.SizeBytes)
					} else {
						dirtyString = prettyPrintSize(v.DirtyBytes)
						sizeString = prettyPrintSize(v.SizeBytes)
					}

					cells := []string{
						v.Name.String(), b, v.Master, strings.Join(containerNames, ","),
						sizeString, fmt.Sprintf("%d", v.CommitCount), dirtyString,
					}
					fmt.Fprintf(target, start)
					for _, cell := range cells {
						fmt.Fprintf(target, cell+"\t")
					}
					fmt.Fprintf(target, "\n")
				}
				// ehhhh
				w, ok := target.(*tabwriter.Writer)
				if ok {
					w.Flush()
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
		&scriptingMode, "scripting", "H", false,
		"scripting mode. Do not print headers, separate fields by "+
			"a single tab instead of arbitrary whitespace.",
	)
	return cmd
}
