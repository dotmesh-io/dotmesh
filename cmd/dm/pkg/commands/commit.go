package commands

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

func NewCmdCommit(out io.Writer) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "commit",
		Short: "Record changes to a dot",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#commit-dm-commit-m-message",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				if commitMsg == "" {
					return fmt.Errorf("Please provide a commit message")
				}

				var metadataPairs = make(map[string]string)

				for _, metadataString := range *commitMetadata {
					if strings.Index(metadataString, "=") == -1 {
						return fmt.Errorf("Each metadata value must be a name=value pair: %s", metadataString)
					}
					metadataStringParts := strings.Split(metadataString, "=")
					metadataPairs[metadataStringParts[0]] = metadataStringParts[1]
				}

				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
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

				id, err := dm.Commit(v, b, commitMsg, metadataPairs)
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", id)
				return nil
			}()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&commitMsg, "message", "m", "",
		"Use the given string as the commit message.")

	commitMetadata = cmd.Flags().StringSliceP("metadata", "d", []string{},
		"Add custom metadata to the commit (e.g. --metadata name=value).")

	return cmd
}
