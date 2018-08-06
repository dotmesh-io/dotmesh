package commands

import (
	"fmt"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/howeyc/gopass"
	"github.com/spf13/cobra"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

func NewCmdRemote(out io.Writer) *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
		Use:   "remote [-v]",
		Short: "List remote clusters. Use dm remote -v to see remotes",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-remotes-dm-remote-v",
		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				if len(args) > 0 {
					return fmt.Errorf("Too many arguments specified.")
				}

				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				remotes := dm.Configuration.GetRemotes()
				s3Remotes := dm.Configuration.GetS3Remotes()
				keys := []string{}
				// sort the keys so we can iterate over in human friendly order
				for k, _ := range remotes {
					keys = append(keys, k)
				}
				for k, _ := range s3Remotes {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				if verbose {
					currentRemote := dm.Configuration.GetCurrentRemote()
					for _, k := range keys {
						var current string
						if k == currentRemote {
							current = "* "
						} else {
							current = "  "
						}
						remote, ok := remotes[k]
						if ok {
							fmt.Fprintf(
								out, "%s%s\t%s@%s\n",
								current, k, remote.User, remote.Hostname,
							)
						} else {
							fmt.Fprintf(
								out, "%s\t%s\n",
								k, s3Remotes[k].KeyID,
							)
						}
					}
				} else {
					for _, k := range keys {
						fmt.Fprintln(out, k)
					}
				}
				return nil
			})
		},
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "add <remote-name> <user@cluster-hostname>[:<port-number>]",
		Short: "Add a remote",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#add-a-new-remote-dm-remote-add-name-user-hostname",

		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				if len(args) != 2 {
					return fmt.Errorf(
						"Please specify <remote-name> <user@cluster-hostname>",
					)
				}
				remote := args[0]
				shrapnel := strings.SplitN(args[1], "@", 2)
				if len(shrapnel) != 2 {
					return fmt.Errorf(
						"Please specify user@cluster-hostname, got %s", shrapnel,
					)
				}
				user := shrapnel[0]
				hostname := shrapnel[1]
				port := 0
				var err error
				if strings.Contains(hostname, ":") {
					shrapnel = strings.SplitN(hostname, ":", 2)
					if len(shrapnel) != 2 {
						return fmt.Errorf(
							"Please specify user@cluster-hostname:port, got %s", shrapnel,
						)
					}
					hostname = shrapnel[0]
					port, err = strconv.Atoi(shrapnel[1])
					if err != nil {
						return fmt.Errorf(
							"Please specify a port as an integer, e.g user@cluster-hostname:port, got %s", shrapnel[1],
						)
					}
				}
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				// allow this to be used be a script
				apiKey := os.Getenv("DOTMESH_PASSWORD")
				if apiKey == "" {
					fmt.Printf("API key: ")
					enteredApiKey, err := gopass.GetPasswd()
					fmt.Printf("\n")
					if err != nil {
						return err
					}
					apiKey = string(enteredApiKey)
				}
				client := &remotes.JsonRpcClient{
					User:     user,
					Hostname: hostname,
					Port:     port,
					ApiKey:   apiKey,
				}
				_, err = remotes.Ping(client)

				if err != nil {
					return err
				}

				err = dm.Configuration.AddRemote(remote, user, hostname, port, string(apiKey))
				if err != nil {
					return err
				}
				fmt.Fprintln(out, "Remote added.")
				currentRemote := dm.Configuration.GetCurrentRemote()
				if currentRemote == "" {
					err = dm.Configuration.SetCurrentRemote(remote)
					if err != nil {
						return err
					}
					fmt.Fprintln(out, "Automatically switched to first remote.")
				}
				return nil
			})
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "rm <remote>",
		Short: "Remove a remote",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#remove-a-remote-dm-remote-rm-name",

		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				if len(args) != 1 {
					return fmt.Errorf(
						"Please specify <remote-name>",
					)
				}
				return dm.Configuration.RemoveRemote(args[0])
			})
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "switch <remote>",
		Short: "Switch to a remote",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#select-the-current-remote-dm-remote-switch-name",
		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				if len(args) != 1 {
					return fmt.Errorf(
						"Please specify <remote-name>",
					)
				}
				return dm.Configuration.SetCurrentRemote(args[0])
			})
		},
	})
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose list of remotes")
	return cmd
}
