package commands

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/howeyc/gopass"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

var configPath string
var makeBranch bool
var forceMode bool
var scriptingMode bool
var commitMsg string
var commitMetadata *[]string
var resetHard bool

var MainCmd = &cobra.Command{
	Use:   "dm",
	Short: "dotmesh (dm) is like git for your data in Docker",
	Long: `dotmesh (dm) is like git for your data in Docker.

This is the client. Configure it to talk to a dotmesh cluster with 'dm remote
add'. Create a dotmesh cluster with 'dm cluster init'.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		configPathInner, err := homedir.Expand(configPath)
		configPath = configPathInner
		if err != nil {
			return err
		}
		dirPath := filepath.Dir(configPath)
		if _, err := os.Stat(dirPath); err != nil {
			if err := os.MkdirAll(dirPath, 0700); err != nil {
				return fmt.Errorf(
					"Could not create config directory %s: %v", configPath, err,
				)
			}
		}
		return nil
	},
}

func Initialise() {

	MainCmd.AddCommand(NewCmdCluster(os.Stdout))
	MainCmd.AddCommand(NewCmdRemote(os.Stdout))
	MainCmd.AddCommand(NewCmdList(os.Stdout))
	MainCmd.AddCommand(NewCmdInit(os.Stdout))
	MainCmd.AddCommand(NewCmdSwitch(os.Stdout))
	MainCmd.AddCommand(NewCmdCommit(os.Stdout))
	MainCmd.AddCommand(NewCmdLog(os.Stdout))
	MainCmd.AddCommand(NewCmdBranch(os.Stdout))
	MainCmd.AddCommand(NewCmdCheckout(os.Stdout))
	MainCmd.AddCommand(NewCmdReset(os.Stdout))
	MainCmd.AddCommand(NewCmdClone(os.Stdout))
	MainCmd.AddCommand(NewCmdPull(os.Stdout))
	MainCmd.AddCommand(NewCmdPush(os.Stdout))
	MainCmd.AddCommand(NewCmdDebug(os.Stdout))
	MainCmd.AddCommand(NewCmdDot(os.Stdout))
	MainCmd.AddCommand(NewCmdVersion(os.Stdout))
	MainCmd.AddCommand(NewCmdMount(os.Stdout))

	MainCmd.PersistentFlags().StringVarP(
		&configPath, "config", "c",
		"~/.dotmesh/config",
		"Config file to use",
	)
}

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

				dm, err := remotes.NewDotmeshAPI(configPath)
				if err != nil {
					return err
				}
				remotes := dm.Configuration.GetRemotes()
				keys := []string{}
				// sort the keys so we can iterate over in human friendly order
				for k, _ := range remotes {
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
						fmt.Fprintf(
							out, "%s%s\t%s@%s\n",
							current, k, remotes[k].User, remotes[k].Hostname,
						)
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
		Use:   "add <remote-name> <user@cluster-hostname>",
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
				dm, err := remotes.NewDotmeshAPI(configPath)
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

				_, err = dm.Ping()
				if err != nil {
					return err
				}

				err = dm.Configuration.AddRemote(remote, user, hostname, string(apiKey))
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
				dm, err := remotes.NewDotmeshAPI(configPath)
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
				dm, err := remotes.NewDotmeshAPI(configPath)
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
				dm, err := remotes.NewDotmeshAPI(configPath)
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

func NewCmdList(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "Enumerate dots on the current remote",
		Long:    "Online help: https://docs.dotmesh.com/references/cli/#list-the-available-dots-dm-list-h-scripting",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
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

				vs, err := dm.AllVolumes()
				if err != nil {
					return err
				}

				for _, v := range vs {
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
					containerInfo, err := dm.RelatedContainers(v.Name, b)
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

func NewCmdInit(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init <dot>",
		Short: "Create an empty dot",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#create-an-empty-dot-dm-init-dot",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
				if err != nil {
					return err
				}
				if len(args) > 1 {
					return fmt.Errorf("Too many arguments specified (more than 1).")
				}
				if len(args) == 0 {
					return fmt.Errorf("No dot name specified.")
				}
				v := args[0]
				if !remotes.CheckName(v) {
					return fmt.Errorf("Error: %v is an invalid name", v)
				}
				exists, err := dm.VolumeExists(v)
				if err != nil {
					return err
				}
				if exists {
					return fmt.Errorf("Error: %v exists already", v)
				}
				err = dm.NewVolume(v)
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

func NewCmdSwitch(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch",
		Short: "Change which dot is active",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#select-a-different-current-dot-dm-switch-dot",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
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

				dm, err := remotes.NewDotmeshAPI(configPath)
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

func NewCmdLog(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log",
		Short: "Show commit logs",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-commits-dm-log",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
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
					fmt.Fprintf(out, "author: %s\n", (*commit.Metadata)["author"])
					fmt.Fprintf(out, "date: %s\n", (*commit.Metadata)["timestamp"])

					for name, value := range *commit.Metadata {
						if name != "author" && name != "message" && name != "timestamp" {
							fmt.Fprintf(out, "%s: %s\n", name, value)
						}
					}

					fmt.Fprintf(out, "\n")
					fmt.Fprintf(out, "    %s\n\n", (*commit.Metadata)["message"])
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

func NewCmdBranch(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "List branches",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-the-branches-dm-branch",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
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

func NewCmdReset(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset [--hard] <ref>",
		Short: "Reset current HEAD to the specified state",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#roll-back-commits-dm-reset-hard-commit",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				dm, err := remotes.NewDotmeshAPI(configPath)
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
