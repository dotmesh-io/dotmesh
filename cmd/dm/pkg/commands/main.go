package commands

import (
	"fmt"
	"os"
	"path/filepath"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

var configPath string
var verboseOutput bool
var makeBranch bool
var forceMode bool
var scriptingMode bool
var parseDebugParamsJSON bool
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
	MainCmd.AddCommand(NewCmdS3(os.Stdout))
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

	MainCmd.PersistentFlags().BoolVarP(
		&verboseOutput, "verbose", "",
		false,
		"Display details of RPC requests and responses to the dotmesh server",
	)
}
