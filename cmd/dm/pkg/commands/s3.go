package commands

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

var localVolumeName string

func NewCmdS3(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "s3",
		Short: "Commands that handle S3 connections",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#list-remotes-dm-remote-v",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "remote add <remote-name> <key-id:secret-key>[@endpoint]",
		Short: "Add an S3 remote",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#add-a-new-s3-remote-dm-s3-remote-add-access-key-secret-key-host-port",

		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				if len(args) != 3 {
					return fmt.Errorf(
						"Please specify <remote-name> <key-id:secret-key>",
					)
				}
				remote := args[1]
				var endpoint string
				var awsCredentials []string
				pieces := strings.SplitN(args[2], "@", 2)
				if len(pieces) == 2 {
					awsCredentials = strings.SplitN(pieces[0], ":", 2)
					endpoint = pieces[1]
				} else if len(pieces) == 1 {
					awsCredentials = strings.SplitN(args[2], ":", 2)
				} else {
					return fmt.Errorf("Please specify key-id:secret-key[@endpoint], got %s", pieces)
				}
				if len(awsCredentials) != 2 {
					return fmt.Errorf(
						"Please specify key-id:secret-key, got %s", awsCredentials,
					)
				}
				keyID := awsCredentials[0]
				secretKey := awsCredentials[1]
				config := &aws.Config{Credentials: credentials.NewStaticCredentials(keyID, secretKey, "")}
				if endpoint != "" {
					config.Endpoint = &endpoint
				}
				sess, err := session.NewSession(config)
				if err != nil {
					return fmt.Errorf("Could not establish connection with AWS using supplied credentials")
				}
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				err = dm.Configuration.AddS3Remote(remote, keyID, secretKey, endpoint)
				if err != nil {
					return err
				}
				fmt.Fprintln(out, "s3 remote added.")
				return nil
			})
		},
	})
	subCommand := &cobra.Command{
		Use:   "clone-subset <remote> <bucket> <prefixes> [--local-name=<dot>]",
		Short: "Clone an s3 bucket, but only select a subset as dictated by comma-separated prefixes. (for full bucket clones see dm clone as normal)",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#clone-a-section-of-an-s3-bucket-dm-s3-clone-subset-remote-bucket-prefixes-local-name-local-dot",

		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				prefixes := strings.Split(args[2], ",")
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				peer, filesystemName, branchName, err := resolveTransferArgs(args[:len(args)-1])
				if err != nil {
					return err
				}
				transferId, err := dm.RequestTransfer(
					"pull", peer,
					localVolumeName, branchName,
					filesystemName, branchName,
					prefixes,
					// TODO also switch to the remote?
				)
				if err != nil {
					return err
				}
				err = dm.PollTransfer(transferId, out)
				if err != nil {
					return err
				}

				return nil
			})
		},
	}
	subCommand.PersistentFlags().StringVarP(&localVolumeName, "local-name", "", "",
		"Local dot name to create")
	cmd.AddCommand(subCommand)
	return cmd
}
