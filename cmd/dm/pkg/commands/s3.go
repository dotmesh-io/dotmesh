package commands

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
		// TODO update docs
		Long: "Online help: https://docs.dotmesh.com/references/cli/#add-a-new-remote-dm-remote-add-name-user-hostname",

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
				// I don't think region actually matters, but if none is supplied the client complains
				svc := s3.New(sess, aws.NewConfig().WithRegion("us-east-1"))
				_, err = svc.ListBuckets(nil)
				if err != nil {
					fmt.Printf("Error: %#v", err)
					return fmt.Errorf("Could not list accessible buckets using supplied credentials")
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
		Short: "Clone an s3 bucket, but only select a subset as dictated by prefixes. (for full bucket clones see dm clone as normal)",
		// TODO add this to the docs
		Long: "Online help: https://docs.dotmesh.com/references/cli/#add-a-new-remote-dm-remote-add-name-user-hostname",

		Run: func(cmd *cobra.Command, args []string) {
			runHandlingError(func() error {
				prefixes := strings.Split(args[2], ",")
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				transferId, err := dm.RequestS3SubsetTransfer(
					args[0],
					"pull",
					cloneLocalVolume,
					args[2],
					prefixes,
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
