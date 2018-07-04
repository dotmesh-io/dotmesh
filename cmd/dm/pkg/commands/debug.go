package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
)

func NewCmdDebug(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Make API calls",
		Run: func(cmd *cobra.Command, args []string) {
			err := func() error {
				method := args[0]
				dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
				if err != nil {
					return err
				}
				var response interface{}
				if len(args) > 1 {
					if parseDebugParamsJSON {
						var params map[string]interface{}
						err = json.Unmarshal([]byte(args[1]), &params)
						if err != nil {
							fmt.Fprintln(os.Stderr, err.Error())
							os.Exit(1)
						}
						err = dm.CallRemote(context.Background(), method, params, &response)
					} else {
						err = dm.CallRemote(context.Background(), method, args[1], &response)
					}
				} else {
					err = dm.CallRemote(context.Background(), method, nil, &response)
				}
				r, err := json.Marshal(response)
				if err != nil {
					return err
				}
				fmt.Printf(string(r) + "\n")
				return err
			}()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.Flags().BoolVarP(
		&parseDebugParamsJSON, "parse-json", "", false,
		"Parse the params argument into JSON before handing off to CallRemote.  "+
			"This enables the passing of structured params to RPC endpoints.",
	)
	return cmd
}
