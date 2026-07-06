package command

import (
	"fmt"
	"os"

	"s3-dedup/internal/configParser"

	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use:   "s3-dedup",
	Short: "File deduplicator for S3-storage",
	Long:  "Service-deduplicator for object S3 storage",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := configParser.Config_parser(configPath)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%#v\n", config)
	},
}

func init() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	rootCmd.MarkFlagRequired("config")
}

func Execute() {
	//rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
