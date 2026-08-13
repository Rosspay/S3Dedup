package command

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"s3-dedup/internal/config"
	"s3-dedup/internal/downloader"
	"s3-dedup/internal/logger"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/s3"
	"strings"

	"github.com/minio/minio-go/v6"
	"github.com/spf13/cobra"
)

var bucketName string
var prefixName string

var errStopListing = errors.New("stop listing")

func waitForNextPage(reader *bufio.Reader) (bool, error) {
	for {
		fmt.Print("\nEnter — продолжить, q — выйти: ")

		value, err := reader.ReadString('\n')
		if err != nil {
			return false, err
		}

		switch strings.ToLower(strings.TrimSpace(value)) {
		case "":
			return true, nil
		case "q":
			return false, nil
		}
	}
}

var listPointers = &cobra.Command{
	Use:   "list-pointers",
	Short: "Lists objects in bucket stated",
	Long:  "Lists objects in bucket stated",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		cfg, err := config.ConfigParser(configPath)
		if err != nil {
			return err
		}

		logging, err := logger.New(cfg.Logging.Level, cfg.Logging.File)
		if err != nil {
			return err
		}

		originalsClient, err := s3.NewClient(ctx, cfg, *logging)
		if err != nil {
			return err
		}

		reader := bufio.NewReader(os.Stdin)
		pageSize := 20
		objectsOnPage := 0

		fmt.Printf("Bucket name: %s\n", bucketName)
		fmt.Println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
		fmt.Printf("|%-100s|%-20s|%-20s|%-30s|\n", "Key", "Size", "Content-Type", "Last Modified")
		listErr := originalsClient.ListObjects(ctx, bucketName, prefixName, true, func(info minio.ObjectInfo) error {
			statInfo, statErr := originalsClient.StatObject(ctx, bucketName, info.Key)
			if statErr != nil {
				return statErr
			}
			if statInfo.ContentType != pointer.ContentPointerType {
				return nil
			}
			if objectsOnPage == pageSize {
				proceed, err := waitForNextPage(reader)
				if err != nil {
					return err
				}
				if !proceed {
					return errStopListing
				}
				objectsOnPage = 0
			}

			fmt.Printf("|%-100s|%-20d|%-20s|%-30s|\n", statInfo.Key, statInfo.Size, statInfo.ContentType, statInfo.LastModified)
			objectsOnPage++
			return nil
		})

		switch {
		case listErr == nil:
			return nil
		case errors.Is(listErr, errStopListing):
			return nil
		default:
			return listErr
		}
	},
}

var objectKey string
var downloadPath string

var downloadObject = &cobra.Command{
	Use:     "download",
	Aliases: []string{"download-original"},
	Short:   "Downloads an object, resolving pointers to their original content",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		cfg, err := config.ConfigParser(configPath)
		if err != nil {
			return err
		}

		logging, err := logger.New(cfg.Logging.Level, cfg.Logging.File)
		if err != nil {
			return err
		}
		defer logging.Close()

		s3Client, err := s3.NewClient(ctx, cfg, *logging)
		if err != nil {
			return err
		}

		directory := downloadPath
		if directory == "" {
			directory = "."
		}
		destination := filepath.Join(directory, filepath.Base(objectKey))
		result, err := downloader.New(s3Client).Download(ctx, bucketName, objectKey, destination)
		if err != nil {
			return err
		}
		fmt.Printf("Downloaded %s/%s to %s\n", result.Bucket, result.Key, result.Destination)
		return nil
	},
}

func init() {

	listPointers.Flags().StringVarP(&bucketName, "name", "n", "", "Bucket name to list objects from")
	listPointers.MarkFlagRequired("name")
	listPointers.Flags().StringVarP(&prefixName, "prefix", "p", "", "Prefix in a bucket")

	downloadObject.Flags().StringVarP(&bucketName, "name", "n", "", "Bucket containing the object")
	downloadObject.MarkFlagRequired("name")
	downloadObject.Flags().StringVarP(&objectKey, "key", "k", "", "Object key to download")
	downloadObject.MarkFlagRequired("key")
	downloadObject.Flags().StringVarP(&downloadPath, "path", "p", "", "Directory to download the object to")

	rootCmd.AddCommand(listPointers)
	rootCmd.AddCommand(downloadObject)
}
