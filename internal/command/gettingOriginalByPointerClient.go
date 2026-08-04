package command

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"s3-dedup/internal/config"
	"s3-dedup/internal/logger"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/s3"
	"strings"

	"github.com/minio/minio-go/v6"
	"github.com/spf13/cobra"
)

type S3ClientForDownload interface {
	ListObjects(
		ctx context.Context,
		bucket string,
		prefix string,
		recursive bool,
		fn func(minio.ObjectInfo) error,
	) error

	GetObject(
		ctx context.Context,
		bucket string,
		key string,
	) (io.ReadCloser, error)

	StatObject(
		ctx context.Context,
		bucket string,
		objectName string,
	) (minio.ObjectInfo, error)
}

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

var pointerKey string
var downloadPath string

var getObjectByPointerKey = &cobra.Command{
	Use:   "download-original",
	Short: "Downloads original by pointer key",
	Long:  "Downloads original by pointer key",
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

		statInfo, err := originalsClient.StatObject(ctx, bucketName, pointerKey)
		if err != nil {
			return err
		}
		if statInfo.ContentType != pointer.ContentPointerType {
			return fmt.Errorf("Object key given is not a pointer type")
		}

		obj, err := originalsClient.GetObject(ctx, bucketName, pointerKey)
		if err != nil {
			return err
		}
		defer obj.Close()

		p, err := pointer.ReadPointer(obj)
		if err != nil {
			return err
		}
		_, err = originalsClient.StatObject(ctx, p.BlobBucket, p.BlobKey)
		if err != nil {
			return fmt.Errorf("Blob does not exists")
		}
		original, err := originalsClient.GetObject(ctx, p.BlobBucket, p.BlobKey)
		if err != nil {
			return err
		}
		defer original.Close()

		if downloadPath == "" {
			downloadPath = "."
		}

		path, err := filepath.Abs(downloadPath)
		if err != nil {
			return err
		}

		destination := filepath.Join(path, filepath.Base(pointerKey))

		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("create download directory %q: %w", path, err)
		}

		temp, err := os.CreateTemp(path, ".s3-dedup-download-*")
		if err != nil {
			return err
		}
		tempName := temp.Name()
		success := false
		defer func() {
			temp.Close()
			if !success {
				os.Remove(tempName)
			}
		}()

		written, err := io.Copy(temp, original)
		if err != nil {
			return err
		}
		if written != p.Size {
			return fmt.Errorf("download size: got %d, expected %d", written, p.Size)
		}
		if err := temp.Close(); err != nil {
			return err
		}
		if err := os.Rename(tempName, destination); err != nil {
			return err
		}
		success = true
		return nil
	},
}

func init() {

	listPointers.Flags().StringVarP(&bucketName, "name", "n", "", "Bucket name to list objects from")
	listPointers.MarkFlagRequired("name")
	listPointers.Flags().StringVarP(&prefixName, "prefix", "p", "", "Prefix in a bucket")

	getObjectByPointerKey.Flags().StringVarP(&bucketName, "name", "n", "", "Bucket name to list objects from")
	getObjectByPointerKey.MarkFlagRequired("name")
	getObjectByPointerKey.Flags().StringVarP(&pointerKey, "key", "k", "", "Key for pointer to get the original")
	getObjectByPointerKey.MarkFlagRequired("key")
	getObjectByPointerKey.Flags().StringVarP(&downloadPath, "path", "p", "", "Path to download the original to")

	rootCmd.AddCommand(listPointers)
	rootCmd.AddCommand(getObjectByPointerKey)
}
