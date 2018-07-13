package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// stuff we use for s3 management which likely isn't needed in other situations

func (f *fsMachine) getLastNonMetadataSnapshot() (*snapshot, error) {
	// for all the snapshots we have, start from the latest, work backwards until we find a snapshot which isn't just a metadata change (i.e a write of a json file about s3 versions)
	// in theory, we should only ever go back to latest-1, but could potentially go back further if we've had multiple commits slip in there.
	snaps, err := f.state.snapshotsForCurrentMaster(f.filesystemId)
	if err != nil {
		return nil, err
	}
	var latestSnap *snapshot
	for idx := len(snaps) - 1; idx > -1; idx-- {
		commitType, ok := (*snaps[idx].Metadata)["type"]
		if !ok || commitType != "dotmesh.metadata_only" {
			latestSnap = &snaps[idx]
			break
		}
	}
	return latestSnap, nil
}

// these are both kind of generic "take a map, read/write it as json" functions which could probably be used in non-s3 cases.
func loadS3Meta(filesystemId, latestSnapId string, latestMeta *map[string]string) error {
	// todo this is the only thing linking it to being s3 metadata, should we refactor this method to look more like the one below?
	pathToCommitMeta := fmt.Sprintf("%s/dm.s3-versions/%s", mnt(filesystemId), latestSnapId)
	data, err := ioutil.ReadFile(pathToCommitMeta)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, latestMeta)
	if err != nil {
		return err
	}
	return nil
}

func writeS3Metadata(path string, versions map[string]string) error {
	data, err := json.Marshal(versions)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path, data, 0600)
	if err != nil {
		return err
	}
	return nil
}

func getKeysForDir(parentPath string, subPath string) (map[string]int64, int64, error) {
	// given a directory, recurse it creating s3 style keys for all the files in it (aka relative paths from that directory)
	// send back a map of keys -> file sizes, and the whole directory's size
	path := parentPath
	if subPath != "" {
		path += "/" + subPath
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, 0, err
	}
	var dirSize int64
	keys := make(map[string]int64)
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			paths, size, err := getKeysForDir(path, fileInfo.Name())
			if err != nil {
				return nil, 0, err
			}
			for k, v := range paths {
				keys[k] = v
			}
			dirSize += size
		} else {
			keyPath := fileInfo.Name()
			if subPath != "" {
				keyPath = subPath + "/" + keyPath
			}
			keys[keyPath] = fileInfo.Size()
			dirSize += fileInfo.Size()
		}
	}
	return keys, dirSize, nil
}

func getS3Client(transferRequest S3TransferRequest) (*s3.S3, error) {
	config := &aws.Config{Credentials: credentials.NewStaticCredentials(transferRequest.KeyID, transferRequest.SecretKey, "")}
	if transferRequest.Endpoint != "" {
		config.Endpoint = &transferRequest.Endpoint
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	region, err := s3manager.GetBucketRegion(context.Background(), sess, transferRequest.RemoteName, "us-west-1")
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess, aws.NewConfig().WithRegion(region))
	return svc, nil
}

func downloadS3Bucket(svc *s3.S3, bucketName, destPath, transferRequestId string, prefixes []string, pollResult *TransferPollResult, currentKeyVersions map[string]string) (bool, map[string]string, error) {
	if len(prefixes) == 0 {
		return downloadPartialS3Bucket(svc, bucketName, destPath, transferRequestId, "", pollResult, currentKeyVersions)
	}
	var changed bool
	var err error
	for _, prefix := range prefixes {
		changed, currentKeyVersions, err = downloadPartialS3Bucket(svc, bucketName, destPath, transferRequestId, prefix, pollResult, currentKeyVersions)
		if err != nil {
			return false, nil, err
		}
	}
	return changed, currentKeyVersions, nil
}

func downloadPartialS3Bucket(svc *s3.S3, bucketName, destPath, transferRequestId, prefix string, pollResult *TransferPollResult, currentKeyVersions map[string]string) (bool, map[string]string, error) {
	// for every version in the bucket
	// 1. Delete anything locally that's been deleted in S3.
	// 2. Download new versions of things that have changed
	// 3. Return a map of object key -> s3 version id, plus an indicator of whether anything actually changed during this process so we know whether to make a snapshot.
	var bucketChanged bool
	// TODO refactor this a lil so we can get the folder structure easily
	fmt.Printf("[downloadS3Bucket] currentVersions: %#v", currentKeyVersions)
	params := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}
	if prefix != "" {
		params.Prefix = prefix
	}
	downloader := s3manager.NewDownloaderWithClient(svc)
	var innerError error
	err := svc.ListObjectVersionsPages(params,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, item := range page.DeleteMarkers {
				latestMeta, _ := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					deletePath := fmt.Sprintf("%s/%s", destPath, *item.Key)
					log.Printf("Got object for deletion: %#v, key: %s", item, *item.Key)
					err := os.RemoveAll(deletePath)
					if err != nil && !os.IsNotExist(err) {
						innerError = err
						return false
					}
					currentKeyVersions[*item.Key] = *item.VersionId
					bucketChanged = true

				}
			}
			for _, item := range page.Versions {
				latestMeta, _ := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					log.Printf("Got object: %#v, key: %s", item, *item.Key)
					pollResult.Index += 1
					pollResult.Total += 1
					pollResult.Size = *item.Size
					pollResult.Status = "Pulling"
					// ERROR CATCHING?
					innerError = updatePollResult(transferRequestId, *pollResult)
					if innerError != nil {
						return false
					}
					innerError = downloadS3Object(downloader, *item.Key, *item.VersionId, bucketName, destPath)
					if innerError != nil {
						return false
					}
					pollResult.Sent = *item.Size
					pollResult.Status = "Pulled file successfully"
					// todo error catching
					innerError = updatePollResult(transferRequestId, *pollResult)
					if innerError != nil {
						return false
					}
					currentKeyVersions[*item.Key] = *item.VersionId
					bucketChanged = true

				}
			}
			return !lastPage
		})
	if pollResult.Total == pollResult.Index {
		pollResult.Status = "finished"
		updatePollResult(transferRequestId, *pollResult)
	}

	if err != nil {
		return bucketChanged, nil, err
	} else if innerError != nil {
		return bucketChanged, nil, innerError
	}
	fmt.Printf("New key versions: %#v", currentKeyVersions)
	return bucketChanged, currentKeyVersions, nil
}

func downloadS3Object(downloader *s3manager.Downloader, key, versionId, bucket, destPath string) error {
	fpath := fmt.Sprintf("%s/%s", destPath, key)
	directoryPath := fpath[:strings.LastIndex(fpath, "/")]
	err := os.MkdirAll(directoryPath, 0666)
	if err != nil {
		log.Printf("Hit an error making all dirs")
		return err
	}
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: &versionId,
	})
	if err != nil {
		return err
	}
	return nil
}

func removeOldS3Files(keyToVersionIds map[string]string, paths map[string]int64, bucket string, svc *s3.S3) (map[string]string, error) {
	// get a list of the objects in s3, if there's anything there that isn't in our list of files in dotmesh, delete it.
	params := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	var innerError error
	err := svc.ListObjectsV2Pages(params, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range output.Contents {
			if _, ok := paths[*item.Key]; !ok {
				deleteOutput, innerError := svc.DeleteObject(&s3.DeleteObjectInput{
					Key:    item.Key,
					Bucket: aws.String(bucket),
				})
				if innerError != nil {
					return false
				}
				keyToVersionIds[*item.Key] = *deleteOutput.VersionId
			}
		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}
	if innerError != nil {
		return nil, innerError
	}
	return keyToVersionIds, nil
}

func updateS3Files(keyToVersionIds map[string]string, paths map[string]int64, pathToMount, transferRequestId, bucket string, svc *s3.S3, pollResult TransferPollResult) (map[string]string, error) {
	// push every key up to s3 and then send back a map of object key -> s3 version id
	uploader := s3manager.NewUploaderWithClient(svc)
	for key, size := range paths {
		path := fmt.Sprintf("%s/%s", pathToMount, key)
		file, err := os.Open(path)
		pollResult.Index += 1

		if err != nil {
			return nil, err
		}
		output, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   file,
		})
		if err != nil {
			return nil, err
		}
		keyToVersionIds[key] = *output.VersionID
		pollResult.Sent += size
		updatePollResult(transferRequestId, pollResult)
		err = file.Close()
		if err != nil {
			return nil, err
		}
	}
	return keyToVersionIds, nil
}
