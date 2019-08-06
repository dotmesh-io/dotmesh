package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

// ReadFile - reads a file from the volume into the supplied Contents io.Writer,
// response will be sent to a provided Response channel
func (f *FsMachine) ReadFile(destination *types.OutputFile) {
	f.fileOutputIO <- destination
}

// WriteFile - reads the supplied Contents io.Reader and writes into the volume,
// response will be sent to a provided Response channel
func (f *FsMachine) WriteFile(source *types.InputFile) {
	f.fileInputIO <- source
}

func (f *FsMachine) getLastNonMetadataSnapshot() (*types.Snapshot, error) {
	// for all the snapshots we have, start from the latest, work backwards until we find a snapshot which isn't just a metadata change (i.e a write of a json file about s3 versions)
	// in theory, we should only ever go back to latest-1, but could potentially go back further if we've had multiple commits slip in there.
	snaps, err := f.state.SnapshotsForCurrentMaster(f.filesystemId)
	if err != nil {
		return nil, err
	}
	var latestSnap *types.Snapshot
	for idx := len(snaps) - 1; idx > -1; idx-- {
		commitType, ok := snaps[idx].Metadata["type"]
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
	pathToCommitMeta := fmt.Sprintf("%s/dm.s3-versions/%s", utils.Mnt(filesystemId), latestSnapId)
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

//GetKeysForDirLimit - recurse it creating s3 style keys for all the files in it (aka relative paths from that directory)
// send back a map of keys -> file sizes, and the whole directory's size
func GetKeysForDirLimit(parentPath string, subPath string, limit int64) (keys map[string]os.FileInfo, dirSize int64, used int64, err error) {
	path := parentPath
	if subPath != "" {
		path += "/" + subPath
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, 0, 0, err
	}

	keys = make(map[string]os.FileInfo)
	dirsToTraverse := make([]os.FileInfo, 0)

	for _, fileInfo := range files {
		if limit != 0 && limit-used <= 0 {
			// no point looking at more files when we're over the limit
			break
		}
		if strings.HasPrefix(fileInfo.Name(), ".") {
			continue
		}
		if fileInfo.IsDir() {
			// save these for later, so that we're breadth first
			dirsToTraverse = append(dirsToTraverse, fileInfo)
		} else {
			used = used + 1
			keyPath := fileInfo.Name()
			if subPath != "" {
				keyPath = subPath + "/" + keyPath
			}
			keys[keyPath] = fileInfo
			dirSize += fileInfo.Size()
		}
	}

	for _, dirInfo := range dirsToTraverse {
		// no point recursing into more directories when we're over the limit
		if limit != 0 && limit-used <= 0 {
			break
		}

		var currentLimit int64
		if limit != 0 {
			currentLimit = limit - used
		}

		paths, size, recursiveUsed, err := GetKeysForDirLimit(path, dirInfo.Name(), currentLimit)
		used = used + recursiveUsed
		if err != nil {
			return nil, 0, 0, err
		}
		for k, v := range paths {
			if subPath == "" {
				keys[k] = v
			} else {
				keys[subPath+"/"+k] = v
			}
		}
		dirSize += size
	}

	return keys, dirSize, used, nil
}

func getS3Client(transferRequest types.S3TransferRequest) (*s3.S3, error) {
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

func downloadS3Bucket(f *FsMachine, svc *s3.S3, bucketName, destPath, transferRequestId string, prefixes []string, currentKeyVersions map[string]string) (bool, map[string]string, error) {
	log.Debugf("[downloadS3Bucket] Prefixes: %#v, len: %d", prefixes, len(prefixes))
	if len(prefixes) == 0 {
		return downloadPartialS3Bucket(f, svc, bucketName, destPath, transferRequestId, "", currentKeyVersions)
	}
	var changed bool
	var err error

	for _, prefix := range prefixes {
		log.Debugf("[downloadS3Bucket] Pulling down objects prefixed %s", prefix)
		changed, currentKeyVersions, err = downloadPartialS3Bucket(f, svc, bucketName, destPath, transferRequestId, prefix, currentKeyVersions)
		if err != nil {
			return false, nil, err
		}
	}
	log.Debugf("[downloadS3Bucket] currentVersions: %#v", currentKeyVersions)
	return changed, currentKeyVersions, nil
}

func downloadPartialS3Bucket(f *FsMachine, svc *s3.S3, bucketName, destPath, transferRequestId, prefix string, currentKeyVersions map[string]string) (bool, map[string]string, error) {
	// for every version in the bucket
	// 1. Delete anything locally that's been deleted in S3.
	// 2. Download new versions of things that have changed
	// 3. Return a map of object key -> s3 version id, plus an indicator of whether anything actually changed during this process so we know whether to make a snapshot.
	var bucketChanged bool
	params := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}
	if prefix != "" {
		params.SetPrefix(prefix)
	}
	log.Debugf("[downloadPartialS3Bucket] params: %#v", *params)
	downloader := s3manager.NewDownloaderWithClient(svc)
	var innerError error
	err := svc.ListObjectVersionsPages(params,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, item := range page.DeleteMarkers {
				latestMeta := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					deletePath := fmt.Sprintf("%s/%s", destPath, *item.Key)
					log.Debugf("Got object for deletion: %#v, key: %s", item, *item.Key)
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
				latestMeta := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					log.Debugf("Got object: %#v, key: %s", item, *item.Key)

					f.transferUpdates <- types.TransferUpdate{
						Kind: types.TransferNextS3File,
						Changes: types.TransferPollResult{
							Status: "Pulling",
							Size:   *item.Size,
						},
					}
					// ERROR CATCHING?
					innerError = downloadS3Object(f.transferUpdates, downloader, *item.Key, *item.VersionId, bucketName, destPath)
					if innerError != nil {
						return false
					}
					f.transferUpdates <- types.TransferUpdate{
						Kind: types.TransferFinishedS3File,
						Changes: types.TransferPollResult{
							Status: "Pulled file successfully",
							Sent:   *item.Size,
						},
					}
					currentKeyVersions[*item.Key] = *item.VersionId
					bucketChanged = true

				}
			}
			return !lastPage
		})

	if err != nil {
		return bucketChanged, nil, err
	} else if innerError != nil {
		return bucketChanged, nil, innerError
	}
	log.Debugf("[downloadPartialS3Bucket] New key versions: %#v", currentKeyVersions)
	return bucketChanged, currentKeyVersions, nil
}

type progressWriter struct {
	written int64
	writer  io.WriterAt
	updates chan types.TransferUpdate
}

func (pw *progressWriter) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(&pw.written, int64(len(p)))
	pw.updates <- types.TransferUpdate{
		Kind: types.TransferProgress,
		Changes: types.TransferPollResult{
			Status: "pulling",
			Sent:   pw.written,
		},
	}

	return pw.writer.WriteAt(p, off)
}

func downloadS3Object(updates chan types.TransferUpdate, downloader *s3manager.Downloader, key, versionId, bucket, destPath string) error {
	fpath := fmt.Sprintf("%s/%s", destPath, key)
	directoryPath := fpath[:strings.LastIndex(fpath, "/")]
	err := os.MkdirAll(directoryPath, 0666)
	if err != nil {
		log.WithError(err).Warn("[downloadS3Object] got an error while making all dirs")
		return err
	}
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	writer := &progressWriter{writer: file, written: 0, updates: updates}
	_, err = downloader.Download(writer, &s3.GetObjectInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: &versionId,
	})
	if err != nil {
		return err
	}
	return nil
}

func removeOldS3Files(keyToVersionIds map[string]string, paths map[string]os.FileInfo, bucket string, prefixes []string, svc *s3.S3) (map[string]string, error) {
	if len(prefixes) == 0 {
		return removeOldPrefixedS3Files(keyToVersionIds, paths, bucket, "", svc)
	}
	var err error
	for _, prefix := range prefixes {
		keyToVersionIds, err = removeOldPrefixedS3Files(keyToVersionIds, paths, bucket, prefix, svc)
		if err != nil {
			return nil, err
		}
	}
	return keyToVersionIds, nil
}
func removeOldPrefixedS3Files(keyToVersionIds map[string]string, paths map[string]os.FileInfo, bucket, prefix string, svc *s3.S3) (map[string]string, error) {
	// get a list of the objects in s3, if there's anything there that isn't in our list of files in dotmesh, delete it.
	params := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	if prefix != "" {
		params.SetPrefix(prefix)
	}
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

func updateS3Files(f *FsMachine, keyToVersionIds map[string]string, paths map[string]os.FileInfo, pathToMount, transferRequestId, bucket string, prefixes []string, svc *s3.S3) (map[string]string, error) {
	// push every key up to s3 and then send back a map of object key -> s3 version id
	uploader := s3manager.NewUploaderWithClient(svc)
	// filter out any paths we don't care about in an S3 remote
	filtered := make(map[string]os.FileInfo)
	if len(prefixes) == 0 {
		filtered = paths
		log.Debugf("[updateS3Files] files: %#v", filtered)
	}
	for _, elem := range prefixes {
		for key, size := range paths {
			if strings.HasPrefix(key, elem) {
				filtered[key] = size
			}
		}
	}
	for key, fileInfo := range filtered {
		path := fmt.Sprintf("%s/%s", pathToMount, key)
		versionId, err := uploadFileToS3(path, key, bucket, uploader)
		if err != nil {
			return nil, err
		}
		keyToVersionIds[key] = versionId

		f.transferUpdates <- types.TransferUpdate{
			Kind: types.TransferIncrementIndex,
			Changes: types.TransferPollResult{
				Sent: fileInfo.Size(),
			},
		}
	}
	return keyToVersionIds, nil
}

func uploadFileToS3(path, key, bucket string, uploader *s3manager.Uploader) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	output, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return "", err
	}
	return *output.VersionID, nil
}
