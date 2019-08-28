package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"
	"time"

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
		if !ok || commitType != "dotmesh.metadata_only" && commitType != "dotmesh.initial" {
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

// get a recursive list of all file keys
// we do this so we can pick the correct page of results
// based on the limit and page query params for a recursive request
func GetAllKeysForDir(query types.ListFileRequest) (results []types.ListFileItem, err error) {
	readDirectoryPath := query.Base + "/" + query.Path
	fileNames, err := ioutil.ReadDir(readDirectoryPath)
	if err != nil {
		return nil, err
	}

	prependPath := query.Path

	if query.Path != "" {
		prependPath = prependPath + "/"
	}

	var directories []os.FileInfo
	var files []os.FileInfo

	for _, fileInfo := range fileNames {
		if strings.HasPrefix(fileInfo.Name(), ".") {
			// don't include dotfiles
			continue
		}
		if fileInfo.IsDir() {
			directories = append(directories, fileInfo)
		} else {
			files = append(files, fileInfo)
		}
	}

	// if we are including directories in the results then do so first
	if query.IncludeDirectories {
		for _, directoryInfo := range directories {
			results = append(results, types.ListFileItem{
				Key:          prependPath + directoryInfo.Name(),
				Size:         directoryInfo.Size(),
				LastModified: directoryInfo.ModTime(),
				Directory:    true,
			})
		}
	}

	for _, fileInfo := range files {
		results = append(results, types.ListFileItem{
			Key:          prependPath + fileInfo.Name(),
			Size:         fileInfo.Size(),
			LastModified: fileInfo.ModTime(),
			Directory:    false,
		})
	}

	if query.Recursive {
		for _, directoryInfo := range directories {
			directoryListRequest := types.ListFileRequest{
				Base:               query.Base,
				Path:               prependPath + directoryInfo.Name(),
				Limit:              0,
				Page:               0,
				Recursive:          true,
				IncludeDirectories: query.IncludeDirectories,
			}
			directoryListResponse, err := GetAllKeysForDir(directoryListRequest)
			if err != nil {
				return results, err
			}
			results = append(results, directoryListResponse...)
		}
	}

	return results, nil
}

//GetKeysForDirLimit - recurse it creating s3 style keys for all the files in it (aka relative paths from that directory)
// send back a map of keys -> file sizes, and the whole directory's size
func GetKeysForDirLimit(query types.ListFileRequest) (results types.ListFileResponse, err error) {
	items, err := GetAllKeysForDir(query)

	if err != nil {
		return results, err
	}

	results.TotalCount = int64(len(items))
	results.Items = items

	return results, nil
}

func getS3Client(transferRequest types.S3TransferRequest) (*s3.S3, error) {
	config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(transferRequest.KeyID, transferRequest.SecretKey, ""),
		MaxRetries:  aws.Int(5),
	}
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
	params := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}
	if prefix != "" {
		params.SetPrefix(prefix)
	}
	log.Debugf("[downloadPartialS3Bucket] params: %#v", *params)
	downloader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
		d.Concurrency = 10
	})
	var innerError error
	startTime := time.Now()
	var sent int64

	// build up the full list of files we will download and delete
	filesToDelete := make([]*s3.DeleteMarkerEntry, 0)
	filesToDownload := make([]*s3.ObjectVersion, 0)

	// loop over objects in the bucket and add them to the delete or download collection
	log.Debugf("Started collecting files to download and delete...")
	var totalSize int64 = 0
	err := svc.ListObjectVersionsPages(params,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, item := range page.DeleteMarkers {
				latestMeta := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					filesToDelete = append(filesToDelete, item)
				}
			}
			for _, item := range page.Versions {
				latestMeta := currentKeyVersions[*item.Key]
				if *item.IsLatest && latestMeta != *item.VersionId {
					filesToDownload = append(filesToDownload, item)
					totalSize += *item.Size
				}
			}
			return !lastPage
		})

	if err != nil {
		return false, nil, err
	}
	log.WithField("added", len(filesToDownload)).WithField("deleted", len(filesToDelete)).Debugf("[pkg/fsm/s3.go.downloadPartialS3Bucket] Ok, done. Will delete files now.")

	// loop over the files marked for deletion and remove them from the file system
	for _, item := range filesToDelete {
		deletePath := fmt.Sprintf("%s/%s", destPath, *item.Key)
		err := os.RemoveAll(deletePath)
		if err != nil && !os.IsNotExist(err) {
			return false, nil, err
		}
		currentKeyVersions[*item.Key] = *item.VersionId
	}
	// work out how many objects we have and the total size of them all

	totalObjects := len(filesToDownload)

	// pass this info off to the fsm
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStartS3Bucket,
		Changes: types.TransferPollResult{
			Status:  "Initiating Bucket Download",
			Message: "Starting download",
			Total:   totalObjects,
			Size:    totalSize,
		},
	}
	log.Debugf("[pkg/fsm/s3.go.downloadPartialS3Bucket] Ok, files deleted. Will download files now.")
	completed := make(chan types.ItemData, len(filesToDownload))
	sem := make(chan bool, 100)
	// loop over the files marked for download
	for _, item := range filesToDownload {
		sem <- true
		f.transferUpdates <- types.TransferUpdate{
			Kind: types.TransferNextS3File,
			Changes: types.TransferPollResult{
				Status:  "Pulling",
				Message: "Downloading " + *item.Key,
			},
		}
		go func(item *s3.ObjectVersion) {
			for i := 0; i < 5; i++ {
				innerError = downloadS3Object(f.transferUpdates, downloader, sent, startTime, *item.Key, *item.VersionId, bucketName, destPath, *item.Size)
				if innerError == nil {
					f.transferUpdates <- types.TransferUpdate{
						Kind: types.TransferFinishedS3File,
						Changes: types.TransferPollResult{
							Status: "Pulled file successfully",
						},
					}
					completed <- types.ItemData{
						Name:      *item.Key,
						VersionId: *item.VersionId,
						Size:      *item.Size,
						Err:       nil,
					}
					<-sem
					return
				}
				f.transferUpdates <- types.TransferUpdate{
					Kind: types.TransferS3Stuck,
					Changes: types.TransferPollResult{
						Status:  "stuck",
						Message: fmt.Sprintf("S3 file %s got stuck, retrying in 10 seconds...", *item.Key),
					},
				}
				time.Sleep(10 * time.Second)
			}
			if innerError != nil {
				f.transferUpdates <- types.TransferUpdate{
					Kind: types.TransferS3Failed,
					Changes: types.TransferPollResult{
						Status:  "S3 stuck!",
						Message: innerError.Error(),
					},
				}
				completed <- types.ItemData{
					Name:      *item.Key,
					VersionId: *item.VersionId,
					Size:      *item.Size,
					Err:       innerError,
				}
				<-sem
				return
			}
		}(item)
	}
	fileCount := len(filesToDownload)
	log.Debugf("[pkg/fsm/s3.go.downloadPartialS3Bucket] %d files started downloading, waiting for completion...", fileCount)
	counter := 0
	for {
		select {
		default:
			if counter == fileCount {
				log.Debugf("[pkg/fsm/s3.go.downloadPartialS3Bucket] Finished downloading!")
				return len(filesToDelete) > 0 || fileCount > 0, currentKeyVersions, nil
			}
		case item := <-completed:
			if item.Err != nil {
				return false, nil, item.Err
			}
			sent += item.Size
			currentKeyVersions[item.Name] = item.VersionId
			counter += 1
		}
	}
}

type progressWriter struct {
	key       string
	written   int64
	writer    io.WriterAt
	startTime time.Time
	updates   chan types.TransferUpdate
}

func (pw *progressWriter) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(&pw.written, int64(len(p)))
	elapsed := time.Since(pw.startTime).Nanoseconds()
	pw.updates <- types.TransferUpdate{
		Kind: types.TransferS3Progress,
		Changes: types.TransferPollResult{
			Status:             "pulling",
			Sent:               int64(len(p)),
			NanosecondsElapsed: elapsed,
			Message:            "Downloading " + pw.key,
		},
	}

	return pw.writer.WriteAt(p, off)
}

func downloadS3Object(updates chan types.TransferUpdate, downloader *s3manager.Downloader, startSent int64, startTime time.Time, key, versionId, bucket, destPath string, fileSize int64) error {
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
	writer := &progressWriter{
		key:       key,
		writer:    file,
		updates:   updates,
		startTime: startTime,
	}
	var size int64
	context, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := downloader.DownloadWithContext(context, writer, &s3.GetObjectInput{
			Bucket:    &bucket,
			Key:       &key,
			VersionId: &versionId,
		})
		done <- err
	}()
	for {
		select {
		case err := <-done:
			return err
		default:
			if fileSize != 0 {
				time.Sleep(10 * time.Second)
			}
			info, err := file.Stat()
			if err != nil {
				cancel()
				return err
			}
			if info.Size() == size && size != fileSize {
				log.WithFields(log.Fields{
					"size":       size,
					"key":        key,
					"bucket":     bucket,
					"version_id": versionId,
				}).Error("The file hasn't grown in 10 seconds, the download might be stuck. Cancelling and deleting the file...")
				cancel()
				err = file.Close()
				if err != nil {
					return err
				}
				os.Remove(fpath)
				return fmt.Errorf("Failed downloading file, download got stuck")
			} else if info.Size() > size {
				size = info.Size()
			}
		}
	}
}

func removeOldS3Files(keyToVersionIds map[string]string, files []types.ListFileItem, bucket string, prefixes []string, svc *s3.S3) (map[string]string, error) {
	if len(prefixes) == 0 {
		return removeOldPrefixedS3Files(keyToVersionIds, files, bucket, "", svc)
	}
	var err error
	for _, prefix := range prefixes {
		keyToVersionIds, err = removeOldPrefixedS3Files(keyToVersionIds, files, bucket, prefix, svc)
		if err != nil {
			return nil, err
		}
	}
	return keyToVersionIds, nil
}
func removeOldPrefixedS3Files(keyToVersionIds map[string]string, files []types.ListFileItem, bucket, prefix string, svc *s3.S3) (map[string]string, error) {
	// get a list of the objects in s3, if there's anything there that isn't in our list of files in dotmesh, delete it.
	params := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	if prefix != "" {
		params.SetPrefix(prefix)
	}
	paths := make(map[string]types.ListFileItem)
	for _, file := range files {
		paths[file.Key] = file
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

func updateS3Files(f *FsMachine, keyToVersionIds map[string]string, files []types.ListFileItem, pathToMount, transferRequestId, bucket string, prefixes []string, svc *s3.S3) (map[string]string, error) {
	// push every key up to s3 and then send back a map of object key -> s3 version id
	uploader := s3manager.NewUploaderWithClient(svc)
	// filter out any paths we don't care about in an S3 remote
	//filtered := make(map[string]os.FileInfo)
	var filtered []types.ListFileItem
	if len(prefixes) == 0 {
		filtered = files
		log.Debugf("[updateS3Files] files: %#v", filtered)
	}
	for _, elem := range prefixes {
		for _, file := range files {
			if strings.HasPrefix(file.Key, elem) {
				filtered = append(filtered, file)
			}
		}
	}
	for _, file := range filtered {
		path := fmt.Sprintf("%s/%s", pathToMount, file.Key)
		versionId, err := uploadFileToS3(path, file.Key, bucket, uploader)
		if err != nil {
			return nil, err
		}
		keyToVersionIds[file.Key] = versionId

		f.transferUpdates <- types.TransferUpdate{
			Kind: types.TransferIncrementIndex,
			Changes: types.TransferPollResult{
				Sent: file.Size,
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
