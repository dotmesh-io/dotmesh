package fsm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const keyRegex = "[a-z]+[a-z0-9-]*"

var rxKeyRegex = regexp.MustCompile(keyRegex)

func encodeMetadata(meta map[string]string) ([]string, error) {
	/*
	   Encode a map of key value pairs into metadata-setting zfs command
	   list-of-command-arguments (as part of 'zfs snapshot'), ie:

	   []string{"-o foo=bar", "-o baz=bash"}

	   Keys must be alphanumeric, start with a letter, and have numbers or
	   hyphens after the initial character.
	*/
	metadataEncoded := []string{}
	for k, v := range meta {
		if !rxKeyRegex.MatchString(k) {
			return []string{}, fmt.Errorf("%s does not match %s", k, keyRegex)

		}

		encoded := base64.StdEncoding.EncodeToString([]byte(v))
		if v == "" {
			encoded = "."
		}

		if len(encoded) > 1024 {
			return []string{}, fmt.Errorf("Encoded metadata value size exceeds 1024 bytes")
		}

		metadataEncoded = append(
			metadataEncoded, "-o",
			fmt.Sprintf("%s%s=%s", types.MetaKeyPrefix, k, encoded),
		)
	}
	return metadataEncoded, nil
}

func encodeMapValues(meta map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range meta {
		encoded := base64.StdEncoding.EncodeToString([]byte(v))
		if v == "" {
			encoded = "."
		}
		result[k] = encoded
	}
	return result
}

func (f *FsMachine) writeMetadata(meta map[string]string, filesystemId, snapshotId string) error {
	pathToFs := utils.Mnt(f.filesystemId)
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	metaFile := fmt.Sprintf("%s/dotmesh.metadata/%s.json", pathToFs, snapshotId)
	log.WithField("meta_file", metaFile).WithField("path", pathToFs).Debug("Writing metadata to file")
	err = os.MkdirAll(filepath.Dir(metaFile), 0666)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(metaFile, data, 0666)
	if err != nil {
		return err
	}
	return nil
}

func (f *FsMachine) getMetadata(commit *types.Snapshot) (map[string]string, error) {
	pathToFs := utils.Mnt(f.filesystemId)
	result := f.Mount()
	if result.Name != "mounted" {
		return nil, fmt.Errorf("Failed mounting filesystem, event - %#v", result)
	}
	log.WithField("path", pathToFs).WithField("event", *result).Debug("[metadata.getMetadata] ok, mounted, will read files now")
	metaFile := fmt.Sprintf("%s/dotmesh.metadata/%s.json", pathToFs, commit.Id)
	data, err := ioutil.ReadFile(metaFile)
	// ignore os.IsNotExist - that probably means it's a commit from before we started writing commit metadata to a file
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		log.Debug("[metadata.getMetadata] did not find any data, will return whatever was there already")
		return commit.Metadata, nil
	}
	var overrides map[string]string
	err = json.Unmarshal(data, &overrides)
	if err != nil {
		return nil, err
	}
	for key, value := range overrides {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, err
		}
		commit.Metadata[key] = string(decoded)
	}
	return commit.Metadata, nil
}
