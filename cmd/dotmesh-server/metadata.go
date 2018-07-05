package main

import (
	"encoding/base64"
	"fmt"
	"regexp"
)

const keyRegex = "[a-z]+[a-z0-9-]*"

var rxKeyRegex = regexp.MustCompile(keyRegex)

func encodeMetadata(meta metadata) ([]string, error) {
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
		if len(encoded) > 1024 {
			return []string{}, fmt.Errorf("Encoded metadata value size exceeds 1024 bytes")
		}
		metadataEncoded = append(
			metadataEncoded, "-o",
			fmt.Sprintf("%s%s=%s", META_KEY_PREFIX, k, encoded),
		)
	}
	return metadataEncoded, nil
}
