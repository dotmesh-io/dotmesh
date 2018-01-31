package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
)

func encodeMetadata(meta metadata) ([]string, error) {
	/*
	   Encode a map of key value pairs into metadata-setting zfs command
	   list-of-command-arguments (as part of 'zfs snapshot'), ie:

	   []string{"-o foo=bar", "-o baz=bash"}

	   Keys must be alphanumeric, start with a letter, and have numbers or
	   hyphens after the initial character.
	*/
	metadataEncoded := []string{}
	KEY_REGEX := "[a-z]+[a-z0-9-]*"
	for k, v := range meta {
		result, err := regexp.Match(KEY_REGEX, []byte(k))
		if err != nil {
			return []string{}, err
		}
		if !result {
			return []string{}, errors.New(
				fmt.Sprintf("%r does not match %s", k, KEY_REGEX),
			)
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(v))
		if len(encoded) > 1024 {
			return []string{}, errors.New(
				"Encoded metadata value size exceeds 1024 bytes",
			)
		}
		metadataEncoded = append(
			metadataEncoded, "-o",
			fmt.Sprintf("%s%s=%s", META_KEY_PREFIX, k, encoded),
		)
	}
	return metadataEncoded, nil
}
