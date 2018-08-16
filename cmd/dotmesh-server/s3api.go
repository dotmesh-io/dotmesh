package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"
)

// functions to make dotmesh server act like an S3 server

type S3ApiRequest struct {
	Filename    string
	Data        []byte
	RequestType string
}

func s3ApiRequestify(in interface{}) (S3ApiRequest, error) {
	typed, ok := in.(map[string]interface{})
	if !ok {
		log.Printf("[s3ApiRequestify] Unable to cast %s to map[string]interface{}", in)
		return S3ApiRequest{}, fmt.Errorf(
			"Unable to cast %s to map[string]interface{}", in,
		)
	}
	data, ok := typed["Data"].(string)
	if !ok {
		return S3ApiRequest{}, fmt.Errorf("Could not cast %#v to string", typed["Data"])
	}
	bytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return S3ApiRequest{}, fmt.Errorf("Unable to decode data %s to bytes", data)
	}
	return S3ApiRequest{
		Filename:    typed["Filename"].(string),
		Data:        bytes,
		RequestType: "put",
	}, nil
}

func (f *fsMachine) saveFile(request S3ApiRequest) stateFn {
	log.Printf("Saving file %s", request.Filename)
	// create the default paths
	destPath := fmt.Sprintf("%s/%s/%s", mnt(f.filesystemId), "__default__", request.Filename)
	directoryPath := destPath[:strings.LastIndex(destPath, "/")]
	err := os.MkdirAll(directoryPath, 0775)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-create-dir",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	file, err := os.Create(destPath)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-create-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	_, err = file.Write(request.Data)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-write-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	err = file.Close()
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-close-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	return discoveringState
}
