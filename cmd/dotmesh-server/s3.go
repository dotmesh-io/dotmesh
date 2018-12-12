package main

import (
	"io/ioutil"
	"os"
)

func getKeysForDir(parentPath string, subPath string) (map[string]os.FileInfo, int64, error) {
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
	keys := make(map[string]os.FileInfo)
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			paths, size, err := getKeysForDir(path, fileInfo.Name())
			if err != nil {
				return nil, 0, err
			}
			for k, v := range paths {
				if subPath == "" {
					keys[k] = v
				} else {
					keys[subPath+"/"+k] = v
				}
			}
			dirSize += size
		} else {
			keyPath := fileInfo.Name()
			if subPath != "" {
				keyPath = subPath + "/" + keyPath
			}
			keys[keyPath] = fileInfo
			dirSize += fileInfo.Size()
		}
	}
	return keys, dirSize, nil
}
