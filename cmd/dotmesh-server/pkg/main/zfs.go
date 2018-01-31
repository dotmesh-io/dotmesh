package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

// functions which relate to interacting directly with zfs

// how many bytes has a filesystem diverged from its latest snapshot?
// also how many bytes does the filesystem take up on disk in total?
// TODO rename getDirtyDelta and dirtyInfo etc to sizeInfo
func getDirtyDelta(filesystemId, latestSnap string) (int64, int64, error) {
	o, err := exec.Command(
		"zfs", "get", "-pHr", "referenced,used", fq(filesystemId),
	).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf(
			"[pollDirty] 'zfs get -pHr referenced,used %s' errored with: %s %s",
			fq(filesystemId), err, o,
		)
	}
	/*
		pool/y  referenced      104948736       -
		pool/y  used    209887232       -
		pool/y@now      referenced      104948736       -
		pool/y@now      used    104938496       -
	*/
	var referDataset, referLatestSnap, usedLatestSnap, usedDataset int64
	lines := strings.Split(string(o), "\n")
	for _, line := range lines {
		shrap := strings.Fields(line)
		if len(shrap) >= 3 {
			if shrap[0] == fq(filesystemId) {
				if shrap[1] == "referenced" {
					referDataset, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				} else if shrap[1] == "used" {
					usedDataset, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				}
			} else if shrap[0] == fq(filesystemId)+"@"+latestSnap {
				if shrap[1] == "referenced" {
					referLatestSnap, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				} else if shrap[1] == "used" {
					usedLatestSnap, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				}
			}
		}
	}
	// Dirty filesystems that have been rolled back to the latest snapshot
	// sometimes exhibit 1024 bytes used.
	if usedLatestSnap <= 1024 {
		usedLatestSnap = 0
	}
	return intDiff(referDataset, referLatestSnap) + usedLatestSnap, usedDataset, nil
}

func intDiff(a, b int64) int64 {
	if a-b < 0 {
		return b - a
	} else {
		return a - b
	}
}

func findLocalPoolId() (string, error) {
	output, err := exec.Command(ZPOOL, "get", "-H", "guid", POOL).CombinedOutput()
	if err != nil {
		return string(output), err
	}
	i, err := strconv.ParseUint(strings.Split(string(output), "\t")[2], 10, 64)
	if err != nil {
		return string(output), err
	}
	return fmt.Sprintf("%x", i), nil
}

func findFilesystemIdsOnSystem() []string {
	// synchronously, return slice of filesystem ids that exist.
	log.Print("Finding filesystem ids...")
	listArgs := []string{"list", "-H", "-r", "-o", "name", POOL + "/" + ROOT_FS}
	// look before you leap (check error code of zfs list)
	code, err := returnCode(ZFS, listArgs...)
	if err != nil {
		log.Fatalf("%s, when running zfs list", err)
	}
	// creates pool/dmfs on demand if it doesn't exist.
	if code != 0 {
		output, err := exec.Command(
			ZFS, "create", "-o", "mountpoint=legacy", POOL+"/"+ROOT_FS).CombinedOutput()
		if err != nil {
			out("Unable to create", POOL+"/"+ROOT_FS, "- does ZFS pool '"+POOL+"' exist?\n")
			log.Printf(string(output))
			log.Fatal(err)
		}
	}
	// get output
	output, err := exec.Command(ZFS, listArgs...).Output()
	if err != nil {
		log.Fatalf("%s, while getting output from zfs list", err)
	}
	// output should now contain newline delimited list of fq filesystem names.
	newLines := []string{}
	lines := strings.Split(string(output), "\n")
	// strip off the first one, which is always the root pool itself, and last
	// one which is empty newline
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		newLines = append(newLines, unfq(line))
	}
	return newLines
}

func deleteFilesystemInZFS(fs string) error {
	cmd := exec.Command(ZFS, "destroy", "-r", fq(fs))
	errBuffer := bytes.Buffer{}
	cmd.Stderr = &errBuffer
	err := cmd.Run()
	if err != nil {
		readErr, err := ioutil.ReadAll(&errBuffer)
		if readErr != nil {
			return fmt.Errorf("error reading error: %v", readErr)
		}
		return fmt.Errorf("error running zfs destroy on filesystem %s: %v, %v", fs, err, string(errBuffer.Bytes()))
	}

	return nil
}

func discoverSystem(fs string) (*filesystem, error) {
	// TODO sanitize fs
	// does filesystem exist? (early exit if not)
	code, err := returnCode(ZFS, "list", fq(fs))
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return &filesystem{
			id:     fs,
			exists: false,
		}, nil
	}
	// is filesystem mounted?
	code, err = returnCode("mountpoint", mnt(fs))
	if err != nil {
		return nil, err
	}
	mounted := code == 0
	// what metadata is encoded in any snapshots' zfs properties?
	// construct metadata where it exists
	//filesystemMeta := metadata{} // TODO fs-specific metadata
	snapshotMeta := map[string]metadata{}
	output, err := exec.Command(
		ZFS, "get", "all", "-H", "-r", "-s", "local,received", fq(fs),
	).Output()
	if err != nil {
		return nil, err
	}
	metaLines := strings.Split(string(output), "\n")
	// strip off trailing newline
	metaLines = metaLines[:len(metaLines)-1]
	for _, values := range metaLines {
		shrapnel := strings.Split(values, "\t")
		if len(shrapnel) > 3 {
			fsSnapshot := shrapnel[0]
			// strip off meta prefix
			keyEncoded := shrapnel[1]
			if strings.HasPrefix(keyEncoded, META_KEY_PREFIX) {
				keyEncoded = keyEncoded[len(META_KEY_PREFIX):]
				// base64 decode or die
				valueEncoded := shrapnel[2]

				decoded, err := base64.StdEncoding.DecodeString(valueEncoded)
				if err != nil {
					log.Printf(
						"Unable to base64 decode metadata value '%s' for %s",
						valueEncoded,
						fsSnapshot,
					)
				} else {
					if strings.Contains(fsSnapshot, "@") {
						id := strings.Split(fsSnapshot, "@")[1]
						_, ok := snapshotMeta[id]
						if !ok {
							snapshotMeta[id] = metadata{}
						}
						snapshotMeta[id][keyEncoded] = string(decoded)
					} else {
						// TODO populate filesystemMeta
					}
				}
			}
		}
	}

	// what snapshots exist of the filesystem?
	output, err = exec.Command(ZFS,
		"list", "-H", "-t", "filesystem,snapshot", "-r", fq(fs)).Output()
	if err != nil {
		return nil, err
	}
	listLines := strings.Split(string(output), "\n")

	// strip off trailing newline and root pool
	listLines = listLines[1 : len(listLines)-1]
	snapshots := []*snapshot{}
	for _, values := range listLines {
		fsSnapshot := strings.Split(values, "\t")[0]
		id := strings.Split(fsSnapshot, "@")[1]
		meta, ok := snapshotMeta[id]
		if !ok {
			meta = metadata{}
		}
		snapshot := &snapshot{Id: id, Metadata: &meta}
		snapshots = append(snapshots, snapshot)
	}
	filesystem := &filesystem{
		id:        fs,
		exists:    true,
		mounted:   mounted,
		snapshots: snapshots,
	}
	return filesystem, nil
}
