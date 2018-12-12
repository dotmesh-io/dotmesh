package zfs

import (
	"log"
	"os/exec"
	"strconv"
	"strings"
)

func GetZPoolCapacity(zpoolExecPath, poolName string) (float64, error) {
	output, err := exec.Command(zpoolExecPath,
		"list", "-H", "-o", "capacity", poolName).Output()
	if err != nil {
		log.Fatalf("%s, when running zpool list", err)
		return 0, err
	}

	parsedCapacity := strings.Trim(string(output), "% \n")
	capacityF, err := strconv.ParseFloat(parsedCapacity, 64)
	if err != nil {
		return 0, err
	}

	return capacityF, err
}
