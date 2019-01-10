package utils

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

func Out(s ...interface{}) {
	stringified := []string{}
	for _, item := range s {
		stringified = append(stringified, fmt.Sprintf("%v", item))
	}
	ss := strings.Join(stringified, " ")
	os.Stdout.Write([]byte(ss))
}

func GetLogfile(logfile string) *os.File {
	// if LOG_TO_STDOUT {
	// TODO: make configurable
	if true {
		return os.Stdout
	}
	f, err := os.OpenFile(
		fmt.Sprintf("%s.log", logfile),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666,
	)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	return f
}
