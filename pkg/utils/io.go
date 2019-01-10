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

// general purpose function, intended to be runnable in a goroutine, which
// reads bytes from a Reader and writes them to a Writer, closing the Writer
// when the Reader yields EOF. should be useable both to pipe command outputs
// into http responses, as well as piping http requests into command inputs.
//
// when EOF is read from the Reader, it writes true into the finished chan to
// notify of completion.
//
// it also performs non-blocking reads on the canceller channel during the
// loop, and aborts reading, closing both Reader and Writer in that case.
// when cancellation happens, cancelFunc is run with the object that was read
// from the canceller chan, in case it needs to be reused. we assume that
// Events flow over the canceller chan.
//
// if the writer implements http.Flusher, Flush() is called after each write.

// TODO: pipe would be better named Copy
func Pipe(
	r io.Reader, rDesc string, w io.Writer, wDesc string,
	finished chan bool, canceller chan *types.Event,
	cancelFunc func(*types.Event, chan *types.Event),
	notifyFunc func(int64, int64),
	compressMode string,
) {
	startTime := time.Now().UnixNano()
	var lastUpdate int64 // in UnixNano
	var totalBytes int64
	buffer := make([]byte, types.BufLength)

	// Incomplete idea below.
	/*
		// async buffer e.g. let the network read up to 32MiB of data that's
		// already been written to the buffer without blocking the buffer, or let
		// zfs write 32MiB of data that hasn't been read yet without stalling it
		nioBufOut := niobuffer.New(1024 * 1024 * 1024 * 32)
		bufROut, w := nio.Pipe(nioBufOut)
		go func() {
			nio.Copy(originalW, bufROut, nioBufOut)
		}()
		nioBufIn := niobuffer.New(1024 * 1024 * 1024 * 32)
		r, bufWIn := nio.Pipe(nioBufIn)
		go func() {
			nio.Copy(bufWIn, originalR, nioBufIn)
		}()
	*/

	// only call f() if 1 sec of nanosecs elapsed since last call to f()
	rateLimit := func(f func()) {
		if time.Now().UnixNano()-lastUpdate > 1e+9 {
			f()
			lastUpdate = time.Now().UnixNano()
		}
	}

	handleErr := func(message string, r io.Reader, w io.Writer, r2 io.Reader, w2 io.Writer) {
		if message != "" {
			log.Printf("[pipe:handleErr] " + message)
		}
		// NB: c.Close returns unhandled err here, and below.
		if c, ok := r.(io.Closer); ok {
			c.Close()
		}
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
		if c, ok := r2.(io.Closer); ok {
			c.Close()
		}
		if c, ok := w2.(io.Closer); ok {
			c.Close()
		}
		finished <- true
	}

	var writer io.Writer
	var reader io.Reader
	var err error

	log.Printf("[PIPE] reader %s => writer %s, COMPRESSMODE=%s", rDesc, wDesc, compressMode)

	if compressMode == "compress" {
		writer = gzip.NewWriter(w)
		reader = r
	} else if compressMode == "decompress" {
		reader, err = gzip.NewReader(r)
		if err != nil {
			handleErr(fmt.Sprintf("Unable to create gzip reader: %s", err), r, w, r, w)
			return
		}
		writer = w
	} else if compressMode == "none" {
		// no compression
		reader = r
		writer = w
	} else {
		handleErr(
			fmt.Sprintf(
				"Unsupported compression mode %s, choose one of 'compress', "+
					"'decompress' or 'none'",
				compressMode,
			), r, w, r, w,
		)
		return
	}

	for {
		select {
		case e := <-canceller:
			// call the cancellation function asynchronously, because it may
			// block, and we don't want to deadlock
			go cancelFunc(e, canceller)
			handleErr(
				fmt.Sprintf("Cancelling pipe from %s to %s because %s event "+
					"received on cancellation channel", rDesc, wDesc, e),
				reader, writer, r, w,
			)
			return
		default:
			// non-blocking read
		}
		nr, err := reader.Read(buffer)
		if nr > 0 {
			data := buffer[0:nr]
			nw, wErr := writer.Write(data)
			if nw != nr {
				handleErr(fmt.Sprintf("short write %d (read) != %d (written)", nr, nw), reader, writer, r, w)
				return
			}
			if f, ok := writer.(http.Flusher); ok {
				f.Flush()
			}
			if f, ok := writer.(*gzip.Writer); ok {
				// special case, we know we might have to flush the writer in
				// case of a small replication stream (and we're not speaking
				// directly to an http.Flusher any more)
				f.Flush()
			}
			totalBytes += int64(nr)
			rateLimit(func() {
				// rate limit to once per second to avoid hammering notifyFunc
				// on fast connections.
				notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			})
			if wErr != nil {
				handleErr(fmt.Sprintf("Error writing to %s: %s", wDesc, wErr), reader, writer, r, w)
				return
			}
		}
		// NB: handleErr as the final thing we do in both of the following
		// cases because it's polite to stop notifying (notifyFunc) after we're
		// said we're finished (handleErr).

		if err == io.EOF {
			// sync notification here (and in error case below) in case the
			// caller depends on synchronous notification of final state before
			// exit
			notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			// expected case, log no error
			handleErr("", reader, writer, r, w)
			return
		} else if err != nil {
			notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			handleErr(fmt.Sprintf("Error reading from %s: %s", rDesc, err), reader, writer, r, w)
			return
		}
	}
}
