package breader

import (
	"errors"
	"io"
	"runtime"
	"strings"

	"github.com/brentp/xopen"
)

// Chunk is a struct compossing with slice of data and error as status
type Chunk struct {
	Data []interface{}
	Err  error
}

// BufferedReader is BufferedReader
type BufferedReader struct {
	reader      *xopen.Reader
	BufferSize  int
	ChunkSize   int
	ProcessFunc func(string) (interface{}, bool, error)

	Ch        chan Chunk
	done      chan struct{}
	finished  bool
	cancelled bool
}

// NewDefaultBufferedReader creates BufferedReader with default parameter
func NewDefaultBufferedReader(file string) (*BufferedReader, error) {
	reader, err := initBufferedReader(file, runtime.NumCPU(), 1000000, DefaultFunc)
	if err != nil {
		return reader, err
	}
	reader.run()
	return reader, nil
}

// NewBufferedReader is the constructor of BufferedReader with full parameters
func NewBufferedReader(file string, bufferSize int, chunkSize int, fn func(line string) (interface{}, bool, error)) (*BufferedReader, error) {
	reader, err := initBufferedReader(file, bufferSize, chunkSize, fn)
	if err != nil {
		return reader, err
	}
	reader.run()
	return reader, nil
}

// DefaultFunc just trim the new line symbol
var DefaultFunc = func(line string) (interface{}, bool, error) {
	line = strings.TrimRight(line, "\n")
	return line, true, nil
}

func initBufferedReader(file string, bufferSize int, chunkSize int, fn func(line string) (interface{}, bool, error)) (*BufferedReader, error) {
	if bufferSize < 0 {
		bufferSize = 0
	}
	if chunkSize < 1 {
		chunkSize = 1
	}

	reader := new(BufferedReader)
	fh, err := xopen.Ropen(file)
	if err != nil {
		return nil, err
	}
	reader.reader = fh

	reader.BufferSize = bufferSize
	reader.ChunkSize = chunkSize
	reader.ProcessFunc = fn
	reader.Ch = make(chan Chunk, bufferSize)
	reader.done = make(chan struct{})

	reader.finished = false
	reader.cancelled = false
	return reader, nil
}

// ErrorCanceled means that the reading process is canceled
var ErrorCanceled = errors.New("reading canceled")

func (reader *BufferedReader) run() {
	go func() {
		var (
			i    int
			line string
			err  error
		)
		chunkData := make([]interface{}, reader.ChunkSize)
		for {
			select {
			case <-reader.done:
				if !reader.finished {
					reader.Ch <- Chunk{chunkData[0:i], ErrorCanceled}
					close(reader.Ch)
					reader.finished = true
					reader.reader.Close()
					return
				}
			default:

			}

			line, err = reader.reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					result, ok, err := reader.ProcessFunc(line)
					if err != nil {
						reader.Ch <- Chunk{chunkData[0:i], err}
						close(reader.Ch)
						reader.finished = true
						reader.reader.Close()
						return
					}
					if ok {
						chunkData[i] = result
						i++
					}

					reader.Ch <- Chunk{chunkData[0:i], nil}
					close(reader.Ch)
					reader.finished = true
					reader.reader.Close()
					return
				}
				reader.Ch <- Chunk{chunkData[0:i], err}
				close(reader.Ch)
				reader.finished = true
				reader.reader.Close()
				return
			}

			result, ok, err := reader.ProcessFunc(line)
			if err != nil {
				reader.Ch <- Chunk{chunkData[0:i], err}
				close(reader.Ch)
				reader.finished = true
				reader.reader.Close()
				return
			}
			if ok {
				chunkData[i] = result
				i++
			}
			if i == reader.ChunkSize {
				reader.Ch <- Chunk{chunkData[0:i], nil}
				chunkData = make([]interface{}, reader.ChunkSize)
				i = 0
			}
		}
	}()
}

// Cancel method cancel the reading process
func (reader *BufferedReader) Cancel() {
	if !reader.finished && !reader.cancelled {
		close(reader.done)
		reader.cancelled = true
	}
}
