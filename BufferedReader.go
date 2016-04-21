/*Package breader (Buffered File Reader), asynchronous parsing and pre-processing while
 reading file. Safe cancellation is also supported.

Detail: https://github.com/shenwei356/breader

*/
package breader

import (
	"errors"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/brentp/xopen"
	"github.com/cznic/sortutil"
)

// Chunk is a struct compossing with slice of data and error as status
type Chunk struct {
	ID   uint64 // useful for keeping the order of chunk in downstream process
	Data []interface{}
	Err  error
}

// chunk is chunk of lines
type linesChunk struct {
	ID   uint64 // useful for keeping the order of chunk in downstream process
	Data []string
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
	reader, err := initBufferedReader(file, runtime.NumCPU(), 1000, DefaultFunc)
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
	line = strings.TrimRight(line, "\r\n")
	return line, true, nil
}

func initBufferedReader(file string, bufferSize int, chunkSize int, fn func(line string) (interface{}, bool, error)) (*BufferedReader, error) {
	if bufferSize < 1 {
		bufferSize = 1
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
	ch2 := make(chan Chunk, reader.BufferSize)

	// receive processed chunks and return them in order
	go func() {
		var id uint64
		chunks := make(map[uint64]Chunk)

		for chunk := range ch2 {
			if chunk.Err != nil {
				reader.Ch <- chunk
				close(reader.Ch)
				return
			}
			if chunk.ID == id {
				reader.Ch <- chunk
				id++
			} else { // check bufferd result
				for true {
					if chunk1, ok := chunks[id]; ok {
						reader.Ch <- chunk1
						delete(chunks, chunk1.ID)
						id++
					} else {
						break
					}
				}
				chunks[chunk.ID] = chunk
			}
		}
		if len(chunks) > 0 {
			ids := make(sortutil.Uint64Slice, len(chunks))
			i := 0
			for id := range chunks {
				ids[i] = id
				i++
			}
			sort.Sort(ids)
			for _, id := range ids {
				chunk := chunks[id]
				reader.Ch <- chunk
			}
		}
		close(reader.Ch)
	}()

	// receive lines and process with ProcessFunc
	ch := make(chan linesChunk, reader.BufferSize)
	go func() {
		var wg sync.WaitGroup
		tokens := make(chan int, reader.BufferSize)
		var hasErr bool
		for chunk := range ch {
			tokens <- 1
			wg.Add(1)

			go func(chunk linesChunk) {
				defer func() {
					wg.Done()
					<-tokens
				}()

				var chunkData []interface{}
				for _, line := range chunk.Data {
					result, ok, err := reader.ProcessFunc(line)
					if err != nil {
						ch2 <- Chunk{chunk.ID, chunkData, err}
						close(ch2)
						hasErr = true
						return
					}
					if ok {
						chunkData = append(chunkData, result)
					}
				}
				if !hasErr {
					ch2 <- Chunk{chunk.ID, chunkData, nil}
				}
			}(chunk)
		}
		wg.Wait()
		if !hasErr {
			close(ch2)
		}
	}()

	// read lines
	go func() {
		var (
			i    int
			id   uint64
			line string
			err  error
		)
		chunkData := make([]string, reader.ChunkSize)
		for {
			select {
			case <-reader.done:
				if !reader.finished {
					reader.finished = true
					reader.reader.Close()
					close(ch)
					return
				}
			default:
			}
			line, err = reader.reader.ReadString('\n')
			if err != nil {
				chunkData[i] = line
				i++
				ch <- linesChunk{id, chunkData[0:i]}

				reader.finished = true
				reader.reader.Close()
				close(ch)
				return
			}
			chunkData[i] = line
			i++
			if i == reader.ChunkSize {
				ch <- linesChunk{id, chunkData[0:i]}
				id++
				chunkData = make([]string, reader.ChunkSize)
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
