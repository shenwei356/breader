/*Package breader (Buffered File Reader), asynchronous parsing and pre-processing while
 reading file. Safe cancellation is also supported

Examples:

1). Simple example with default parameters (`ChunkSize`: 1000000;
    `BufferSize`: #. of CPUs, `ProcessFunc`: trimming new-line symbol)

	import "github.com/shenwei356/breader"

	reader, err := breader.NewDefaultBufferedReader(file)
	checkErr(err)
	
	fmt.Println(chunk.ID) // useful for keeping the order of chunk in downstream process
	
	for chunk := range reader.Ch {
	    checkError(chunk.Err)
	    for _, data := range chunk.Data {
	        line := data.(string)
	        fmt.Println(line)
	    }
	}

2). Example with custom pre-processing function: splitting line to slice.
    **Note the processing of interface{} containing slice**.

	fn := func(line string) (interface{}, bool, error) {
	    line = strings.TrimRight(line, "\n")
	    if line == "" || line[0] == '#' { // ignoring blank line and comment line
	        return "", false, nil
	    }
	    items := strings.Split(line, "\t")
	    if len(items) != 2 {
	        return items, false, nil
	    }
	    return items, true, nil
	}

	reader, err := breader.NewBufferedReader(file, runtime.NumCPU(), 1000000, fn)
	checkErr(err)

	for chunk := range reader.Ch {
	    checkError(chunk.Err)

	    for _, data := range chunk.Data {
	        // do not simply use: data.(slice)
	        switch reflect.TypeOf(data).Kind() {
	        case reflect.Slice:
	            s := reflect.ValueOf(data)
	            items := make([]string, s.Len())
	            for i := 0; i < s.Len(); i++ {
	                items[i] = s.Index(i).String()
	            }
	            fmt.Println(items) // handle of slice
	        }
	    }
	}


3). Example with custom pre-processing function: creating object from line data.


	type string2int struct {
	    id    string
	    value int
	}

	fn := func(line string) (interface{}, bool, error) {
	    line = strings.TrimRight(line, "\n")
	    if line == "" || line[0] == '#' {
	        return nil, false, nil
	    }
	    items := strings.Split(line, "\t")
	    if len(items) != 2 {
	        return nil, false, nil
	    }
	    if items[0] == "" || items[1] == "" {
	        return nil, false, nil
	    }
	    id := items[0]
	    value, err := strconv.Atoi(items[1])
	    if err != nil {
	        return nil, false, err
	    }
	    return string2int{id, value}, true, nil
	}


	reader, err := breader.NewBufferedReader(file, runtime.NumCPU(), 1000000, fn)
	checkErr(err)

	for chunk := range reader.Ch {
	    checkError(chunk.Err)

	    for _, data := range chunk.Data {
	        obj := data.(string2int)
	        // handle of the string2int object
	    }
	}


4). Example of cancellation. **Note that `range chanel` is buffered, therefore,
`for-select-case` is used.**


	reader, err := breader.NewBufferedReader(testfile, 0, 1, breader.DefaultFunc)
	checkErr(err)

	// note that range is bufferd. using range will be failed
	// for chunk := range reader.Ch {
	for {
	    select {
	    case chunk := <-reader.Ch:
	        checkError(chunk.Err)

	        // do some thing

	        reader.Cancel()
	    default:
	    }
	}

*/
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
	ID   uint64 // useful for keeping the order of chunk in downstream process
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
			id   uint64
			line string
			err  error
		)
		chunkData := make([]interface{}, reader.ChunkSize)
		for {
			select {
			case <-reader.done:
				if !reader.finished {
					reader.Ch <- Chunk{id, chunkData[0:i], ErrorCanceled}
					id++
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
						reader.Ch <- Chunk{id, chunkData[0:i], err}
						id++
						close(reader.Ch)
						reader.finished = true
						reader.reader.Close()
						return
					}
					if ok {
						chunkData[i] = result
						i++
					}

					reader.Ch <- Chunk{id, chunkData[0:i], nil}
					id++
					close(reader.Ch)
					reader.finished = true
					reader.reader.Close()
					return
				}
				reader.Ch <- Chunk{id, chunkData[0:i], err}
				id++
				close(reader.Ch)
				reader.finished = true
				reader.reader.Close()
				return
			}

			result, ok, err := reader.ProcessFunc(line)
			if err != nil {
				reader.Ch <- Chunk{id, chunkData[0:i], err}
				id++
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
				reader.Ch <- Chunk{id, chunkData[0:i], nil}
				id++
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
