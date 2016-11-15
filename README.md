# breader

[Godoc](http://godoc.org/github.com/shenwei356/breader)
[![Go Report Card](https://goreportcard.com/badge/github.com/shenwei356/bio)](https://goreportcard.com/report/github.com/shenwei356/bio)

breader (Buffered File Reader), asynchronous parsing and pre-processing while
 reading file. Safe cancellation is also supported.

## Example

1). Simple example with default parameters (`ChunkSize`: 100;
    `BufferSize`: #. of CPUs, `ProcessFunc`: trimming new-line symbol)

```go
import "github.com/shenwei356/breader"

reader, err := breader.NewDefaultBufferedReader(file)
checkErr(err)

for chunk := range reader.Ch {
    checkError(chunk.Err)
    for _, data := range chunk.Data {
        line := data.(string)
        fmt.Println(line)
    }
}
```

2). Example with custom pre-processing function: splitting line to slice.
    **Note the processing of interface{} containing slice,
        using a custom struct is recommended**.

```go
type Slice []string // custom type
fn := func(line string) (interface{}, bool, error) {
    line = strings.TrimRight(line, "\n")
    if line == "" || line[0] == '#' { // ignoring blank line and comment line
        return "", false, nil
    }
    items := strings.Split(line, "\t")
    if len(items) != 2 {
        return items, false, nil
    }
    return Slice(items), true, nil
}

reader, err := breader.NewBufferedReader(file, runtime.NumCPU(), 100, fn)
checkErr(err)

for chunk := range reader.Ch {
    checkError(chunk.Err)

    for _, data := range chunk.Data {
        // do not simply use: data.(slice)
        fmt.Println(data.(Slice))
    }
}

```

3). Example with custom pre-processing function: creating object from line data.

```go
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


reader, err := breader.NewBufferedReader(file, runtime.NumCPU(), 100, fn)
checkErr(err)

for chunk := range reader.Ch {
    checkError(chunk.Err)

    for _, data := range chunk.Data {
        obj := data.(string2int)
        // handle of the string2int object
    }
}
```

4). Example of cancellation. **Note that `range chanel` is buffered, therefore,
`for-select-case` is used.**

```go
reader, err := breader.NewBufferedReader(testfile, 0, 1, breader.DefaultFunc)
checkErr(err)

// note that range is bufferd. using range will be failed
// for chunk := range reader.Ch {
LOOP:
    for {
        select {
        case chunk := <-reader.Ch:
            if chunk.Err != nil {
                t.Log(chunk.Err)
                return
            }
            reader.Cancel()
            break LOOP
        default:
        }
    }

```

## License

[MIT License](https://github.com/shenwei356/breader/blob/master/LICENSE)
