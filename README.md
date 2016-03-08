# breader

breader (Buffered File Reader), asynchronous parsing and pre-processing while
 reading file. Safe cancellation is also supported.

## API reference

[Godoc](http://godoc.org/github.com/shenwei356/breader)
 
## Example

1). Simple example with default parameters (`ChunkSize`: 1000000;
    `BufferSize`: #. of CPUs, `ProcessFunc`: trimming new-line symbol)

```
import "github.com/shenwei356/breader"

reader, err := breader.NewDefaultBufferedReader(file)
checkErr(err)

for chunk := range reader.Ch {
    checkError(chunk.Err)
    
    fmt.Println(chunk.ID) // useful for keeping the order of chunk in downstream process
    
    for _, data := range chunk.Data {
        line := data.(string)
        fmt.Println(line)
    }
}
```

2). Example with custom pre-processing function: splitting line to slice.
    **Note the processing of interface{} containing slice**.

```
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

```

3). Example with custom pre-processing function: creating object from line data.

```
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
```

4). Example of cancellation. **Note that `range chanel` is buffered, therefore,
`for-select-case` is used.**

```
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
```


## License


[MIT License](https://github.com/shenwei356/breader/blob/master/LICENSE)
