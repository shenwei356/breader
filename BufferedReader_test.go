package breader

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

var testfile = "testdata.tsv"

func TestUnprocessedText(t *testing.T) {
	var text []string

	fn := func(line string) (interface{}, bool, error) {
		return line, true, nil
	}

	reader, err := NewBufferedReader(testfile, 2, 4, fn)
	if err != nil {
		t.Error(err)
		return
	}

	for chunk := range reader.Ch {
		if chunk.Err != nil {
			t.Error(chunk.Err)
			return
		}
		for _, data := range chunk.Data {
			text = append(text, data.(string))
		}
	}

	originalText, err := readFileText(testfile)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(text, "") != originalText {
		t.Error("text unmatch")
	}
}

func TestProcessedText(t *testing.T) {
	type Slice []string
	fn := func(line string) (interface{}, bool, error) {
		line = strings.TrimRight(line, "\n")
		if line == "" || line[0] == '#' {
			return "", false, nil
		}
		items := strings.Split(line, "\t")
		if len(items) != 2 {
			return items, false, nil
		}
		return Slice(items), true, nil
	}

	reader, err := NewBufferedReader(testfile, 2, 4, fn)
	if err != nil {
		t.Error(err)
		return
	}

	n := 0
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			t.Error(chunk.Err)
			return
		}
		// for _, data := range chunk.Data {
		// switch reflect.TypeOf(data).Kind() {
		// case reflect.Slice:
		// 	s := reflect.ValueOf(data)
		// 	items := make([]string, s.Len())
		// 	for i := 0; i < s.Len(); i++ {
		// 		items[i] = s.Index(i).String()
		// 	}
		// 	fmt.Println(items)
		// 	n++
		// }
		// fmt.Println(data.(Slice))
		// 	n++
		// }

		n += len(chunk.Data)

	}

	if n != 9 {
		t.Error("testing TestProcessedText failed")
	}
}

func TestCancellation(t *testing.T) {
	reader, err := NewBufferedReader(testfile, 1, 1, DefaultFunc)
	if err != nil {
		t.Error(err)
		return
	}

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

}

func TestProcessedTextReturnObject(t *testing.T) {
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

	reader, err := NewBufferedReader(testfile, 2, 4, fn)
	if err != nil {
		t.Error(err)
		return
	}

	n := 0
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			t.Error(chunk.Err)
			return
		}
		for _, data := range chunk.Data {
			_ = data.(string2int)
			n++
		}
	}
	if n != 7 {
		t.Error("testing TestProcessedTextReturnObject failed")
	}
}

func readFileText(file string) (string, error) {
	fh, err := os.Open(file)
	defer fh.Close()
	if err != nil {
		return "", err
	}
	bs, _ := ioutil.ReadAll(fh)
	return string(bs), nil
}
