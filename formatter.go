package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/exchangedataset/exdgo"
)

// Formatter formats lines.
type Formatter interface {
	// WriteHeader write a header to `sb`.
	WriteHeader(buf *bytes.Buffer) error
	// WriteTo write a formatted line to `sb`.
	WriteTo(buf *bytes.Buffer, values map[string]interface{}) error
}

type formatterCSV struct {
	// List of columns that should be included in order.
	fields []string
}

func (f *formatterCSV) WriteHeader(buf *bytes.Buffer) error {
	for i, field := range f.fields {
		buf.WriteString(field)
		if i != len(f.fields)-1 {
			buf.WriteRune(',')
		} else {
			buf.WriteRune('\n')
		}
	}
	return nil
}

func (f *formatterCSV) WriteTo(buf *bytes.Buffer, values map[string]interface{}) error {
	for i, key := range f.fields {
		if value, ok := values[key]; ok && value != nil {
			switch value.(type) {
			case exdgo.LineType:
				buf.WriteString(string(value.(exdgo.LineType)))
			case string:
				// strings.Builder write functions always return a nil error
				buf.WriteString(value.(string))
			case float64:
				str := strconv.FormatFloat(value.(float64), 'f', 10, 64)
				buf.WriteString(str)
			case int64:
				str := strconv.FormatInt(value.(int64), 10)
				buf.WriteString(str)
			default:
				return fmt.Errorf("csv WriteTo: type of value not supported: %v", value)
			}
		}
		if i != len(f.fields)-1 {
			buf.WriteRune(',')
		} else {
			buf.WriteRune('\n')
		}
	}
	return nil
}

func newFormatterCSV(fields []string) Formatter {
	f := new(formatterCSV)
	f.fields = fields
	return f
}

type formatterJSON struct {
	filter map[string]bool
}

func (f *formatterJSON) WriteHeader(buf *bytes.Buffer) error {
	return nil
}

func (f *formatterJSON) WriteTo(buf *bytes.Buffer, values map[string]interface{}) error {
	filtered := values
	if f.filter != nil {
		filtered = make(map[string]interface{})
		for key := range f.filter {
			filtered[key] = values[key]
		}
	}
	marshaled, serr := json.Marshal(filtered)
	if serr != nil {
		return serr
	}
	buf.Write(marshaled)
	buf.WriteRune('\n')
	return nil
}

func newFormatterJSON(fields []string) Formatter {
	f := new(formatterJSON)
	if fields != nil {
		f.filter = make(map[string]bool)
		for _, field := range fields {
			f.filter[field] = true
		}
	}
	return f
}
