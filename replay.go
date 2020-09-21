package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/exchangedataset/exdgo"
)

const (
	fieldExchange  = "line_exchange"
	fieldType      = "line_type"
	fieldTimestamp = "line_timestamp"
	fieldChannel   = "line_channel"
)

// Formatter formats lines.
type Formatter interface {
	// WriteTo write a formatted line to `sb`.
	WriteTo(sb *bytes.Buffer, values map[string]interface{}) error
}

type formatterCSV struct {
	// List of columns that should be included in order.
	fields []string
}

func (f *formatterCSV) WriteTo(buf *bytes.Buffer, values map[string]interface{}) error {
	for i, key := range f.fields {
		if value, ok := values[key]; ok {
			switch value.(type) {
			case string:
				// strings.Builder write functions always return a nil error
				buf.WriteString(value.(string))
			case float64:
				str := strconv.FormatFloat(value.(float64), 'f', 10, 64)
				buf.WriteString(str)
			default:
				return errors.New("csv WriteTo: type of value not supported")
			}
		}
		if i != len(f.fields)-1 {
			buf.WriteRune(',')
		}
	}
	return nil
}

func newFormatterCSV(fields []string) *formatterCSV {
	f := new(formatterCSV)
	f.fields = fields
	return f
}

type formatterJSON struct {
	filter map[string]bool
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
	return nil
}

func newFormatterJSON(fields []string) *formatterJSON {
	f := new(formatterJSON)
	if fields != nil {
		f.filter = make(map[string]bool)
		for _, field := range fields {
			f.filter[field] = true
		}
	}
	return f
}

func makeReplayRequestParameter(optFilter *string, optStart *string, optEnd *string) (rrp exdgo.ReplayRequestParam, err error) {
	if *optFilter == "" {
		err = errors.New("--filter must be specified")
		return
	}
	filter := make(map[string][]string)
	err = json.Unmarshal([]byte(*optFilter), &filter)
	if err != nil {
		err = fmt.Errorf("--filter is not in JSON: %v", err)
		return
	}
	rrp.Filter = filter

	if *optStart == "" {
		err = errors.New("--start must be specified")
		return
	}
	start, serr := convertDatetimeParam(*optStart)
	if serr != nil {
		err = fmt.Errorf("--start: %v", serr)
		return
	}
	rrp.Start = start
	if *optEnd == "" {
		err = errors.New("--end must be specified")
		return
	}
	end, serr := convertDatetimeParam(*optEnd)
	if serr != nil {
		err = fmt.Errorf("--end: %v", end)
		return
	}
	rrp.End = end

	return
}

func subCmdReplay(args []string) (err error) {
	// Define command option/flags
	flg := flag.NewFlagSet("replay", flag.ExitOnError)
	optFilter := flg.String("filter", "", "JSON. Set names of target exchanges and its channels to filter-in.")
	optStart := flg.String("start", "", "Datetime. Set a start datetime of the stream.")
	optEnd := flg.String("end", "", "Datetime. Set a end datetime of the stream.")
	optFormat := flg.String("format", "", "Optinal. String. Set the output format. 'json', 'csv' are supported. Default is 'json'.")
	optFields := flg.String("fields", "", "Optional. Set the field to be included.")
	optProgress := flg.Bool("progress", false, "Optional. Show progress in stderr. Default is false.")
	// Parse command flag/options
	err = flg.Parse(args)
	if err != nil {
		return
	}
	// Load config
	err = initConfig()
	if err != nil {
		return
	}
	// Load flags
	var fields []string
	if *optFields != "" {
		fields = strings.Split(*optFields, ",")
	}
	var formatter Formatter
	switch *optFormat {
	default:
		formatter = newFormatterJSON(fields)
	case "csv":
		formatter = newFormatterCSV(fields)
	}
	// Setup FilterParam from flags/options
	rrp, serr := makeReplayRequestParameter(optFilter, optStart, optEnd)
	if serr != nil {
		err = serr
		return
	}
	// Call API
	// From here, error will be prefixed
	defer func() {
		if err != nil {
			err = fmt.Errorf("Error occurred: %v", err)
		}
	}()
	cp := makeClientParam()
	// Get a request instance
	req, err := exdgo.Replay(cp, rrp)
	if err != nil {
		return
	}
	// Get an iterator
	var itr exdgo.StructLineIterator
	itr, err = req.Stream()
	if err != nil {
		return
	}
	defer func() {
		serr := itr.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	// Print loop
	// Buffer to store a line before output
	bufSlice := make([]byte, 0, 100000)
	buf := bytes.NewBuffer(bufSlice)
	// Used for showing progress
	startTime := time.Now()
	lastProgTime := startTime
	pi := 0
	for {
		line, ok, serr := itr.Next()
		if !ok {
			err = serr
			break
		}
		// Prepare values map which contains all fields and its values to be formatted
		var values map[string]interface{}
		if line.Type == exdgo.LineTypeMessage {
			values = line.Message.(map[string]interface{})
		} else {
			values = make(map[string]interface{})
		}
		values[fieldExchange] = line.Exchange
		var typ string
		switch line.Type {
		case exdgo.LineTypeStart:
			typ = "start"
		case exdgo.LineTypeEnd:
			typ = "end"
		case exdgo.LineTypeError:
			typ = "error"
		case exdgo.LineTypeMessage:
			typ = "message"
		case exdgo.LineTypeSend:
			typ = "send"
		default:
			typ = "!unknown"
		}
		values[fieldType] = typ
		timestamp := strconv.FormatInt(line.Timestamp, 10)
		values[fieldTimestamp] = timestamp
		// Channel might not be present
		if line.Channel != nil {
			values[fieldChannel] = *line.Channel
		}
		err = formatter.WriteTo(buf, values)
		if err != nil {
			return
		}
		buf.WriteRune('\n')
		_, err = os.Stdout.Write(buf.Bytes())
		if err != nil {
			return
		}
		buf.Reset()
		if *optProgress {
			// Show progress
			now := time.Now()
			if pi%10000 == 0 && now.Sub(lastProgTime) >= time.Second {
				lineTime := time.Unix(line.Timestamp/int64(time.Second), line.Timestamp%int64(time.Second))
				perc := float64(lineTime.Sub(rrp.Start)) / float64(rrp.End.Sub(rrp.Start))
				elapsed := now.Sub(startTime)
				estimate := time.Duration(float64(elapsed)/perc) - elapsed
				fmt.Fprintf(os.Stderr, "\r%f%% Elapsed: %v Estimate: %v", 100*perc, elapsed, estimate)
				lastProgTime = now
			}
			pi++
		}
	}
	if *optProgress {
		// This will erase estimate line
		// Ignore error
		os.Stderr.Write([]byte{'\n'})
	}
	return
}
