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
	optFields := flg.String("fields", "", "Optional for 'json', required for 'csv'. Set the field to be included.")
	optProgress := flg.Bool("progress", false, "Optional. Show progress in stderr. Default is false.")
	optOnlyMsg := flg.Bool("only-msg", false, "Optional. Print only message type lines. Default is false.")
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
	} else if *optFormat == "csv" {
		return errors.New("--fields must be set if 'csv' format is specified")
	}
	progress := *optProgress
	onlyMsg := *optOnlyMsg
	var formatter Formatter
	switch *optFormat {
	case "":
		formatter = newFormatterJSON(fields)
	case "json":
		formatter = newFormatterJSON(fields)
	case "csv":
		formatter = newFormatterCSV(fields)
	default:
		return fmt.Errorf("--format: '%v' not supported", *optFormat)
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
		if onlyMsg && line.Type != exdgo.LineTypeMessage {
			continue
		}
		// Prepare values map which contains all fields and its values to be formatted
		var values map[string]interface{}
		if line.Type == exdgo.LineTypeMessage {
			values = line.Message.(map[string]interface{})
		} else {
			values = make(map[string]interface{})
		}
		values[fieldExchange] = line.Exchange
		values[fieldType] = line.Type
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
		_, err = os.Stdout.Write(buf.Bytes())
		if err != nil {
			return
		}
		buf.Reset()
		if progress {
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
	if progress {
		// This will erase estimate line
		// Ignore error
		os.Stderr.Write([]byte{'\n'})
	}
	return
}
