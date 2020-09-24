package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/exchangedataset/exdgo"
)

// This is used for showing progress
type rapidDownloadStage int

const (
	rapidDownloadStateEmpty = rapidDownloadStage(iota)
	rapidDownloadStagePreparing
	rapidDownloadStageDownloading
	rapidDownloadStageProcessing
	rapidDownloadStageDone
	rapidDownloadStageWatingOthers
	rapidDownloadStageWatingBufferReturn
)

type rapidDownloadSlot struct {
	buf   *bytes.Buffer
	stage rapidDownloadStage
}

// rapidParalellResult is the data and its metadata or error during process of child worker (download worker) as the result.
type rapidDownloadResult struct {
	pos int
	err error
}

// rapidDownload downloads filter data from the server in paralell way, and is optimized for this purpose to utilize high speed internet connection
// and the resource of the host computer.
type rapidDownload struct {
	// Formatter used to format the response
	form Formatter
	// Definition of the message
	msgDef map[string]string
	// exdgo.Client used to call HTTPFilter
	c *exdgo.Client
	// This struct will be used to provide a parameter to HTTPFilter many times by modifying the `minute` value
	fp exdgo.FilterParam
	// Maximum number of how much routines will work to download data parallelly
	parallelCount int
	// Buffer of downloaded data that can not be sent out because the earlier data has not yet been avaiable
	// Has a size of parallelCount
	buffer []rapidDownloadSlot
	// Position in the buffer where data has not yet been available
	readPos int
	// Position in the buffer where the result from the next soon-be-lauched download routine will be stored
	writePos int
	// Number of routines currently running or waiting for data retrival via a channel
	running int
	// Context the manager routine will run on
	ctx       context.Context
	cancelCtx context.CancelFunc
	// Channel to which the manager routine and its download routines will send an error
	// An error will be send only once and never
	err chan error
	// The last error to be observed
	lastError error
	// Channel to which the result will be sent from the manager routine
	out chan *bytes.Buffer
	// Channel to which the caller will return `bytes.Buffer` from
	ret chan *bytes.Buffer
	// Indicates if this is closed or not
	closed bool
}

func (r *rapidDownload) rapidDownload(ctx context.Context, fp exdgo.FilterParam, slot *rapidDownloadSlot, pos int, resultCh chan rapidDownloadResult) {
	var err error
	defer func() {
		if err != nil {
			resultCh <- rapidDownloadResult{
				pos: pos,
				err: err,
			}
		}
	}()
	slot.stage = rapidDownloadStageDownloading
	lines, err := r.c.HTTPFilterWithContext(ctx, fp)
	if err != nil {
		return
	}
	slot.stage = rapidDownloadStageProcessing
	values := make(map[string]interface{})
	beforeStartLine := false
	for _, line := range lines {
		if line.Type == exdgo.LineTypeMessage {
			if beforeStartLine {
				// Skip definition
				continue
			}
			err = json.Unmarshal(line.Message, &values)
			if err != nil {
				return
			}
			for key, typ := range r.msgDef {
				if val, ok := values[key]; ok && val != nil && typ == "int" {
					values[key] = int64(val.(float64))
				}
			}
		} else if line.Type == exdgo.LineTypeStart {
			beforeStartLine = true
			continue
		}
		values[fieldExchange] = line.Exchange
		values[fieldType] = line.Type
		if line.Channel != nil {
			values[fieldChannel] = *line.Channel
		}
		values[fieldTimestamp] = line.Timestamp
		err = r.form.WriteTo(slot.buf, values)
		if err != nil {
			return
		}
		// This will be optimized by the compiler
		for key := range values {
			delete(values, key)
		}
	}
	slot.stage = rapidDownloadStageDone
	resultCh <- rapidDownloadResult{
		pos: pos,
	}
}

// manager is the function intended to be ran as a routine that manages multiple worker routine (download routine)
// and the waiting buffer where data be stored when the sequentially ealier data are not yet available.
func (r *rapidDownload) manager() {
	// This ensures the manager sends an error only once
	var err error
	defer func() {
		if err != nil {
			r.err <- fmt.Errorf("manager: %v", err)
		}
		close(r.err)
	}()
	// Close out channel first so r.err will be listened
	defer close(r.out)
	startMinute := r.fp.Start.Unix() / 60
	endMinute := (r.fp.End.Unix() - 1) / 60
	// Channel to which child routines (download routines) will use to send the result
	results := make(chan rapidDownloadResult)
	// This defer function will ensure no running goroutines will be left out before this manager routine is stopped
	defer func() {
		for r.running > 0 {
			// Ignore errors
			<-results
			r.running--
		}
	}()
	// Cancel this context to stop all child routines
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	// Initialize the waiting buffer
	r.buffer = make([]rapidDownloadSlot, r.parallelCount)
	// Initialize the buffer slots and start running download routines
	for ; startMinute+int64(r.writePos) <= endMinute && r.running < r.parallelCount; r.writePos++ {
		// Allocate memory for bytes buffer
		// This bytes buffer will be used to return the result from a download routine
		// It is also be reused for another routine throughout the lifetime of this manager routine
		bufSlice := make([]byte, 0, 10*1024*1024)
		buf := bytes.NewBuffer(bufSlice)
		slot := &r.buffer[r.writePos]
		// Set buffer slot
		*slot = rapidDownloadSlot{
			buf:   buf,
			stage: rapidDownloadStagePreparing,
		}
		r.fp.Minute = time.Unix((startMinute+int64(r.writePos))*60, 0)
		go r.rapidDownload(ctx, r.fp, slot, r.writePos, results)
		r.running++
	}
	// Fetch result from download routines as well as saving them for sending out later
	// Launch new routine when the buffer slot became empty and futher fetching is needed
	for {
		select {
		case result := <-results:
			r.running--
			if result.err != nil {
				// A routine returned error
				err = fmt.Errorf("download routine, futher errors are ignored: %v", result.err)
				return
			}
			slot := &r.buffer[result.pos%r.parallelCount]
			if slot.stage != rapidDownloadStageDone {
				err = errors.New("received job done, but the slot are not in the done stage")
				return
			}
			slot.stage = rapidDownloadStageWatingOthers
			// Send out data in the buffer
			for ; ; r.readPos++ {
				slot := &r.buffer[r.readPos%r.parallelCount]
				if slot.stage != rapidDownloadStageWatingOthers {
					// This slot is not ready
					break
				}
				// Send
				select {
				case r.out <- slot.buf:
				case <-r.ctx.Done():
					// Context cancelled
					err = r.ctx.Err()
					return
				}
				slot.stage = rapidDownloadStageWatingBufferReturn
				// Wait for the buffer to be returned
				select {
				case buf := <-r.ret:
					// Do not forget to reset buffer
					buf.Reset()
					slot.buf = buf
				case <-r.ctx.Done():
					// Context cancelled
					err = r.ctx.Err()
					return
				}
				// Should it launch a new worker routine?
				if startMinute+int64(r.writePos) <= endMinute {
					// Run new routine to fetch the next
					slot.stage = rapidDownloadStagePreparing
					r.fp.Minute = time.Unix((startMinute+int64(r.writePos))*60, 0)
					go r.rapidDownload(ctx, r.fp, slot, r.writePos, results)
					r.running++
					r.writePos++
				} else {
					// Nothing to do, mark this slot as empty
					slot.buf = nil
					slot.stage = rapidDownloadStateEmpty
				}
			}
			// Did we reached the end?
			if r.readPos == r.writePos && r.running == 0 {
				return
			}
		case <-r.ctx.Done():
			// Context cancelled
			err = r.ctx.Err()
			return
		}
	}
}

func (r *rapidDownload) Get() (*bytes.Buffer, bool, error) {
	if r.closed {
		// If this has already been closed, return the last error
		return nil, false, r.lastError
	}
	select {
	case buf, ok := <-r.out:
		return buf, ok, nil
	case err, ok := <-r.err:
		if !ok {
			// r.out is also closed
			return nil, false, nil
		}
		r.lastError = fmt.Errorf("rapidDownload Get: %v", err)
		return nil, false, r.lastError
	}
}

func (r *rapidDownload) ReturnBuffer(buf *bytes.Buffer) error {
	if r.closed {
		// If this has already been closed, return the last error
		return r.lastError
	}
	select {
	case r.ret <- buf:
	case err, ok := <-r.err:
		if !ok {
			// The manager routine is dead
			return nil
		}
		r.lastError = fmt.Errorf("rapidDownload ReturnBuffer: %v", err)
		return r.lastError
	}
	return nil
}
func (r *rapidDownload) Close() error {
	if r.closed {
		// If this has already been closed, return the last error
		return r.lastError
	}
	// This stops the manager routine and its childs
	r.cancelCtx()
	if err, ok := <-r.err; ok {
		r.lastError = fmt.Errorf("rapidDownload Close: %v", err)
		return r.lastError
	}
	close(r.ret)
	// Mark this as closed
	r.closed = true
	return nil
}

// newRapidDownload makes new rapidDownload and spawns a manager routine.
func newRapidDownload(ctx context.Context, c *exdgo.Client, parallelCount int, exchange string, channel string, start time.Time, end time.Time, msgDef map[string]string, form Formatter) (r *rapidDownload) {
	r = new(rapidDownload)
	r.ctx, r.cancelCtx = context.WithCancel(ctx)
	r.c = c
	r.parallelCount = parallelCount
	format := "json"
	r.fp = exdgo.FilterParam{
		Exchange: exchange,
		Channels: []string{channel},
		Start:    &start,
		End:      &end,
		Format:   &format,
	}
	r.msgDef = msgDef
	r.form = form
	r.err = make(chan error)
	r.out = make(chan *bytes.Buffer)
	r.ret = make(chan *bytes.Buffer)
	go r.manager()
	return
}

func sortDefinitionKeys(def map[string]string) []string {
	keys := make([]string, len(def))
	i := 0
	for key := range def {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}

func subCmdRapid(args []string) (err error) {
	flg := flag.NewFlagSet("rapid", flag.ExitOnError)
	optExchange := flg.String("exchange", "", "String. Set the target exchange.")
	optChannel := flg.String("channel", "", "String. Set the target channel of the target exchange.")
	optStart := flg.String("start", "", "Datetime. Set a start datetime of the stream.")
	optEnd := flg.String("end", "", "Datetime. Set a end datetime of the stream.")
	optFormat := flg.String("format", "", "Optinal. String. Set the output format. 'json', 'csv' are supported. Default is 'json'.")
	optParalell := flg.Int("paralell", 50, "Optional. Int. Set how much filter request will be run in paralell. Higher is faster, but limited by the sequential processing and the computational power. Default is 50.")
	optFields := flg.String("fields", "", "String. Optional. List of fields to be included separated by ','.")

	err = flg.Parse(args)
	if err != nil {
		return
	}

	// Load flags/options
	if *optExchange == "" {
		return errors.New("--exchange must be set")
	}
	exchange := *optExchange
	if *optChannel == "" {
		return errors.New("--channel must be set")
	}
	channel := *optChannel
	if *optStart == "" {
		return errors.New("--start must be set")
	}
	start, serr := convertDatetimeParam(*optStart)
	if serr != nil {
		err = fmt.Errorf("--start: %v", serr)
		return
	}
	if *optEnd == "" {
		return errors.New("--end must be set")
	}
	end, serr := convertDatetimeParam(*optEnd)
	if serr != nil {
		err = fmt.Errorf("--end: %v", end)
		return
	}
	var fields []string
	if *optFields != "" {
		fields = strings.Split(*optFields, ",")
	}
	var createFormatter func([]string) Formatter
	switch *optFormat {
	case "":
		createFormatter = newFormatterJSON
	case "json":
		createFormatter = newFormatterJSON
	case "csv":
		createFormatter = newFormatterCSV
	default:
		return fmt.Errorf("--format: '%v' not supported", *optFormat)
	}
	paralellCount := *optParalell

	// Load config and make client parameter
	err = initConfig()
	if err != nil {
		return
	}
	cp := makeClientParam()
	c, serr := exdgo.CreateClient(cp)
	if serr != nil {
		return serr
	}

	format := "json"
	ssp := exdgo.SnapshotParam{
		At:       start,
		Exchange: exchange,
		Channels: []string{channel},
		Format:   &format,
	}
	// Download snapshots and the channel definition
	ss, serr := exdgo.HTTPSnapshot(cp, ssp)
	if serr != nil {
		return serr
	}
	if len(ss) == 0 {
		// This channel is not available at this timestamp
		return fmt.Errorf("channel '%s' is not available at %v", channel, start.Format(time.RFC3339))
	}
	// Unmarshal the definition
	def := make(map[string]string)
	serr = json.Unmarshal(ss[0].Snapshot, &def)
	if serr != nil {
		return fmt.Errorf("def: %v", serr)
	}
	// Extract keys (fields names) from the definition
	if fields == nil {
		fields = make([]string, len(def)+4)
		i := 0
		fields[i] = fieldExchange
		i++
		fields[i] = fieldType
		i++
		fields[i] = fieldTimestamp
		i++
		fields[i] = fieldChannel
		i++
		for _, field := range sortDefinitionKeys(def) {
			fields[i] = field
			i++
		}
	}
	// Create new formatter
	form := createFormatter(fields)
	// Prepare buffer to write lines to
	bufSlice := make([]byte, 0, 100000)
	buf := bytes.NewBuffer(bufSlice)
	// Write header
	err = form.WriteHeader(buf)
	if err != nil {
		return err
	}
	if _, err = os.Stdout.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("header: %v", err)
	}
	buf.Reset()
	// Output the rest of snapshots
	values := make(map[string]interface{})
	for i := 1; i < len(ss); i++ {
		err = json.Unmarshal(ss[i].Snapshot, &values)
		if err != nil {
			return fmt.Errorf("snapshot: %v", err)
		}
		for key, typ := range def {
			if val, ok := values[key]; ok && typ == "int" {
				values[key] = int64(val.(float64))
			}
		}
		values[fieldType] = exdgo.LineTypeMessage
		values[fieldExchange] = exchange
		values[fieldChannel] = ss[i].Channel
		values[fieldTimestamp] = ss[i].Timestamp
		err = form.WriteTo(buf, values)
		if err != nil {
			return err
		}
		// Clear values map, this will be optimized by the compiler
		for key := range values {
			delete(values, key)
		}
		if _, err = os.Stdout.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("snapshot: %v", err)
		}
		buf.Reset()
	}
	// Free memory
	buf = nil
	bufSlice = nil
	// Fetch and output in paralell
	rd := newRapidDownload(context.Background(), c, paralellCount, exchange, channel, start, end, def, form)
	defer func() {
		serr := rd.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	stopProg := make(chan struct{})
	defer close(stopProg)
	go rapidShowProgress(rd, stopProg)
	for {
		if buf, ok, serr := rd.Get(); ok {
			if _, err = os.Stdout.Write(buf.Bytes()); err != nil {
				return
			}
			if err = rd.ReturnBuffer(buf); serr != nil {
				return
			}
		} else if serr != nil {
			err = serr
			return
		} else {
			return
		}
	}
}

func rapidShowProgress(rd *rapidDownload, stop chan struct{}) {
	started := time.Now()
	tim := time.NewTicker(500 * time.Millisecond)
	defer tim.Stop()
	// var sb strings.Builder
	defer fmt.Fprint(os.Stderr, "\n")
	for {
		select {
		case <-tim.C:
			// Show buffer status
			// st := make([]rapidDownloadStage, len(rd.buffer))
			// for i, slot := range rd.buffer {
			// 	st[i] = slot.stage
			// }
			// for _, s := range st {
			// 	switch s {
			// 	case rapidDownloadStateEmpty:
			// 		sb.WriteRune(' ')
			// 	case rapidDownloadStagePreparing:
			// 		sb.WriteRune('p')
			// 	case rapidDownloadStageDownloading:
			// 		sb.WriteRune('d')
			// 	case rapidDownloadStageProcessing:
			// 		sb.WriteRune('P')
			// 	case rapidDownloadStageDone:
			// 		sb.WriteRune('d')
			// 	case rapidDownloadStageWatingOthers:
			// 		sb.WriteRune('w')
			// 	case rapidDownloadStageWatingBufferReturn:
			// 		sb.WriteRune('W')
			// 	default:
			// 		sb.WriteRune('o')
			// 	}
			// }
			// // Ignore error
			// fmt.Fprintf(os.Stderr, "\r|%s|", sb.String())
			// sb.Reset()
			// Show time estimate
			now := time.Now()
			perc := float64(rd.readPos) / float64(rd.fp.End.Sub(*rd.fp.Start)/time.Minute)
			elapsed := now.Sub(started)
			estimate := time.Duration(float64(elapsed)/perc) - elapsed
			fmt.Fprintf(os.Stderr, "\rElapsed: %s Estimate: %v", elapsed, estimate)
		case <-stop:
			return
		}
	}
}
