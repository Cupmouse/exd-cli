package main

import (
	"strings"

	"github.com/exchangedataset/exdgo"

	"errors"
	"flag"
	"fmt"
	"os"
)

func subCmdHTTPFilter(set *flag.FlagSet) (err error) {
	optExchange := set.String("exchange", "", "String. Set a name of target exchange.")
	optChannels := set.String("channels", "", "String. Set channels to filter-in, separated by ',' (comma) ex: orderBookL2,trade")
	optMinute := set.String("minute", "", "Datetime. Set a target minute. A fraction less than a minute will be floored and ignored.")
	optStart := set.String("start", "", "Optional. Datetime. Set a start datetime for filtering.")
	optEnd := set.String("end", "", "Optinal. Datetime. Set a end datetime for filtering.")
	optFormat := set.String("format", "", "Optinal. String. Set a format of response. Server chooses the format if not specified.")
	optPostFilter := set.String("post-filter", "", "Optional. String. Set channels to filter-in after messages had been formatted, separated by ','. Disabled if not specified.")
	err = set.Parse(os.Args[3:])
	if err != nil {
		return
	}

	// Load config
	err = initConfig()
	if err != nil {
		return
	}

	// Setup FilterParam from flags/options
	var fp exdgo.FilterParam

	if *optExchange == "" {
		return errors.New("--exchange must be specified")
	}
	fp.Exchange = *optExchange

	if *optChannels == "" {
		return errors.New("at least one channel have to be specified using --channels")
	}
	channels := strings.Split(*optChannels, ",")
	fp.Channels = channels

	if *optMinute == "" {
		return errors.New("--minute must be specified")
	}
	minute, serr := convertDatetimeParam(*optMinute)
	if serr != nil {
		return fmt.Errorf("--minute: %v", serr)
	}
	fp.Minute = minute

	// Optional parameters
	if *optStart != "" {
		start, serr := convertDatetimeParam(*optStart)
		if serr != nil {
			return fmt.Errorf("--start: %v", serr)
		}
		fp.Start = &start
	}
	if *optEnd != "" {
		end, serr := convertDatetimeParam(*optEnd)
		if serr != nil {
			return fmt.Errorf("--end: %v", end)
		}
		fp.End = &end
	}
	if *optFormat != "" {
		fp.Format = optFormat
	}
	if *optPostFilter != "" {
		postFilter := strings.Split(*optPostFilter, ",")
		fp.PostFilter = postFilter
	}

	// Call API
	defer func() {
		if err != nil {
			err = fmt.Errorf("Error occurred: %v", err)
		}
	}()
	cp := makeClientParam()
	results, err := exdgo.HTTPFilter(cp, fp)
	if err != nil {
		return
	}
	for i, f := range results {
		if _, err = os.Stdout.Write(f.Exchange); err != nil {

		}
	}
	return nil
}

func subCmdHTTPSnapshot() error {
	return errors.New("not implemented")
}
