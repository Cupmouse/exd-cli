package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Converts `Datetime` type parameter to time.Time struct.
func convertDatetimeParam(str string) (time.Time, error) {
	unixNanoTime, serr := strconv.ParseInt(str, 10, 64)
	if serr == nil {
		return time.Unix(unixNanoTime/int64(time.Nanosecond), unixNanoTime%int64(time.Nanosecond)), nil
	}
	rfc3339NanoTime, serr := time.Parse(time.RFC3339Nano, str)
	if serr != nil {
		return time.Time{}, fmt.Errorf("convertDatetimeParam: %v", serr)
	}
	return rfc3339NanoTime, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Subcommands of %v\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "  configure\tConfigure API-key and other credentials.")
		fmt.Fprintln(flag.CommandLine.Output(), "  replay\tReplay historical data.")
		fmt.Fprintln(flag.CommandLine.Output(), "  rapid\tHigh speed dump for a single channel.")
	}
	// Shows the usage if help flag is provided
	flag.Parse()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "subcommand is missing")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "configure":
		// Handle configure subcommand
		err := subCmdConfigure(os.Args[2:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "replay":
		// Handle replay command
		err := subCmdReplay(os.Args[2:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "rapid":
		err := subCmdRapid(os.Args[2:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand '%v'\n", os.Args[1])
		os.Exit(1)
	}
	os.Exit(0)
}
