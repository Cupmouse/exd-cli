package main

import (
	"flag"
	"fmt"
	"os"
	"path"
)

func main() {
	configureSubCmd := flag.NewFlagSet("configure", flag.ExitOnError)
	configureSubCmd.Usage = func() {
		fmt.Fprintln(configureSubCmd.Output(), "Usage of configure:")
		fmt.Fprintln(configureSubCmd.Output(), "Configures Exchangedataset credentials used to access the API in interactive way.")
	}
	httpFilterSubCmd := flag.NewFlagSet("filter", flag.ExitOnError)
	httpSnapshotSubCmd := flag.NewFlagSet("snapshot", flag.ExitOnError)
	httpSubCmd := flag.NewFlagSet("http", flag.ExitOnError)
	httpSubCmd.Usage = func() {
		fmt.Fprintln(httpSubCmd.Output(), "Usage of http:")
		fmt.Fprintln(httpSubCmd.Output(), "Subcommand to directly interact with the HTTP API.")
		fmt.Fprintln(httpSubCmd.Output(), "Subcommands of http:")
		fmt.Fprintln(httpSubCmd.Output(), "  filter\tcall Filter HTTP Endpoint and displays the response.")
		fmt.Fprintln(httpSubCmd.Output(), "  snapshot\tcall Snapshot HTTP Endpoint and displays the response.")
	}
	rawSubCmd := flag.NewFlagSet("raw", flag.ExitOnError)
	replaySubCmd := flag.NewFlagSet("replay", flag.ExitOnError)
	flag.Usage = func() {
		fmt.Fprintf(rawSubCmd.Output(), "Usage of : %s\n", os.Args[0])
		fmt.Fprintln(rawSubCmd.Output(), "  raw\t\tDownload and output stream of data starting from 'start' to 'end' of multiple exchanges and channels.")
		fmt.Fprintln(rawSubCmd.Output(), "  replay\tDownload and output formatted, structured stream of data starting from 'start' to 'end' of multiple exchanges and channels.")
	}

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "subcommand is missing")
		fmt.Fprintf(os.Stderr, "Run '%s --help' for more information.\n", path.Base(os.Args[0]))
		os.Exit(1)
	}

	switch os.Args[1] {
	case "http":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "http: subcommand is missing")
			fmt.Fprintf(os.Stderr, "Run '%s http --help' for more information.\n", path.Base(os.Args[0]))
			os.Exit(1)
		}
		var serr error
		switch os.Args[2] {
		case "filter":
			serr := subCmdHTTPFilter(httpFilterSubCmd)
			if serr != nil {
				fmt.Fprintln(os.Stderr, serr)
				os.Exit(1)
			}
			os.Exit(0)
		case "snapshot":
			httpSnapshotSubCmd.Parse(os.Args[3:])
			serr = subCmdHTTPSnapshot()
			if serr != nil {
				fmt.Fprintf(os.Stderr, "Error occurred: %v\n", serr)
				os.Exit(1)
			}
		default:
			httpSubCmd.Parse(os.Args[2:])
			fmt.Fprintf(os.Stderr, "wrong subcommand: %v\n", os.Args[2])
			fmt.Fprintf(os.Stderr, "Run '%s http --help' for more information.\n", path.Base(os.Args[0]))
		}
		if serr != nil {
			fmt.Fprintf(os.Stderr, "Error occurred: %v\n", serr)
		}
	case "raw":
		rawSubCmd.Parse(os.Args[2:])
	case "replay":
		replaySubCmd.Parse(os.Args[2:])
		// serr := subCmdReplay()
		// if serr != nil {
		// 	fmt.Fprintf(os.Stderr, "Error occurred: %v\n", serr)
		// 	os.Exit(1)
		// }
	case "configure":
		configureSubCmd.Parse(os.Args[2:])
		serr := subCmdConfigure()
		if serr != nil {
			fmt.Fprintf(os.Stderr, "Error occurred: %v\n", serr)
			os.Exit(1)
		}
	default:
		// If help command is requested, parsing flag will show the help and exit with 0 status
		flag.Parse()
		fmt.Fprintf(os.Stderr, "wrong subcommand: %v\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Run '%s --help' for more information.\n", path.Base(os.Args[0]))
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "** Reached the end")
	os.Exit(1)
}
