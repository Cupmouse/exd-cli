package main

import (
	"fmt"
	"os"
)

func main() {
	// configureSubCmd := flag.NewFlagSet("configure", flag.ExitOnError)

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "subcommand is missing")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "http":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "subcommand is missing")
			os.Exit(1)
		}
		switch os.Args[2] {
		case "filter":
		case "snapshot":
		}
	case "raw":
	case "replay":
	case "configure":
		serr := subCmdConfigure()
		if serr != nil {
			fmt.Fprintf(os.Stderr, "Error occurred: %v\n", serr)
			os.Exit(1)
		}
	}
}
