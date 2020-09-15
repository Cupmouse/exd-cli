package main

import (
	"fmt"
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
