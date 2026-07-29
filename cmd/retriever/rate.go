package main

import "time"

func commandPerSecond(count int64, elapsed time.Duration) float64 {
	if count <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}
