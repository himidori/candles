package main

import (
	"context"
	"flag"
	"time"
)

const (
	outFile5m   = "candles_5min.csv"
	outFile20m  = "candles_20min.csv"
	outFile240m = "candles_240min.csv"
)

const (
	pipelineTimeout = time.Second * 5
)

const (
	timeframe5m   = "5m"
	timeframe20m  = "20m"
	timeframe240m = "240m"
)

var (
	filePath string
)

type candle struct {
	ticker    string
	price     float32
	amount    int32
	timestamp time.Time
}

type builtCandle struct {
	ticker     string
	timestamp  time.Time
	startPrice float32
	maxPrice   float32
	minPrice   float32
	endPrice   float32
	timeframe  string
}

var (
	candleCache = newCache()
)

func init() {
	flag.StringVar(&filePath, "file", "candles.csv", "path to candles file")
	flag.Parse()
}

func main() {
	fileRead := make(chan struct{})
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), pipelineTimeout)

	go saveCandles(buildCandles(readLines(filePath, fileRead)), done, fileRead)

	for {
		select {
		case <-done:
			return

		case <-ctx.Done():
			cancel()
			panic("pipeline took too long to finish")
		}
	}
}
