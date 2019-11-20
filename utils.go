package main

import (
	"fmt"
	"time"
)

func getCandleString(bc builtCandle) string {
	return fmt.Sprintf(
		"%s,%s,%.2f,%.2f,%.2f,%.2f",
		bc.ticker,
		bc.timestamp.Format("2006-01-02T15:04:05Z"),
		bc.startPrice,
		bc.maxPrice,
		bc.minPrice,
		bc.endPrice,
	)
}

// проверка что сделка была совершена между 10 утра и 3 ночи следующего дня
func isTimestampValid(timestamp time.Time) bool {
	nextDay := timestamp.AddDate(0, 0, 1)

	after, _ := time.Parse("2006-01-02 15:04", fmt.Sprintf("%d-%02d-%02d %02d:%02d", timestamp.Year(), timestamp.Month(), timestamp.Day(), 10, 0))
	before, _ := time.Parse("2006-01-02 15:04", fmt.Sprintf("%d-%02d-%02d %02d:%02d", nextDay.Year(), nextDay.Month(), nextDay.Day(), 3, 0))

	return timestamp.After(after) && timestamp.Before(before)
}

func getBuiltCandleCopy(bc *builtCandle) builtCandle {
	return builtCandle{
		ticker:     bc.ticker,
		timestamp:  bc.timestamp,
		startPrice: bc.startPrice,
		maxPrice:   bc.maxPrice,
		minPrice:   bc.minPrice,
		endPrice:   bc.endPrice,
		timeframe:  bc.timeframe,
	}
}
