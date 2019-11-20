package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// 1 стадия в пайплайне: получает файл, читает его построчно, отправляет в стадию 2 прочтенную строку
func readLines(path string, fileRead chan<- struct{}) <-chan *candle {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	ch := make(chan *candle)

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			splitData := strings.Split(line, ",")
			ticker := splitData[0]
			price, err := strconv.ParseFloat(splitData[1], 32)
			if err != nil {
				continue
			}
			amount, err := strconv.ParseInt(splitData[2], 10, 32)
			if err != nil {
				continue
			}

			timeLayout := "2006-01-02 15:04:05.000000"
			timestamp, err := time.Parse(timeLayout, splitData[3])
			if err != nil {
				continue
			}

			ch <- &candle{
				ticker:    ticker,
				price:     float32(price),
				amount:    int32(amount),
				timestamp: timestamp,
			}
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		close(ch)
		file.Close()
		fileRead <- struct{}{}
	}()

	return ch
}

// 2 стадия в пайплайне: построение свеч 5m, 30m, 240m
func buildCandles(candles <-chan *candle) <-chan builtCandle {
	ch := make(chan builtCandle)

	go func() {
		for candle := range candles {
			if !isTimestampValid(candle.timestamp) {
				continue
			}

			candleCache.registerTicker(
				candle.ticker,
				timeframe5m,
				candle.price,
				candle.timestamp,
			)
			candleCache.registerTicker(
				candle.ticker,
				timeframe20m,
				candle.price,
				candle.timestamp,
			)
			candleCache.registerTicker(
				candle.ticker,
				timeframe240m,
				candle.price,
				candle.timestamp,
			)

			// 5m
			if int(candle.timestamp.Sub(candleCache.get(candle.ticker, timeframe5m).timestamp).Minutes()) == 5 {
				ch <- getBuiltCandleCopy(candleCache.get(candle.ticker, timeframe5m))
				candleCache.setTimestamp(candle.ticker, timeframe5m, candle.timestamp)
				candleCache.setStartPrice(candle.ticker, timeframe5m, candle.price)
			}
			// 30m
			if int(candle.timestamp.Sub(candleCache.get(candle.ticker, timeframe20m).timestamp).Minutes()) == 30 {
				ch <- getBuiltCandleCopy(candleCache.get(candle.ticker, timeframe20m))
				candleCache.setTimestamp(candle.ticker, timeframe20m, candle.timestamp)
				candleCache.setStartPrice(candle.ticker, timeframe20m, candle.price)
			}
			// 240m
			if int(candle.timestamp.Sub(candleCache.get(candle.ticker, timeframe240m).timestamp).Hours()) == 4 {
				ch <- getBuiltCandleCopy(candleCache.get(candle.ticker, timeframe240m))
				candleCache.setTimestamp(candle.ticker, timeframe240m, candle.timestamp)
				candleCache.setStartPrice(candle.ticker, timeframe240m, candle.price)
			}

			candleCache.setPrice(candle.ticker, timeframe5m, candle.price)
			candleCache.setPrice(candle.ticker, timeframe20m, candle.price)
			candleCache.setPrice(candle.ticker, timeframe240m, candle.price)
		}
		close(ch)
	}()

	return ch
}

// 3 стадия в пайплайне: сохранение полученной свечи в файл сразу при получении
func saveCandles(candles <-chan builtCandle, done chan<- struct{}, fileRead <-chan struct{}) {
	f1, err := os.OpenFile(outFile5m, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	f2, err := os.OpenFile(outFile20m, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	f3, err := os.OpenFile(outFile240m, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case candle, ok := <-candles:
				if !ok {
					break
				}

				switch candle.timeframe {
				case timeframe5m:
					if _, err := f1.WriteString(getCandleString(candle) + "\n"); err != nil {
						log.Fatal(err)
					}
				case timeframe20m:
					if _, err := f2.WriteString(getCandleString(candle) + "\n"); err != nil {
						log.Fatal(err)
					}
				case timeframe240m:
					if _, err := f3.WriteString(getCandleString(candle) + "\n"); err != nil {
						log.Fatal(err)
					}
				}

			case <-fileRead:
				f1.Close()
				f2.Close()
				f3.Close()
				done <- struct{}{}
				return
			}
		}

	}()
}
