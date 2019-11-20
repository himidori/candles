package main

import (
	"sync"
	"time"
)

type cache struct {
	mu *sync.RWMutex
	c  map[string]map[string]*builtCandle
}

func newCache() *cache {
	return &cache{
		mu: &sync.RWMutex{},
		c:  make(map[string]map[string]*builtCandle),
	}
}

func (c cache) registerTicker(name string, frame string, price float32, timestamp time.Time) {
	if c.get(name, frame) != nil {
		return
	}

	if c.c[name] == nil {
		c.c[name] = make(map[string]*builtCandle)
	}

	c.c[name][frame] = &builtCandle{
		timestamp:  timestamp,
		startPrice: price,
		minPrice:   price,
		maxPrice:   price,
		endPrice:   price,
		timeframe:  frame,
		ticker:     name,
	}
}

func (c cache) setPrice(name string, frame string, price float32) {
	bc := c.get(name, frame)

	if bc.maxPrice < price {
		bc.maxPrice = price
	}

	if bc.minPrice > price {
		bc.minPrice = price
	}

	bc.endPrice = price
}

func (c cache) setTimestamp(name string, frame string, timestamp time.Time) {
	bc := c.get(name, frame)

	bc.timestamp = timestamp
}

func (c cache) setStartPrice(name string, frame string, price float32) {
	bc := c.c[name][frame]

	bc.startPrice = price
}

func (c cache) get(name string, frame string) *builtCandle {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.c[name][frame]
}
