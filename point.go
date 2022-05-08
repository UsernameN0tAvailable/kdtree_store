/**
point.go
Author: Tobias Famos & Mattia Pedrazzi
*/

package main

import (
	"errors"
	"math"
)

type Key = []uint64

// would have been nice
// to have generics for
// slice sizes ...
type Point struct {
	coords Key
}

func NewPoint(c Key) Point {
	return Point{coords: c}
}

func (p *Point) GetSize() int {
	return len(p.coords)
}

func (p *Point) GetKeyAt(i int) (error, uint64) {
	if i >= p.GetSize() || i < 0 {
		return errors.New("key index out of range"), 0
	}

	return nil, p.coords[i]
}

func (p *Point) GetDistance(p_1 *Point) (error, float64) {

	if p.GetSize() != p_1.GetSize() {
		return errors.New("Points have different sizes"), 0.0
	}

	deltaSum := 0.0

	for i, k := range p.coords {
		_, p1k := p_1.GetKeyAt(i)
		tmp := k - p1k
		deltaSum += float64(tmp * tmp)
	}

	return nil, math.Sqrt(deltaSum)
}
