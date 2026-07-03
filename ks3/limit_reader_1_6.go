// +build !go1.7

// "golang.org/x/time/rate" is depended on golang context package  go1.7 onward
// this file is only for build,not supports limit speed
package ks3

import (
	"fmt"
	"io"
)

type Ks3Limiter struct {
}

type LimitSpeedReader struct {
	io.ReadCloser
	reader        io.Reader
	ks3Limiter    *Ks3Limiter
	acquiredCount int
}

func GetKs3Limiter(speed int) (ks3Limiter *Ks3Limiter, err error) {
	err = fmt.Errorf("rate.Limiter is not supported below version go1.7")
	return nil, err
}
