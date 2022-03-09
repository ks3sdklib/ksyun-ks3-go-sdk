// +build !go1.7

// "golang.org/x/time/rate" is depended on golang context package  go1.7 onward
// this file is only for build,not supports limit upload speed
package ks3

import (
	"fmt"
	"io"
)

const (
	perTokenBandwidthSize int = 1024
)

type Ks3Limiter struct {
}

type LimitSpeedReader struct {
	io.ReadCloser
	reader     io.Reader
	ks3Limiter *Ks3Limiter
}

func GetKs3Limiter(uploadSpeed int) (ks3Limiter *Ks3Limiter, err error) {
	err = fmt.Errorf("rate.Limiter is not supported below version go1.7")
	return nil, err
}
