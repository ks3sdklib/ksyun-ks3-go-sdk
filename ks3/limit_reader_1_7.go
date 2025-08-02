// +build go1.7

package ks3

import (
	"fmt"
	"io"
	"math"
	"time"

	"golang.org/x/time/rate"
)

const (
	perTokenBandwidthSize int = 1024
)

// Ks3Limiter wrapper rate.Limiter
type Ks3Limiter struct {
	limiter *rate.Limiter
}

// GetKs3Limiter create Ks3Limiter
// uploadSpeed KB/s
func GetKs3Limiter(uploadSpeed int) (ks3Limiter *Ks3Limiter, err error) {
	limiter := rate.NewLimiter(rate.Limit(uploadSpeed), uploadSpeed)

	// first consume the initial full token,the limiter will behave more accurately
	limiter.AllowN(time.Now(), uploadSpeed)

	return &Ks3Limiter{
		limiter: limiter,
	}, nil
}

// LimitSpeedReader for limit bandwidth upload
type LimitSpeedReader struct {
	io.ReadCloser
	reader     io.Reader
	ks3Limiter *Ks3Limiter
}

// Read
func (r *LimitSpeedReader) Read(p []byte) (n int, err error) {
	n = 0
	err = nil
	start := 0
	burst := r.ks3Limiter.limiter.Burst()
	var end int
	var tmpN int
	var tc int
	for start < len(p) {
		if start+burst*perTokenBandwidthSize < len(p) {
			end = start + burst*perTokenBandwidthSize
		} else {
			end = len(p)
		}

		tmpN, err = r.reader.Read(p[start:end])
		if tmpN > 0 {
			n += tmpN
			start = n
		}

		if err != nil {
			return
		}

		tc = int(math.Ceil(float64(tmpN) / float64(perTokenBandwidthSize)))
		now := time.Now()
		re := r.ks3Limiter.limiter.ReserveN(now, tc)
		if !re.OK() {
			err = fmt.Errorf("LimitSpeedReader.Read() failure,ReserveN error,start:%d,end:%d,burst:%d,perTokenBandwidthSize:%d",
				start, end, burst, perTokenBandwidthSize)
			return
		}
		timeDelay := re.Delay()
		time.Sleep(timeDelay)
	}
	return
}

// Close ...
func (r *LimitSpeedReader) Close() error {
	rc, ok := r.reader.(io.ReadCloser)
	if ok {
		return rc.Close()
	}
	return nil
}
