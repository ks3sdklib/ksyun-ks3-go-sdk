// +build go1.7

package ks3

import (
	"io"
	"time"

	"golang.org/x/time/rate"
)

const (
	perTokenBandwidthSize = 1024
)

// Ks3Limiter wraps rate.Limiter for bandwidth control
type Ks3Limiter struct {
	limiter *rate.Limiter
}

// GetKs3Limiter creates Ks3Limiter, speedKB in KB/s
func GetKs3Limiter(speedKB int) (ks3Limiter *Ks3Limiter, err error) {
	capacity := speedKB * perTokenBandwidthSize
	if capacity < MinRateLimiterCapacity {
		capacity = MinRateLimiterCapacity
	}
	burst := capacity / perTokenBandwidthSize
	if burst < 1 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(speedKB), burst)

	// drain initial burst so the limiter behaves accurately from the start
	limiter.AllowN(time.Now(), burst)

	return &Ks3Limiter{limiter: limiter}, nil
}

// LimitSpeedReader throttles read bandwidth
type LimitSpeedReader struct {
	io.ReadCloser
	reader        io.Reader
	ks3Limiter    *Ks3Limiter
	acquiredCount int
}

// Read acquires tokens first, then reads, tracking over-acquired bytes
func (r *LimitSpeedReader) Read(p []byte) (n int, err error) {
	want := len(p)
	if want > r.acquiredCount {
		need := want - r.acquiredCount
		r.acquire(need)
		r.acquiredCount = 0
	} else {
		r.acquiredCount -= want
	}

	n, err = r.reader.Read(p)
	r.acquiredCount += want - n
	return
}

func (r *LimitSpeedReader) acquire(want int) {
	if r.ks3Limiter == nil || want <= 0 {
		return
	}
	tc := (want + perTokenBandwidthSize - 1) / perTokenBandwidthSize
	burst := r.ks3Limiter.limiter.Burst()
	if burst < 1 {
		burst = 1
	}
	for tc > 0 {
		batch := tc
		if batch > burst {
			batch = burst
		}
		rsv := r.ks3Limiter.limiter.ReserveN(time.Now(), batch)
		if !rsv.OK() {
			batch = 1
			rsv = r.ks3Limiter.limiter.ReserveN(time.Now(), batch)
		}
		time.Sleep(rsv.Delay())
		tc -= batch
	}
}

// Close ...
func (r *LimitSpeedReader) Close() error {
	rc, ok := r.reader.(io.ReadCloser)
	if ok {
		return rc.Close()
	}
	return nil
}
