package ks3

import (
	"errors"
	"net/http"
)

func defaultHTTPRedirect(client *http.Client) {
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		req.Header.Add("Authorization", via[0].Header.Get("Authorization"))
		return nil
	}
}
