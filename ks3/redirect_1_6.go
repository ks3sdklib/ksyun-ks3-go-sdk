// +build !go1.7

package ks3

import "net/http"

// http.ErrUseLastResponse only is defined go1.7 onward

func disableHTTPRedirect(client *http.Client) {

}
