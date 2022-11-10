package ks3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// ServiceError contains fields of the error response from Ks3 Service REST API.
type ServiceError struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`      // The error code returned from KS3 to the caller
	Message    string   `xml:"Message"`   // The detail error message from KS3
	RequestID  string   `xml:"RequestId"` // The UUID used to uniquely identify the request
	Endpoint   string   `xml:"Endpoint"`
	RawMessage string   // The raw messages from KS3
	StatusCode int      // HTTP status code
}

// Error implements interface error
func (e ServiceError) Error() string {
	if e.Endpoint == "" {
		return fmt.Sprintf("ks3: service returned error: StatusCode=%d, ErrorCode=%s, ErrorMessage=\"%s\", RequestId=%s",
			e.StatusCode, e.Code, e.Message, e.RequestID)
	}
	return fmt.Sprintf("ks3: service returned error: StatusCode=%d, ErrorCode=%s, ErrorMessage=\"%s\", RequestId=%s, Endpoint=%s",
		e.StatusCode, e.Code, e.Message, e.RequestID, e.Endpoint)
}

// UnexpectedStatusCodeError is returned when a storage service responds with neither an error
// nor with an HTTP status code indicating success.
type UnexpectedStatusCodeError struct {
	allowed []int // The expected HTTP stats code returned from KS3
	got     int   // The actual HTTP status code from KS3
}

// Error implements interface error
func (e UnexpectedStatusCodeError) Error() string {
	s := func(i int) string { return fmt.Sprintf("%d %s", i, http.StatusText(i)) }

	got := s(e.got)
	expected := []string{}
	for _, v := range e.allowed {
		expected = append(expected, s(v))
	}
	return fmt.Sprintf("ks3: status code from service response is %s; was expecting %s",
		got, strings.Join(expected, " or "))
}

// Got is the actual status code returned by ks3.
func (e UnexpectedStatusCodeError) Got() int {
	return e.got
}

// CheckRespCode returns UnexpectedStatusError if the given response code is not
// one of the allowed status codes; otherwise nil.
func CheckRespCode(respCode int, allowed []int) error {
	for _, v := range allowed {
		if respCode == v {
			return nil
		}
	}
	return UnexpectedStatusCodeError{allowed, respCode}
}

// CRCCheckError is returned when crc check is inconsistent between client and server
type CRCCheckError struct {
	clientCRC uint64 // Calculated CRC64 in client
	serverCRC uint64 // Calculated CRC64 in server
	operation string // Upload operations such as PutObject/AppendObject/UploadPart, etc
	requestID string // The request id of this operation
}

// Error implements interface error
func (e CRCCheckError) Error() string {
	return fmt.Sprintf("ks3: the crc of %s is inconsistent, client %d but server %d; request id is %s",
		e.operation, e.clientCRC, e.serverCRC, e.requestID)
}

func CheckDownloadCRC(clientCRC, serverCRC uint64) error {
	if clientCRC == serverCRC {
		return nil
	}
	return CRCCheckError{clientCRC, serverCRC, "DownloadFile", ""}
}

func CheckCRC(resp *Response, operation string) error {
	if resp.Headers.Get(HTTPHeaderKs3CRC64) == "" || resp.ClientCRC == resp.ServerCRC {
		return nil
	}
	return CRCCheckError{resp.ClientCRC, resp.ServerCRC, operation, resp.Headers.Get(HTTPHeaderKs3RequestID)}
}
