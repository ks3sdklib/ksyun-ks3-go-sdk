package ks3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// ValidateMessage 向量桶参数校验失败的详细信息。
type ValidateMessage struct {
	Message string `json:"message"` // 参数校验失败原因
	Path    string `json:"path"`    // 参数字段在请求结构中的位置
}

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

// VectorServiceError 向量桶服务的错误响应（JSON 格式，与普通 KS3 的 XML 错误分离）。
// body 仅含 message 和可选 fieldList；RequestID 不在 body 中，由响应头 x-kss-request-id 填入。
type VectorServiceError struct {
	Message    string            `json:"message"`             // 错误信息
	FieldList  []ValidateMessage `json:"fieldList,omitempty"` // 参数校验失败详情（仅参数校验错误时返回）
	RequestID  string            // 请求 ID（取自响应头 x-kss-request-id）
	RawMessage string            // 原始响应体
	StatusCode int               // HTTP status code
}

// Error implements interface error
func (e VectorServiceError) Error() string {
	s := fmt.Sprintf("ks3: vector service returned error: StatusCode=%d, ErrorMessage=%q", e.StatusCode, e.Message)
	if len(e.FieldList) > 0 {
		f := e.FieldList[0]
		s += fmt.Sprintf(", FieldList[0]: message=%q, path=%q", f.Message, f.Path)
	}
	if e.RequestID != "" {
		s += fmt.Sprintf(", RequestId=%s", e.RequestID)
	}
	return s
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
