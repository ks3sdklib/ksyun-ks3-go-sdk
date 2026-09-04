package ks3

import (
	"math"
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

type Ks3ErrorSuite struct{}

var _ = Suite(&Ks3ErrorSuite{})

func (s *Ks3ErrorSuite) TestCheckCRCHasCRCInResp(c *C) {
	headers := http.Header{
		"Expires":              {"-1"},
		"Content-Length":       {"0"},
		"Content-Encoding":     {"gzip"},
		"X-Kss-Hash-Crc64ecma": {"0"},
	}

	resp := &Response{
		StatusCode: 200,
		Headers:    headers,
		Body:       nil,
		ClientCRC:  math.MaxUint64,
		ServerCRC:  math.MaxUint64,
	}

	err := CheckCRC(resp, "test")
	c.Assert(err, IsNil)
}

func (s *Ks3ErrorSuite) TestCheckCRCNotHasCRCInResp(c *C) {
	headers := http.Header{
		"Expires":          {"-1"},
		"Content-Length":   {"0"},
		"Content-Encoding": {"gzip"},
	}

	resp := &Response{
		StatusCode: 200,
		Headers:    headers,
		Body:       nil,
		ClientCRC:  math.MaxUint64,
		ServerCRC:  math.MaxUint32,
	}

	err := CheckCRC(resp, "test")
	c.Assert(err, IsNil)
}

func (s *Ks3ErrorSuite) TestCheckCRCCNegative(c *C) {
	headers := http.Header{
		"Expires":                  {"-1"},
		"Content-Length":           {"0"},
		"Content-Encoding":         {"gzip"},
		"X-Kss-Checksum-Crc64ecma": {"0"},
	}

	resp := &Response{
		StatusCode: 200,
		Headers:    headers,
		Body:       nil,
		ClientCRC:  0,
		ServerCRC:  math.MaxUint64,
	}

	err := CheckCRC(resp, "test")
	c.Assert(err, NotNil)
	testLogger.Println("error:", err)
}

func (s *Ks3ErrorSuite) TestCheckDownloadCRC(c *C) {
	err := CheckDownloadCRC(0xFBF9D9603A6FA020, 0xFBF9D9603A6FA020)
	c.Assert(err, IsNil)

	err = CheckDownloadCRC(0, 0)
	c.Assert(err, IsNil)

	err = CheckDownloadCRC(0xDB6EFFF26AA94946, 0)
	c.Assert(err, NotNil)
	testLogger.Println("error:", err)
}

func (s *Ks3ErrorSuite) TestServiceErrorEndPoint(c *C) {
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
	<Error>
	  <Code>AccessDenied</Code>
	  <Message>The bucket you visit is not belong to you.</Message>
	  <RequestId>5C1B5E9BD79A6B9B6466166E</RequestId>
	  <HostId>ks3-c-sdk-test-verify-b.ks3-cn-shenzhen.ksyuncs.com</HostId>
	</Error>`
	serverError, _ := serviceErrFromXML([]byte(xmlBody), 403, "5C1B5E9BD79A6B9B6466166E")
	errMsg := serverError.Error()
	c.Assert(strings.Contains(errMsg, "Endpoint="), Equals, false)

	xmlBodyWithEndPoint := `<?xml version="1.0" encoding="UTF-8"?>
	<Error>
      <Code>AccessDenied</Code>
	  <Message>The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.</Message>
	  <RequestId>5C1B595ED51820B569C6A12F</RequestId>
	  <HostId>hello-hangzws.ks3-cn-qingdao.ksyuncs.com</HostId>
	  <Bucket>hello-hangzws</Bucket>
	  <Endpoint>ks3-cn-shenzhen.ksyuncs.com</Endpoint>
	</Error>`
	serverError, _ = serviceErrFromXML([]byte(xmlBodyWithEndPoint), 406, "5C1B595ED51820B569C6A12F")
	errMsg = serverError.Error()
	c.Assert(strings.Contains(errMsg, "Endpoint=ks3-cn-shenzhen.ksyuncs.com"), Equals, true)
}

// TestServiceErrorHTML 校验 HTML 错误页（如网关限流 429）解析为 ServiceError，
// Code 取 http.StatusText，Message 取 <title>。
func (s *Ks3ErrorSuite) TestServiceErrorHTML(c *C) {
	htmlBody := `<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html>
<head><title>429 Too Many Requests</title></head>
<body>
<center><h1>429 Too Many Requests</h1></center>
<hr><center>tengine</center>
</body>
</html>`

	// serviceErrFromHTML：Code 取状态码文本 "Too Many Requests"，Message 取 <title>
	se := serviceErrFromHTML([]byte(htmlBody), 429, "req-html-123")
	c.Assert(se.StatusCode, Equals, 429)
	c.Assert(se.Code, Equals, "Too Many Requests")
	c.Assert(se.Message, Equals, "429 Too Many Requests")
	c.Assert(se.RequestID, Equals, "req-html-123")
	c.Assert(strings.Contains(se.RawMessage, "429 Too Many Requests"), Equals, true)
	// Error() 输出含 ErrorCode=Too Many Requests
	errMsg := se.Error()
	c.Assert(strings.Contains(errMsg, "StatusCode=429"), Equals, true)
	c.Assert(strings.Contains(errMsg, "ErrorCode=Too Many Requests"), Equals, true)
	c.Assert(strings.Contains(errMsg, "429 Too Many Requests"), Equals, true)
	c.Assert(strings.Contains(errMsg, "RequestId=req-html-123"), Equals, true)

	// serviceErrFromBody：Content-Type 含 html 走 HTML 分支
	se2, ok := serviceErrFromBody([]byte(htmlBody), 429, "req-html-456", "text/html")
	c.Assert(ok, Equals, true)
	c.Assert(se2.Message, Equals, "429 Too Many Requests")
	c.Assert(se2.RequestID, Equals, "req-html-456")

	// Content-Type 非 html 走 XML 分支（HTML body 不是合法 XML → ok=false 兜底）
	_, ok = serviceErrFromBody([]byte(htmlBody), 429, "req-html", "application/xml")
	c.Assert(ok, Equals, false)

	// 无 <title> 时兜底：Code 和 Message 都用状态码文本
	noTitle := []byte(`<html><body>error</body></html>`)
	se3 := serviceErrFromHTML(noTitle, 502, "req-502")
	c.Assert(se3.Code, Equals, "Bad Gateway")
	c.Assert(se3.Message, Equals, "Bad Gateway")
	c.Assert(se3.StatusCode, Equals, 502)
}
