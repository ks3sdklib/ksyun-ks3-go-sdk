package ks3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Conn defines KS3 Conn
type Conn struct {
	config *Config
	Url    *UrlMaker
	client *http.Client
}

// init initializes Conn
func (conn *Conn) init(config *Config, urlMaker *UrlMaker, client *http.Client) error {
	if client == nil {
		// New transport
		transport := newTransport(conn, config)

		// Proxy
		if conn.config.IsUseProxy {
			if config.ProxyFromEnvironment {
				transport.Proxy = http.ProxyFromEnvironment
			} else {
				proxyURL, err := url.Parse(config.ProxyHost)
				if err != nil {
					return err
				}
				if config.IsAuthProxy {
					if config.ProxyPassword != "" {
						proxyURL.User = url.UserPassword(config.ProxyUser, config.ProxyPassword)
					} else {
						proxyURL.User = url.User(config.ProxyUser)
					}
				}
				transport.Proxy = http.ProxyURL(proxyURL)
			}
		}
		client = &http.Client{Transport: transport}
		if !config.RedirectEnabled {
			disableHTTPRedirect(client)
		} else {
			defaultHTTPRedirect(client)
		}
	}

	conn.config = config
	conn.Url = urlMaker
	conn.client = client

	return nil
}

// Do send request and returns the response
func (conn Conn) Do(method, bucketName, objectName string, params map[string]interface{}, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	return conn.DoWithContext(nil, method, bucketName, objectName, params, headers, data, initCRC, listener)
}

// DoWithContext sends request and returns the response
func (conn Conn) DoWithContext(ctx context.Context, method, bucketName, objectName string, params map[string]interface{}, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	urlParams := getURLParams(params)
	subResource := getSubResource(params)
	urltmp := encodeKS3Str(objectName)
	uri := conn.Url.getURL(bucketName, urltmp, urlParams)
	resource := getResource(bucketName, objectName, subResource)
	return conn.doRequest(ctx, method, uri, resource, headers, data, initCRC, listener)
}

// DoURL sends the request with signed URL and returns the response result.
func (conn Conn) DoURL(method HTTPMethod, signedURL string, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	return conn.DoURLWithContext(nil, method, signedURL, headers, data, initCRC, listener)
}

// DoURLWithContext sends the request with signed URL and returns the response result.
func (conn Conn) DoURLWithContext(ctx context.Context, method HTTPMethod, signedURL string, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	// Get URI from signedURL
	uri, err := url.ParseRequestURI(signedURL)
	if err != nil {
		return nil, err
	}

	m := strings.ToUpper(string(method))
	req := &http.Request{
		Method:     m,
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	tracker := &readerTracker{completedBytes: 0}
	fd, crc := conn.handleBody(req, data, initCRC, listener, tracker)
	if fd != nil {
		defer func() {
			fd.Close()
			os.Remove(fd.Name())
		}()
	}

	if conn.config.IsAuthProxy {
		auth := conn.config.ProxyUser + ":" + conn.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	req.Header.Set(HTTPHeaderHost, req.Host)
	req.Header.Set(HTTPHeaderUserAgent, conn.config.UserAgent)

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	// Transfer started
	event := newProgressEvent(TransferStartedEvent, 0, req.ContentLength, 0)
	publishProgress(listener, event)

	// print out http req
	conn.LoggerHTTPReq(req)

	startT := time.Now()
	resp, err := conn.client.Do(req)
	cost := time.Now().UnixNano()/1000/1000 - startT.UnixNano()/1000/1000
	conn.config.WriteLog(Debug, "[Resp:%p]send http request, cost:%d(ms)\n", req, cost)

	if err != nil {
		// Transfer failed
		event = newProgressEvent(TransferFailedEvent, tracker.completedBytes, req.ContentLength, 0)
		publishProgress(listener, event)
		conn.config.WriteLog(Debug, "[Resp:%p]http error:%s\n", req, err.Error())
		return nil, err
	}

	// print out http resp
	conn.LoggerHTTPResp(req, resp)

	// Transfer completed
	event = newProgressEvent(TransferCompletedEvent, tracker.completedBytes, req.ContentLength, 0)
	publishProgress(listener, event)

	return conn.handleResponse(resp, crc)
}

func getURLParams(params map[string]interface{}) string {
	// Sort
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Serialize
	var buf bytes.Buffer
	for _, k := range keys {
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(url.QueryEscape(k))
		if params[k] != nil && params[k].(string) != "" {
			buf.WriteString("=" + strings.Replace(url.QueryEscape(params[k].(string)), "+", "%20", -1))
		}
	}

	return buf.String()
}

func getSubResource(params map[string]interface{}) string {
	// Sort
	keys := make([]string, 0, len(params))
	signParams := make(map[string]string)
	for k := range params {
		if isParamSign(k) {
			keys = append(keys, k)
			if params[k] != nil {
				signParams[k] = params[k].(string)
			}
		}
	}
	sort.Strings(keys)

	// Serialize
	var buf bytes.Buffer
	for _, k := range keys {
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(k)
		if _, ok := signParams[k]; ok {
			if signParams[k] != "" {
				buf.WriteString("=" + signParams[k])
			}
		}
	}
	return buf.String()
}

// ks3 encode 字符串
func encodeKS3Str(str string) string {
	objectName := url.QueryEscape(str)
	objectName = strings.ReplaceAll(objectName, "+", "%20")
	objectName = strings.ReplaceAll(objectName, "*", "%2A")
	objectName = strings.ReplaceAll(objectName, "%7E", "~")
	objectName = strings.ReplaceAll(objectName, "%2F", "/")
	objectName = strings.ReplaceAll(objectName, "//", "/%2F")
	if strings.HasPrefix(objectName, "/") {
		objectName = strings.Replace(objectName, "/", "%2F", 1)
	}
	return objectName
}

// getResource gets canonicalized resource
func getResource(bucketName, objectName, subResource string) string {
	if subResource != "" {
		subResource = "?" + subResource
	}
	if bucketName == "" {
		return fmt.Sprintf("/%s%s", bucketName, subResource)
	}
	objectName = encodeKS3Str(objectName)
	tmp := "/" + bucketName + "/" + objectName + subResource
	return tmp
}

func (conn Conn) doRequest(ctx context.Context, method string, uri *url.URL, canonicalizedResource string, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	method = strings.ToUpper(method)
	req := &http.Request{
		Method:     method,
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	tracker := &readerTracker{completedBytes: 0}

	// v4 签名时，在 handleBody 包装 body 前从原始 body 计算 content-sha256，
	// 计算完哈希（并 Seek 回 0）的 reader 替换 data，使 handleBody 仍能拿到完整 body。
	if conn.config.AuthVersion == AuthV4 || conn.config.AuthVersion == AuthV4UnsignedPayload {
		v4s := v4Signer{conn.config, conn.Url}
		contentSha256, hashedData, err := v4s.computeContentSHA256FromReader(data)
		if err != nil {
			return nil, err
		}
		data = hashedData
		req.Header.Set(v4s.getHeaderName(v4ContentSHA256), contentSha256)
	}

	fd, crc := conn.handleBody(req, data, initCRC, listener, tracker)
	if fd != nil {
		defer func() {
			fd.Close()
			os.Remove(fd.Name())
		}()
	}

	if conn.config.IsAuthProxy {
		auth := conn.config.ProxyUser + ":" + conn.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set(HTTPHeaderDate, date)
	req.Header.Set(HTTPHeaderHost, req.Host)
	req.Header.Set(HTTPHeaderUserAgent, conn.config.UserAgent)

	creds := conn.config.GetCredentials()
	if creds.GetSecurityToken() != "" {
		req.Header.Set(HTTPHeaderKs3SecurityToken, creds.GetSecurityToken())
	}

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	if err := conn.signHeader(req, canonicalizedResource); err != nil {
		return nil, err
	}

	// Transfer started
	event := newProgressEvent(TransferStartedEvent, 0, req.ContentLength, 0)
	publishProgress(listener, event)

	// print out http req
	conn.LoggerHTTPReq(req)

	startT := time.Now()
	resp, err := conn.client.Do(req)
	cost := time.Now().UnixNano()/1000/1000 - startT.UnixNano()/1000/1000
	conn.config.WriteLog(Debug, "[Resp:%p]send http request, cost:%d(ms)\n", req, cost)

	// print out http resp
	conn.LoggerHTTPResp(req, resp)

	var ks3Resp *Response
	if err == nil && resp != nil {
		var e error
		ks3Resp, e = conn.handleResponse(resp, crc)
		if e == nil {
			// Transfer completed
			event = newProgressEvent(TransferCompletedEvent, tracker.completedBytes, req.ContentLength, 0)
			publishProgress(listener, event)
			return ks3Resp, e
		} else {
			err = e
		}
	}

	// Transfer failed
	event = newProgressEvent(TransferFailedEvent, tracker.completedBytes, req.ContentLength, 0)
	publishProgress(listener, event)
	conn.config.WriteLog(Debug, "[Resp:%p]http error:%s\n", req, err.Error())
	return ks3Resp, err
}

// signer 按 config.AuthVersion 返回对应的签名器实例（V4→v4Signer，否则→v2Signer）。
func (conn Conn) signer() signer {
	if conn.config.AuthVersion == AuthV4 || conn.config.AuthVersion == AuthV4UnsignedPayload {
		return v4Signer{conn.config, conn.Url}
	}
	return v2Signer{conn.config, conn.Url}
}

func (conn Conn) signHeader(req *http.Request, canonicalizedResource string) error {
	creds := conn.config.GetCredentials()
	if creds.GetAccessKeyID() == "" && creds.GetAccessKeySecret() == "" && creds.GetSecurityToken() == "" {
		return nil
	}
	return conn.signer().signHeader(req, creds, canonicalizedResource)
}

func (conn Conn) signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) (string, error) {
	return conn.signer().signURL(method, bucketName, objectName, expiration, params, headers)
}

func (conn Conn) signPolicyURL(bucketName string, expiration int64, params map[string]interface{}) (string, error) {
	return conn.signer().signPolicyURL(bucketName, expiration, params)
}

// handleBody handles request body
func (conn Conn) handleBody(req *http.Request, body io.Reader, initCRC uint64,
	listener ProgressListener, tracker *readerTracker) (*os.File, hash.Hash64) {
	var file *os.File
	var crc hash.Hash64
	var teeReader io.ReadCloser
	reader := body
	readerLen, err := GetReaderLen(reader)
	if err == nil {
		req.ContentLength = readerLen
	}
	req.Header.Set(HTTPHeaderContentLength, strconv.FormatInt(req.ContentLength, 10))
	if reader != nil {
		teeReader = TeeReader(reader, nil, req.ContentLength, listener, tracker)
	}
	// MD5
	if body != nil && conn.config.IsEnableMD5 && req.Header.Get(HTTPHeaderContentMD5) == "" {
		md5 := ""
		reader, md5, file, _ = calcMD5(body, req.ContentLength, conn.config.MD5Threshold)
		req.Header.Set(HTTPHeaderContentMD5, md5)
	}

	// crc64
	if body != nil && reader != nil && conn.config.IsEnableCRC && req.Header.Get(HTTPHeaderKs3CRC64) == "" {
		crc = NewCRC(CrcTable(), initCRC)
		teeReader = TeeReader(reader, crc, req.ContentLength, listener, tracker)
	}

	// HTTP body
	rc, ok := teeReader.(io.ReadCloser)
	if !ok && reader != nil {
		rc = ioutil.NopCloser(reader)
	}

	if conn.isUploadLimitReq(req) {
		limitReader := &LimitSpeedReader{
			reader:     rc,
			ks3Limiter: conn.config.UploadLimiter,
		}
		req.Body = limitReader
	} else {
		req.Body = rc
	}
	return file, crc
}

// isUploadLimitReq: judge limit upload speed or not
func (conn Conn) isUploadLimitReq(req *http.Request) bool {
	if conn.config.UploadLimitSpeed == 0 || conn.config.UploadLimiter == nil {
		return false
	}

	if req.Method != "GET" && req.Method != "DELETE" && req.Method != "HEAD" {
		if req.ContentLength > 0 {
			return true
		}
	}
	return false
}

func tryGetFileSize(f *os.File) int64 {
	fInfo, _ := f.Stat()
	return fInfo.Size()
}

// handleResponse handles response
func (conn Conn) handleResponse(resp *http.Response, crc hash.Hash64) (*Response, error) {
	var cliCRC uint64
	var srvCRC uint64

	statusCode := resp.StatusCode
	if statusCode/100 != 2 {
		if statusCode >= 400 && statusCode <= 505 {
			// 4xx and 5xx indicate that the operation has error occurred
			var respBody []byte
			respBody, err := readResponseBody(resp)
			if err != nil {
				return nil, err
			}

			if len(respBody) == 0 {
				err = ServiceError{
					StatusCode: statusCode,
					RequestID:  resp.Header.Get(HTTPHeaderKs3RequestID),
				}
			} else {
				// 响应体为服务端错误，按 Content-Type 分发解析（HTML/XML），失败兜底
				srvErr, ok := serviceErrFromBody(respBody, resp.StatusCode, resp.Header.Get(HTTPHeaderKs3RequestID), resp.Header.Get(HTTPHeaderContentType))
				if !ok { // 解析失败，返回原始错误信息
					err = fmt.Errorf("ks3: service returned invalid response body, status = %s, RequestId = %s", resp.Status, resp.Header.Get(HTTPHeaderKs3RequestID))
				} else {
					err = srvErr
				}
			}

			return &Response{
				StatusCode: resp.StatusCode,
				Headers:    resp.Header,
				Body:       ioutil.NopCloser(bytes.NewReader(respBody)), // restore the body
			}, err
		} else if statusCode >= 300 && statusCode <= 307 {
			// KS3 use 3xx, but response has no body
			err := fmt.Errorf("ks3: service returned %d,%s", resp.StatusCode, resp.Status)
			return &Response{
				StatusCode: resp.StatusCode,
				Headers:    resp.Header,
				Body:       resp.Body,
			}, err
		} else {
			// (0,300) [308,400) [506,)
			// Other extended http StatusCode
			var respBody []byte
			respBody, err := readResponseBody(resp)
			if err != nil {
				return &Response{StatusCode: resp.StatusCode, Headers: resp.Header, Body: ioutil.NopCloser(bytes.NewReader(respBody))}, err
			}

			if len(respBody) == 0 {
				err = ServiceError{
					StatusCode: statusCode,
					RequestID:  resp.Header.Get(HTTPHeaderKs3RequestID),
				}
			} else {
				// 响应体为服务端错误，按 Content-Type 分发解析（HTML/XML），失败兜底
				srvErr, ok := serviceErrFromBody(respBody, resp.StatusCode, resp.Header.Get(HTTPHeaderKs3RequestID), resp.Header.Get(HTTPHeaderContentType))
				if !ok { // 解析失败，返回原始错误信息
					err = fmt.Errorf("unkown response body, status = %s, RequestId = %s", resp.Status, resp.Header.Get(HTTPHeaderKs3RequestID))
				} else {
					err = srvErr
				}
			}

			return &Response{
				StatusCode: resp.StatusCode,
				Headers:    resp.Header,
				Body:       ioutil.NopCloser(bytes.NewReader(respBody)), // restore the body
			}, err
		}
	} else {
		if conn.config.IsEnableCRC && crc != nil {
			cliCRC = crc.Sum64()
		}
		srvCRC, _ = strconv.ParseUint(resp.Header.Get(HTTPHeaderKs3CRC64), 10, 64)

		realBody := resp.Body
		if conn.isDownloadLimitResponse(resp) {
			limitReader := &LimitSpeedReader{
				reader:     realBody,
				ks3Limiter: conn.config.DownloadLimiter,
			}
			realBody = limitReader
		}

		// 2xx, successful
		return &Response{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header,
			Body:       realBody,
			ClientCRC:  cliCRC,
			ServerCRC:  srvCRC,
		}, nil
	}
}

// isDownloadLimitResponse: judge limit download speed or not
func (conn Conn) isDownloadLimitResponse(resp *http.Response) bool {
	if resp == nil || conn.config.DownloadLimitSpeed == 0 || conn.config.DownloadLimiter == nil {
		return false
	}

	if strings.EqualFold(resp.Request.Method, "GET") {
		return true
	}
	return false
}

// LoggerHTTPReq Print the header information of the http request
func (conn Conn) LoggerHTTPReq(req *http.Request) {
	if conn.config.LogLevel < Debug || req == nil {
		return
	}
	conn.config.WriteLog(Debug, "[Req:%p]%s %s %s", req, req.Method, req.URL.String(), req.Proto)
	var logBuffer bytes.Buffer
	logBuffer.WriteString(fmt.Sprintf("Request Headers:\n"))
	for k, v := range req.Header {
		var valueBuffer bytes.Buffer
		for j := 0; j < len(v); j++ {
			if j > 0 {
				valueBuffer.WriteString(" ")
			}
			valueBuffer.WriteString(v[j])
		}
		logBuffer.WriteString(fmt.Sprintf("\t%s: %s\n", k, valueBuffer.String()))
	}
	conn.config.WriteLog(Debug, "[Req:%p]%s", req, logBuffer.String())
}

// LoggerHTTPResp Print Response to http request
func (conn Conn) LoggerHTTPResp(req *http.Request, resp *http.Response) {
	if conn.config.LogLevel < Debug || resp == nil {
		return
	}
	conn.config.WriteLog(Debug, "[Resp:%p]%s %s", req, resp.Proto, resp.Status)
	var logBuffer bytes.Buffer
	logBuffer.WriteString(fmt.Sprintf("Response Headers:\n"))
	for k, v := range resp.Header {
		var valueBuffer bytes.Buffer
		for j := 0; j < len(v); j++ {
			if j > 0 {
				valueBuffer.WriteString(" ")
			}
			valueBuffer.WriteString(v[j])
		}
		logBuffer.WriteString(fmt.Sprintf("\t%s: %s\n", k, valueBuffer.String()))
	}
	conn.config.WriteLog(Debug, "[Resp:%p]%s", req, logBuffer.String())
}

func calcMD5(body io.Reader, contentLen, md5Threshold int64) (reader io.Reader, b64 string, tempFile *os.File, err error) {
	if contentLen == 0 || contentLen > md5Threshold {
		// Huge body, use temporary file
		tempFile, err = ioutil.TempFile(os.TempDir(), TempFilePrefix)
		if tempFile != nil {
			io.Copy(tempFile, body)
			tempFile.Seek(0, os.SEEK_SET)
			md5 := md5.New()
			io.Copy(md5, tempFile)
			sum := md5.Sum(nil)
			b64 = base64.StdEncoding.EncodeToString(sum[:])
			tempFile.Seek(0, os.SEEK_SET)
			reader = tempFile
		}
	} else {
		// Small body, use memory
		buf, _ := ioutil.ReadAll(body)
		sum := md5.Sum(buf)
		b64 = base64.StdEncoding.EncodeToString(sum[:])
		reader = bytes.NewReader(buf)
	}
	return
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	out, err := ioutil.ReadAll(resp.Body)
	if err == io.EOF {
		err = nil
	}
	return out, err
}

func serviceErrFromXML(body []byte, statusCode int, requestID string) (ServiceError, error) {
	var storageErr ServiceError

	if err := xml.Unmarshal(body, &storageErr); err != nil {
		return storageErr, err
	}

	storageErr.StatusCode = statusCode
	storageErr.RequestID = requestID
	storageErr.RawMessage = string(body)
	return storageErr, nil
}

// serviceErrFromHTML 解析 HTML 错误响应（如网关/限流返回的 HTML 页）：
// Code 取状态码文本（http.StatusText，如 "Too Many Requests"），Message 取 <title>（兜底同 Code）。
func serviceErrFromHTML(body []byte, statusCode int, requestID string) ServiceError {
	code := http.StatusText(statusCode) // Code 用规范的状态码描述，如 "Too Many Requests"
	msg := code                         // 兜底：无 title 时用状态码文本
	titleRegexp := regexp.MustCompile(`<title>([^<]*)</title>`)
	if m := titleRegexp.FindSubmatch(body); len(m) >= 2 && len(m[1]) > 0 {
		msg = string(m[1])
	}
	return ServiceError{
		StatusCode: statusCode,
		Code:       code,
		Message:    msg,
		RequestID:  requestID,
		RawMessage: string(body),
	}
}

// serviceErrFromBody 解析错误响应体：按响应头 Content-Type 分发——
// HTML（含 text/html）→ ServiceError（取 <title>，无 Code）；否则 → XML 解析；XML 失败兜底原逻辑。
func serviceErrFromBody(body []byte, statusCode int, requestID, contentType string) (ServiceError, bool) {
	if isHTMLContentType(contentType) {
		return serviceErrFromHTML(body, statusCode, requestID), true
	}
	srvErr, err := serviceErrFromXML(body, statusCode, requestID)
	return srvErr, err == nil
}

// isHTMLContentType 判断响应是否为 HTML：Content-Type 含 html 即认为是。
func isHTMLContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "html")
}

func xmlUnmarshal(body io.Reader, v interface{}) error {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	return xml.Unmarshal(data, v)
}

func jsonUnmarshal(body io.Reader, v interface{}) error {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// timeoutConn handles HTTP timeout
type timeoutConn struct {
	conn        net.Conn
	timeout     time.Duration
	longTimeout time.Duration
}

func newTimeoutConn(conn net.Conn, timeout time.Duration, longTimeout time.Duration) *timeoutConn {
	conn.SetReadDeadline(time.Now().Add(longTimeout))
	return &timeoutConn{
		conn:        conn,
		timeout:     timeout,
		longTimeout: longTimeout,
	}
}

func (c *timeoutConn) Read(b []byte) (n int, err error) {
	c.SetReadDeadline(time.Now().Add(c.timeout))
	n, err = c.conn.Read(b)
	c.SetReadDeadline(time.Now().Add(c.longTimeout))
	return n, err
}

func (c *timeoutConn) Write(b []byte) (n int, err error) {
	c.SetWriteDeadline(time.Now().Add(c.timeout))
	n, err = c.conn.Write(b)
	c.SetReadDeadline(time.Now().Add(c.longTimeout))
	return n, err
}

func (c *timeoutConn) Close() error {
	return c.conn.Close()
}

func (c *timeoutConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *timeoutConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *timeoutConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *timeoutConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *timeoutConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// UrlMaker builds URL and resource
const (
	urlTypeCname = 1
	urlTypeIP    = 2
	urlTypeksyun = 3
)

type UrlMaker struct {
	Scheme          string // HTTP or HTTPS
	NetLoc          string // Host or IP
	Type            int    // 1 CNAME, 2 IP, 3 ksyun
	IsProxy         bool   // Proxy
	PathStyleAccess bool   // Access by second level domain
}

// Init parses endpoint
func (um *UrlMaker) Init(endpoint string, isCname bool, isProxy bool, pathStyleAccess bool) error {
	if strings.HasPrefix(endpoint, "http://") {
		um.Scheme = "http"
		um.NetLoc = endpoint[len("http://"):]
	} else if strings.HasPrefix(endpoint, "https://") {
		um.Scheme = "https"
		um.NetLoc = endpoint[len("https://"):]
	} else {
		um.Scheme = "http"
		um.NetLoc = endpoint
	}

	if strings.HasSuffix(um.NetLoc, "/") {
		um.NetLoc = um.NetLoc[0 : len(um.NetLoc)-1]
	}

	//use url.Parse() to get real host
	strUrl := um.Scheme + "://" + um.NetLoc
	_, err := url.Parse(strUrl)
	if err != nil {
		return err
	}

	//um.NetLoc = url.Host
	host, _, err := net.SplitHostPort(um.NetLoc)
	if err != nil {
		host = um.NetLoc
		if host != "" && host[0] == '[' && host[len(host)-1] == ']' {
			if len(host) <= 1 {
				return &net.AddrError{Err: "host is error", Addr: host}
			}
			if host[0] == '[' && host[len(host)-1] == ']' {
				host = host[1 : len(host)-1]
			}
		}

		ip := net.ParseIP(host)
		if ip != nil {
			um.Type = urlTypeIP
		} else if isCname {
			um.Type = urlTypeCname
		} else {
			um.Type = urlTypeksyun
		}
		um.IsProxy = isProxy
		um.PathStyleAccess = pathStyleAccess
	}
	return nil
}

// getURL gets URL
func (um UrlMaker) getURL(bucket, object, params string) *url.URL {
	host, path := um.buildURL(bucket, object)
	addr := ""
	if params == "" {
		addr = fmt.Sprintf("%s://%s%s", um.Scheme, host, path)
	} else {
		addr = fmt.Sprintf("%s://%s%s?%s", um.Scheme, host, path, params)
	}
	uri, _ := url.ParseRequestURI(addr)
	return uri
}

// getSignURL gets sign URL
func (um UrlMaker) getSignURL(bucket, object, params string) string {
	host, path := um.buildURL(bucket, object)
	return fmt.Sprintf("%s://%s%s?%s", um.Scheme, host, path, params)
}

// buildURL builds URL
func (um UrlMaker) buildURL(bucket, object string) (string, string) {
	var host = ""
	var path = ""

	//object = url.QueryEscape(object)
	object = strings.Replace(object, "+", "%20", -1)

	if um.Type == urlTypeCname {
		host = um.NetLoc
		path = "/" + object
	} else if um.Type == urlTypeIP {
		if bucket == "" {
			host = um.NetLoc
			path = "/"
		} else {
			host = um.NetLoc
			path = fmt.Sprintf("/%s/%s", bucket, object)
		}
	} else {
		if bucket == "" {
			host = um.NetLoc
			path = "/"
		} else if um.PathStyleAccess {
			host = um.NetLoc
			path = "/" + bucket + "/" + object
		} else {
			host = bucket + "." + um.NetLoc
			path = "/" + object
		}
	}

	return host, path
}
