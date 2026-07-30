package ks3

import (
	"bytes"
	"encoding/base64"
	"strings"

	"net/http"
	"net/url"

	. "gopkg.in/check.v1"
)

// seekableReadCloser 实现 io.ReadSeeker 与 io.ReadCloser，使 v4 的可重读判断通过。
type seekableReadCloser struct {
	*bytes.Reader
}

func (seekableReadCloser) Close() error { return nil }

// TestV4HeaderSign 构造一个 v4 签名请求并断言 Authorization 头结构与必需的 v4 头。
// 不与已知签名向量比对签名值（仅做结构断言）。
func (s *Ks3ConnSuite) TestV4HeaderSign(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "PUT",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}
	req.Header.Set("Content-Type", "text/plain")
	// 用可 seek 的 body，使 AuthV4 算出真实 sha256。
	req.Body = seekableReadCloser{bytes.NewReader([]byte("hello"))}

	err := conn.signHeader(req, getResource("bucket", "object", ""))
	c.Assert(err, IsNil)

	auth := req.Header.Get(HTTPHeaderAuthorization)
	testLogger.Println("V4 AUTHORIZATION:", auth)
	c.Assert(strings.HasPrefix(auth, "KSS4-HMAC-SHA256 "), Equals, true)
	c.Assert(strings.Contains(auth, "Credential=test-ak/"), Equals, true)
	c.Assert(strings.Contains(auth, "/BEIJING/ks3/kss4_request"), Equals, true)
	c.Assert(strings.Contains(auth, "SignedHeaders="), Equals, true)
	c.Assert(strings.Contains(auth, "Signature="), Equals, true)

	// V4 白名单：SignedHeaders 含 Host/Content-Type/x-kss-*，不含 Date/User-Agent/Content-Length 等
	c.Assert(strings.Contains(auth, "SignedHeaders=content-type;host;x-kss-content-sha256;x-kss-date"), Equals, true)
	c.Assert(strings.Contains(auth, "user-agent"), Equals, false)
	c.Assert(strings.Contains(auth, "content-length"), Equals, false)

	// 必需的 v4 头
	c.Assert(req.Header.Get("x-kss-date"), Not(Equals), "")
	c.Assert(req.Header.Get("x-kss-content-sha256"), Not(Equals), "")
	// 可 seek body 的 content-sha256 在 AuthV4 下不是 "UNSIGNED-PAYLOAD"
	c.Assert(req.Header.Get("x-kss-content-sha256"), Not(Equals), unsignedPayload)
}

// TestV4UnsignedPayloadHeaderSign 验证 AuthV4UnsignedPayload 始终用 UNSIGNED-PAYLOAD。
func (s *Ks3ConnSuite) TestV4UnsignedPayloadHeaderSign(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4UnsignedPayload
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "GET",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	err := conn.signHeader(req, getResource("bucket", "object", ""))
	c.Assert(err, IsNil)
	c.Assert(req.Header.Get("x-kss-content-sha256"), Equals, unsignedPayload)
	c.Assert(strings.HasPrefix(req.Header.Get(HTTPHeaderAuthorization), "KSS4-HMAC-SHA256 "), Equals, true)
}

// TestV4RequiresRegion 验证 v4 未设 region 时构造 client 会失败。
func (s *Ks3ConnSuite) TestV4RequiresRegion(c *C) {
	_, err := New("https://ks3-example.com", "ak", "sk", AuthVersion(AuthV4))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "region"), Equals, true)

	// v2 不设 region 仍可工作（向后兼容）
	_, err = New("https://ks3-example.com", "ak", "sk")
	c.Assert(err, IsNil)

	// v4 设 region 可正常构造
	_, err = New("https://ks3-example.com", "ak", "sk", AuthVersion(AuthV4), Region("BEIJING"))
	c.Assert(err, IsNil)
}

// TestV4PresignedURL 验证 v4 预签名 URL 带 X-Kss-* 查询参数与签名。
func (s *Ks3ConnSuite) TestV4PresignedURL(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{}
	signedURL, err := conn.signURL("GET", "bucket", "object", 3600, params, nil)
	c.Assert(err, IsNil)
	testLogger.Println("V4 PRESIGNED URL:", signedURL)
	c.Assert(strings.Contains(signedURL, "X-Kss-Algorithm=KSS4-HMAC-SHA256"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Credential=test-ak"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Date="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Expires=3600"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-SignedHeaders="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Signature="), Equals, true)
}

// TestV4PresignedURLWithContentType 验证预签名 URL 纳入用户传入的 Content-Type 头。
func (s *Ks3ConnSuite) TestV4PresignedURLWithContentType(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{}
	headers := map[string]string{"Content-Type": "text/plain"}
	signedURL, err := conn.signURL("GET", "bucket", "object", 3600, params, headers)
	c.Assert(err, IsNil)

	// SignedHeaders 应含 content-type（; 在 URL 中编码为 %3B）
	c.Assert(strings.Contains(signedURL, "X-Kss-SignedHeaders=content-type%3Bhost"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Signature="), Equals, true)

	u, err := url.Parse(signedURL)
	c.Assert(err, IsNil)
	c.Assert(u.Query().Get("X-Kss-SignedHeaders"), Equals, "content-type;host")
}

// TestV4AwsPresignedURLWithXKssHeader 验证 AWS 模式预签名把 x-kss- 用户头改写为 x-amz- 再签。
func (s *Ks3ConnSuite) TestV4AwsPresignedURLWithXKssHeader(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.UseAwsSignature = true
	cfg.Region = "us-east-1"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{}
	headers := map[string]string{"X-Kss-Acl": "public-read"}
	signedURL, err := conn.signURL("GET", "bucket", "object", 3600, params, headers)
	c.Assert(err, IsNil)

	u, err := url.Parse(signedURL)
	c.Assert(err, IsNil)
	c.Assert(u.Query().Get("X-Amz-SignedHeaders"), Equals, "host;x-amz-acl")
}

// TestV4CanonicalizedResourcePath 验证 CanonicalURI 对 object key 单次编码、保留 "/"。
func (s *Ks3ConnSuite) TestV4CanonicalizedResourcePath(c *C) {
	c.Assert(v4CanonicalizedResourcePath(""), Equals, "/")
	c.Assert(v4CanonicalizedResourcePath("/"), Equals, "/")
	c.Assert(v4CanonicalizedResourcePath("/photo.jpg"), Equals, "/photo.jpg")
	// 空格单次编码为 %20，不应双重编码成 %2520
	c.Assert(v4CanonicalizedResourcePath("/my file.jpg"), Equals, "/my%20file.jpg")
	c.Assert(strings.Contains(v4CanonicalizedResourcePath("/my file.jpg"), "%25"), Equals, false)
	// "/" 不编码
	c.Assert(v4CanonicalizedResourcePath("/a/b/c"), Equals, "/a/b/c")
}

// TestV4PresignedURLWithSpacedKey 验证含空格的 object key 预签名不双重编码、签名可生成。
func (s *Ks3ConnSuite) TestV4PresignedURLWithSpacedKey(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{}
	signedURL, err := conn.signURL("GET", "bucket", "my file.jpg", 3600, params, nil)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(signedURL, "X-Kss-Signature="), Equals, true)
	// URL 路径里空格应为 %20，不应出现双重编码的 %2520
	c.Assert(strings.Contains(signedURL, "%2520"), Equals, false)
}

// TestSignURLRequiresCredentials 验证 V2/V4 预签名与分享外链在 AK 或 SK 为空时报错（对齐 Java 业务入口校验）。
func (s *Ks3ConnSuite) TestSignURLRequiresCredentials(c *C) {
	for _, auth := range []AuthVersionType{AuthV2, AuthV4} {
		cfg := getDefaultKs3Config()
		cfg.AuthVersion = auth
		if auth == AuthV4 {
			cfg.Region = "BEIJING"
		}
		// AK/SK 留空（getDefaultKs3Config 默认即空）
		um := UrlMaker{}
		um.Init("https://ks3-example.com", false, false, false)
		conn := &Conn{cfg, &um, nil}
		bucket := Bucket{Client: Client{Config: cfg, Conn: conn}, BucketName: "bucket"}

		_, err := bucket.SignURL("object", "GET", 3600)
		c.Assert(err, NotNil)

		_, err = bucket.SignPolicyURL(`{"expiration":"2030-01-01T00:00:00Z"}`, 3600)
		c.Assert(err, NotNil)
	}
}

// TestV4AwsNamespace 验证 useAwsSignature 产出 AWS 兼容的 v4 命名空间：
// AWS4-HMAC-SHA256 算法、s3 service、aws4_request terminator、x-amz-* 头。
func (s *Ks3ConnSuite) TestV4AwsNamespace(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.UseAwsSignature = true
	cfg.Region = "us-east-1"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "PUT",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}
	req.Header.Set("X-Kss-Acl", "public-read") // 一个 x-kss- 头，应被改写为 x-amz-
	req.Body = seekableReadCloser{bytes.NewReader([]byte("hello"))}

	err := conn.signHeader(req, getResource("bucket", "object", ""))
	c.Assert(err, IsNil)

	auth := req.Header.Get(HTTPHeaderAuthorization)
	testLogger.Println("AWS V4 AUTHORIZATION:", auth)
	c.Assert(strings.HasPrefix(auth, "AWS4-HMAC-SHA256 "), Equals, true)
	c.Assert(strings.Contains(auth, "/us-east-1/s3/aws4_request"), Equals, true)
	// x-kss- 头被改写为 x-amz-，并以 x-amz-acl 参与签名
	c.Assert(strings.Contains(auth, "SignedHeaders="), Equals, true)
	c.Assert(strings.Contains(req.Header.Get("x-amz-date"), "T"), Equals, true)
	c.Assert(req.Header.Get("x-amz-content-sha256"), Not(Equals), "")
	// x-kss-acl 改名为 x-amz-acl
	c.Assert(req.Header.Get("X-Amz-Acl"), Equals, "public-read")
	c.Assert(req.Header.Get("X-Kss-Acl"), Equals, "")
}

// TestV4AwsPresignedURL 验证 useAwsSignature 下 v4 预签名 URL 用 X-Amz-* 参数。
func (s *Ks3ConnSuite) TestV4AwsPresignedURL(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.UseAwsSignature = true
	cfg.Region = "us-east-1"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{}
	signedURL, err := conn.signURL("GET", "bucket", "object", 3600, params, nil)
	c.Assert(err, IsNil)
	testLogger.Println("AWS V4 PRESIGNED URL:", signedURL)
	c.Assert(strings.Contains(signedURL, "X-Amz-Algorithm=AWS4-HMAC-SHA256"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Amz-Credential=test-ak"), Equals, true)
	// scope 里的斜杠在 URL 中编码为 %2F，故用编码形式断言
	c.Assert(strings.Contains(signedURL, "us-east-1%2Fs3%2Faws4_request"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Amz-Signature="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-"), Equals, false)
}

// TestV4ShareURL 验证带 X-Kss-Policy 的 v4 分享外链（对应 Java signInQuery 分享分支）：
// 含 Algorithm/Credential/Date/Expires/Policy/Signature，且不带 SignedHeaders 查询参数。
func (s *Ks3ConnSuite) TestV4ShareURL(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.Region = "BEIJING"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{
		"X-Kss-Policy": base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2030-01-01T00:00:00Z"}`)),
		"prefix":       "shared/",
	}
	signedURL, err := conn.signPolicyURL("bucket", 3600, params)
	c.Assert(err, IsNil)
	testLogger.Println("V4 SHARE URL:", signedURL)
	c.Assert(strings.Contains(signedURL, "X-Kss-Algorithm=KSS4-HMAC-SHA256"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Credential=test-ak"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Date="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Expires=3600"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Policy="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Kss-Signature="), Equals, true)
	// 分享外链不带 SignedHeaders
	c.Assert(strings.Contains(signedURL, "SignedHeaders="), Equals, false)
	// business 参数 prefix 不参与签名规范查询串，但仍出现在最终 URL 中
	c.Assert(strings.Contains(signedURL, "prefix=shared"), Equals, true)
}

// TestV4AwsShareURL 验证 useAwsSignature 下分享外链用 X-Amz-* 参数。
func (s *Ks3ConnSuite) TestV4AwsShareURL(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.UseAwsSignature = true
	cfg.Region = "us-east-1"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	params := map[string]interface{}{
		"X-Kss-Policy": base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2030-01-01T00:00:00Z"}`)),
	}
	// AWS 命名空间下 signPolicyURLV4 自动把 X-Kss-Policy 改为 X-Amz-Policy（对应 Java AwsV4Signer.beforeSigning）
	signedURL, err := conn.signPolicyURL("bucket", 3600, params)
	c.Assert(err, IsNil)
	testLogger.Println("AWS V4 SHARE URL:", signedURL)
	c.Assert(strings.Contains(signedURL, "X-Amz-Algorithm=AWS4-HMAC-SHA256"), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Amz-Policy="), Equals, true)
	c.Assert(strings.Contains(signedURL, "X-Amz-Signature="), Equals, true)
	c.Assert(strings.Contains(signedURL, "SignedHeaders="), Equals, false)
	// X-Kss-Policy 已被改写，不应残留
	c.Assert(strings.Contains(signedURL, "X-Kss-Policy="), Equals, false)
}

// TestV2AwsNamespace 验证 v2 在 useAwsSignature 下用 AWS 认证前缀（AWS）和 x-amz- 头。
func (s *Ks3ConnSuite) TestV2AwsNamespace(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV2
	cfg.UseAwsSignature = true
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "PUT",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}
	req.Header.Set("X-Kss-Acl", "public-read")
	req.Header.Set("Content-Type", "text/plain")

	err := conn.signHeader(req, getResource("bucket", "object", ""))
	c.Assert(err, IsNil)
	auth := req.Header.Get(HTTPHeaderAuthorization)
	testLogger.Println("AWS V2 AUTHORIZATION:", auth)
	c.Assert(strings.HasPrefix(auth, "AWS "), Equals, true)
	// x-kss-acl 改名为 x-amz-acl
	c.Assert(req.Header.Get("X-Amz-Acl"), Equals, "public-read")
}

// TestKs3OnlyHeaderPreserved 验证 ks3 专有头（x-kss-callbackurl）在 useAwsSignature 下不被改写。
func (s *Ks3ConnSuite) TestKs3OnlyHeaderPreserved(c *C) {
	endpoint := "https://ks3-example.com"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV4
	cfg.UseAwsSignature = true
	cfg.Region = "us-east-1"
	cfg.AccessKeyID = "test-ak"
	cfg.AccessKeySecret = "test-sk"
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "PUT",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}
	req.Header.Set("X-Kss-Callbackurl", "http://example.com/cb") // ks3-only header, must stay x-kss-
	req.Body = seekableReadCloser{bytes.NewReader([]byte("hello"))}

	err := conn.signHeader(req, getResource("bucket", "object", ""))
	c.Assert(err, IsNil)
	// x-kss-callbackurl 被保留（不改为 x-amz-callbackurl）
	c.Assert(req.Header.Get("X-Kss-Callbackurl"), Equals, "http://example.com/cb")
}
