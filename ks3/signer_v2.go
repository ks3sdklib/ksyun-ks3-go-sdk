package ks3

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// v2Signer 承载 V2（HMAC-SHA1 / KSS 前缀）签名所需的状态，与 Conn 解耦。
type v2Signer struct {
	config   *Config
	urlMaker *UrlMaker
}

// V2 预签名 / 分享外链的查询参数名。
const (
	v2Expires       = "Expires"
	v2Signature     = "Signature"
	v2SecurityToken = "security-token"
)

// V2 签名命名空间常量。
const (
	ks3V2AuthPrefix       = "KSS"
	ks3V2HeaderPrefix     = "x-kss-"
	ks3V2AccessKeyIDParam = "KSSAccessKeyId"

	awsV2AuthPrefix       = "AWS"
	awsV2HeaderPrefix     = "x-amz-"
	awsV2AccessKeyIDParam = "AWSAccessKeyId"
)

// namespace 返回当前命名空间的 V2 前缀。
func (s v2Signer) namespace() (authPrefix, headerPrefix, accessKeyIDParam string) {
	if s.config.UseAwsSignature {
		return awsV2AuthPrefix, awsV2HeaderPrefix, awsV2AccessKeyIDParam
	}
	return ks3V2AuthPrefix, ks3V2HeaderPrefix, ks3V2AccessKeyIDParam
}

// signHeader 用 V2（HMAC-SHA1）对请求头签名并设置 Authorization 头。
func (s v2Signer) signHeader(req *http.Request, creds Credentials, canonicalizedResource string) error {
	authPrefix, headerPrefix, _ := s.namespace()
	if s.config.UseAwsSignature {
		replaceAwsHeaders(req)
	}
	authorizationStr := authPrefix + " " + creds.GetAccessKeyID() + ":" + s.getSignedStr(req, canonicalizedResource, creds.GetAccessKeySecret(), headerPrefix)
	req.Header.Set(HTTPHeaderAuthorization, authorizationStr)
	return nil
}

// signURL 构造 V2 预签名 URL：构造带签名头的请求、算签名、填查询参数、拼最终 URL。
func (s v2Signer) signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) (string, error) {
	creds := s.config.GetCredentials()
	if creds.GetSecurityToken() != "" {
		params[v2SecurityToken] = creds.GetSecurityToken()
	}

	m := strings.ToUpper(string(method))
	req := &http.Request{
		Method: m,
		Header: make(http.Header),
	}

	if s.config.IsAuthProxy {
		auth := s.config.ProxyUser + ":" + s.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	req.Header.Set(HTTPHeaderDate, strconv.FormatInt(expiration, 10))
	req.Header.Set(HTTPHeaderUserAgent, s.config.UserAgent)

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	subResource := getSubResource(params)
	canonicalResource := getResource(bucketName, objectName, subResource)

	_, headerPrefix, accessKeyIdParam := s.namespace()
	if s.config.UseAwsSignature {
		replaceAwsHeaders(req)
	}
	signedStr := s.getSignedStr(req, canonicalResource, creds.GetAccessKeySecret(), headerPrefix)

	params[v2Expires] = strconv.FormatInt(expiration, 10)
	params[accessKeyIdParam] = creds.GetAccessKeyID()
	params[v2Signature] = signedStr
	str := encodeKS3Str(objectName)
	urlParams := getURLParams(params)
	return s.urlMaker.getSignURL(bucketName, str, urlParams), nil
}

// signPolicyURL 构造带 X-Kss-Policy 的 V2 分享外链。
func (s v2Signer) signPolicyURL(bucketName string, expiration int64, params map[string]interface{}) (string, error) {
	creds := s.config.GetCredentials()
	if creds.GetSecurityToken() != "" {
		params[v2SecurityToken] = creds.GetSecurityToken()
	}

	date := strconv.FormatInt(expiration, 10)
	subResource := getSubResource(params)
	canonicalResource := getResource(bucketName, "", subResource)
	signedStr := s.getPolicySignedStr(canonicalResource, date, creds.GetAccessKeySecret())

	params[v2Expires] = strconv.FormatInt(expiration, 10)
	params[ks3V2AccessKeyIDParam] = creds.GetAccessKeyID()
	params[v2Signature] = signedStr

	urlParams := getURLParams(params)
	return s.urlMaker.getSignURL(bucketName, "", urlParams), nil
}

// getSignedStr 用 V2（HMAC-SHA1）对请求头签名，返回 base64 编码的签名串。
func (s v2Signer) getSignedStr(req *http.Request, canonicalizedResource string, keySecret string, headerPrefix string) string {
	ks3HeadersMap := make(map[string]string)
	for k, v := range req.Header {
		if strings.HasPrefix(strings.ToLower(k), headerPrefix) {
			ks3HeadersMap[strings.ToLower(k)] = v[0]
		}
	}

	// 按头名升序构造 canonicalizedKS3Headers
	keys := make([]string, 0, len(ks3HeadersMap))
	for k := range ks3HeadersMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	canonicalizedKS3Headers := ""
	for _, k := range keys {
		canonicalizedKS3Headers += k + ":" + ks3HeadersMap[k] + "\n"
	}

	// 签名 URL 时 date 即 expires
	date := req.Header.Get(HTTPHeaderDate)
	contentType := req.Header.Get(HTTPHeaderContentType)
	contentMd5 := req.Header.Get(HTTPHeaderContentMD5)

	signStr := req.Method + "\n" + contentMd5 + "\n" + contentType + "\n" + date + "\n" + canonicalizedKS3Headers + canonicalizedResource
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(keySecret))

	s.config.WriteLog(Debug, "[Req:%p]v2 signStr:%q\n", req, signStr)

	io.WriteString(h, signStr)
	signedStr := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return signedStr
}

// getPolicySignedStr 用 V2（HMAC-SHA1）对 policy 资源签名（date + "\n" + canonicalizedResource）。
func (s v2Signer) getPolicySignedStr(canonicalizedResource string, date string, keySecret string) string {
	signStr := date + "\n" + canonicalizedResource
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(keySecret))

	s.config.WriteLog(Debug, "policy signStr:%q\n", signStr)

	io.WriteString(h, signStr)
	signedStr := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return signedStr
}
