package ks3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// v4Signer 承载 V4（HMAC-SHA256）签名所需的状态，与 Conn 解耦。
type v4Signer struct {
	config   *Config
	urlMaker *UrlMaker
}

// V4 签名命名空间常量。
const (
	ks3V4Algorithm       = "KSS4-HMAC-SHA256"
	ks3V4Terminator      = "kss4_request"
	ks3V4ServiceName     = "ks3"
	ks3V4SecretKeyPrefix = "KSS4"
	ks3V4HeaderPrefix    = "x-kss-"
	ks3V4QueryPrefix     = "X-Kss-"

	awsV4Algorithm       = "AWS4-HMAC-SHA256"
	awsV4Terminator      = "aws4_request"
	awsV4ServiceName     = "s3"
	awsV4SecretKeyPrefix = "AWS4"
	awsV4HeaderPrefix    = "x-amz-"
	awsV4QueryPrefix     = "X-Amz-"

	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// namespace 返回当前命名空间的 V4 常量。
func (s v4Signer) namespace() (algorithm, terminator, service, secretPrefix, headerPrefix, queryPrefix string) {
	if s.config.UseAwsSignature {
		return awsV4Algorithm, awsV4Terminator, awsV4ServiceName, awsV4SecretKeyPrefix, awsV4HeaderPrefix, awsV4QueryPrefix
	}
	// ks3 默认分支允许用 config.ServiceName 覆盖 service（如向量桶 "s3vectors"），其余命名空间常量不变。
	svc := ks3V4ServiceName
	if s.config.ServiceName != "" {
		svc = s.config.ServiceName
	}
	return ks3V4Algorithm, ks3V4Terminator, svc, ks3V4SecretKeyPrefix, ks3V4HeaderPrefix, ks3V4QueryPrefix
}

// v4 头/查询参数名后缀。
const (
	v4Algorithm     = "Algorithm"
	v4Credential    = "Credential"
	v4Date          = "Date"
	v4Expires       = "Expires"
	v4SignedHeaders = "SignedHeaders"
	v4Signature     = "Signature"
	v4ContentSHA256 = "content-sha256"
	v4SecurityToken = "Security-Token"
	v4Policy        = "Policy"
)

// getHeaderName 返回规范小写头名。
func (s v4Signer) getHeaderName(name string) string {
	_, _, _, _, headerPrefix, _ := s.namespace()
	return headerPrefix + strings.ToLower(name)
}

// getQueryName 返回查询参数名。
func (s v4Signer) getQueryName(name string) string {
	_, _, _, _, _, queryPrefix := s.namespace()
	return queryPrefix + name
}

// signHeader 用 v4 签普通请求。
//
// contentSha256 通常由 doRequest 在 body 被包装成不可 seek 的 reader 之前预算并存到请求上；
// 若缺失则回退用当前 req.Body 现算（仅当 body 仍可 seek 时有效）。
// canonicalizedResource 仅 V2 签名需要，V4 忽略。
func (s v4Signer) signHeader(req *http.Request, creds Credentials, canonicalizedResource string) error {
	region := s.config.Region
	t := time.Now().UTC()

	if s.config.UseAwsSignature {
		replaceAwsHeaders(req)
	}

	req.Header.Set(s.getHeaderName(v4Date), t.Format("20060102T150405Z"))
	req.Header.Set("Host", req.Host)
	contentSha256 := req.Header.Get(s.getHeaderName(v4ContentSHA256))
	if contentSha256 == "" {
		var err error
		contentSha256, err = s.calculateContentHash(req)
		if err != nil {
			return err
		}
		req.Header.Set(s.getHeaderName(v4ContentSHA256), contentSha256)
	}

	canonicalRequest := s.createCanonicalRequest(req, contentSha256)
	stringToSign := s.createStringToSign(canonicalRequest, t, region)

	dateStamp := t.Format("20060102")
	signingKey := s.deriveSigningKey(creds.GetAccessKeySecret(), dateStamp, region)
	signature := v4ComputeSignature(stringToSign, signingKey)

	auth := s.buildAuthorizationHeader(req, signature, creds.GetAccessKeyID(), t, region)
	req.Header.Set(HTTPHeaderAuthorization, auth)

	s.config.WriteLog(Debug, "[Req:%p]v4 canonicalRequest:%q\n", req, canonicalRequest)
	s.config.WriteLog(Debug, "[Req:%p]v4 stringToSign:%q\n", req, stringToSign)
	return nil
}

// signURL 构造 v4 预签名 URL。
//
// 与 v2 signURL 不同，v4 的规范 URI 需要真实 host + path，故用 s.urlMaker 构造真实的
// *http.Request，而非只有 Method+Header 的桩请求。
func (s v4Signer) signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) (string, error) {
	creds := s.config.GetCredentials()
	region := s.config.Region
	t := time.Now().UTC()
	dateStamp := t.Format("20060102")
	algorithm, terminator, service, _, _, _ := s.namespace()
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, region, service, terminator)

	host, path := s.urlMaker.buildURL(bucketName, objectName)

	qAlgorithm := s.getQueryName(v4Algorithm)
	qCredential := s.getQueryName(v4Credential)
	qDate := s.getQueryName(v4Date)
	qExpires := s.getQueryName(v4Expires)
	qSignedHeaders := s.getQueryName(v4SignedHeaders)
	qSignature := s.getQueryName(v4Signature)
	params[qAlgorithm] = algorithm
	params[qCredential] = creds.GetAccessKeyID() + "/" + scope
	params[qDate] = t.Format("20060102T150405Z")
	params[qExpires] = strconv.FormatInt(expiration, 10)
	if creds.GetSecurityToken() != "" {
		params[s.getQueryName(v4SecurityToken)] = creds.GetSecurityToken()
	}

	query := make(url.Values)
	for k, v := range params {
		if v == nil {
			query.Set(k, "")
			continue
		}
		if s, ok := v.(string); ok {
			query.Set(k, s)
		}
	}
	presignHeaders := map[string]string{"Host": host}
	for k, v := range headers {
		if !s.isSignedHeader(k) {
			continue
		}
		key := k
		if s.config.UseAwsSignature {
			key = awsHeaderName(k)
		}
		presignHeaders[key] = v
	}
	signedHeaders := s.signedHeadersStringForHeaders(presignHeaders)
	params[qSignedHeaders] = signedHeaders
	query.Set(qSignedHeaders, signedHeaders)

	req := &http.Request{
		Method: strings.ToUpper(string(method)),
		Header: make(http.Header),
	}
	for k, v := range presignHeaders {
		req.Header.Set(k, v)
	}
	req.URL = &url.URL{Path: path, RawQuery: query.Encode()}

	canonicalRequest := s.createCanonicalRequest(req, unsignedPayload)
	stringToSign := s.createStringToSign(canonicalRequest, t, region)
	signingKey := s.deriveSigningKey(creds.GetAccessKeySecret(), dateStamp, region)
	signature := v4ComputeSignature(stringToSign, signingKey)

	params[qSignature] = signature
	query.Set(qSignature, signature)

	urlParams := getURLParams(params)
	return s.urlMaker.getSignURL(bucketName, objectName, urlParams), nil
}

// signPolicyURL 构造带 X-Kss-Policy 的 v4 分享外链。
//
// 与普通预签名 signURL 的区别：分享 URL 的规范请求只含规范查询串（无 method/path/headers），
// 且只签分享相关的查询参数（Algorithm/Credential/Date/Expires/Policy/Security-Token）。
func (s v4Signer) signPolicyURL(bucketName string, expiration int64, params map[string]interface{}) (string, error) {
	creds := s.config.GetCredentials()
	region := s.config.Region
	t := time.Now().UTC()
	dateStamp := t.Format("20060102")
	algorithm, terminator, service, _, _, _ := s.namespace()
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, region, service, terminator)

	// useAwsSignature 下把 X-Kss-Policy 改写为 X-Amz-Policy。
	if s.config.UseAwsSignature {
		ks3Policy := ks3V4QueryPrefix + v4Policy
		awsPolicy := awsV4QueryPrefix + v4Policy
		if p, ok := params[ks3Policy]; ok {
			params[awsPolicy] = p
			delete(params, ks3Policy)
		}
	}

	qAlgorithm := s.getQueryName(v4Algorithm)
	qCredential := s.getQueryName(v4Credential)
	qDate := s.getQueryName(v4Date)
	qExpires := s.getQueryName(v4Expires)
	qSignature := s.getQueryName(v4Signature)
	params[qAlgorithm] = algorithm
	params[qCredential] = creds.GetAccessKeyID() + "/" + scope
	params[qDate] = t.Format("20060102T150405Z")
	params[qExpires] = strconv.FormatInt(expiration, 10)
	if creds.GetSecurityToken() != "" {
		params[s.getQueryName(v4SecurityToken)] = creds.GetSecurityToken()
	}

	// 规范查询串只含分享相关参数，不含 X-Kss-Signature 本身。
	shareQuery := make(url.Values)
	for _, name := range []string{
		qAlgorithm, qCredential, qDate, qExpires,
		s.getQueryName(v4Policy),
		s.getQueryName(v4SecurityToken),
	} {
		if v, ok := params[name]; ok {
			if s, ok2 := v.(string); ok2 && s != "" {
				shareQuery.Set(name, s)
			}
		}
	}
	canonicalRequest := v4CanonicalizedQueryString(shareQuery) + "\n" + unsignedPayload

	stringToSign := s.createStringToSign(canonicalRequest, t, region)
	signingKey := s.deriveSigningKey(creds.GetAccessKeySecret(), dateStamp, region)
	signature := v4ComputeSignature(stringToSign, signingKey)

	params[qSignature] = signature

	urlParams := getURLParams(params)
	return s.urlMaker.getSignURL(bucketName, "", urlParams), nil
}

// calculateContentHash 计算请求体的 content-sha256（回退路径，主路径见 computeContentSHA256FromReader）：
//   - AuthV4UnsignedPayload 始终返回 UNSIGNED-PAYLOAD
//   - AuthV4 仅当 body 可重读（io.ReadSeeker）时算真实 sha256 并 Seek 回起始，否则 UNSIGNED-PAYLOAD
func (s v4Signer) calculateContentHash(req *http.Request) (string, error) {
	if s.config.AuthVersion == AuthV4UnsignedPayload {
		return unsignedPayload, nil
	}
	body := req.Body
	if body == nil {
		// 空 body：返回空串的 sha256。
		sum := sha256.Sum256(nil)
		return hex.EncodeToString(sum[:]), nil
	}
	// 可重读判断（io.ReadSeeker）。
	r, ok := body.(io.ReadSeeker)
	if !ok {
		return unsignedPayload, nil
	}
	// 算完 sha256 再把流 Seek 回起始，保证 body 仍可正常发送。
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// computeContentSHA256FromReader 在 handleBody 把 body 包装成不可 seek 的 reader 之前，
// 从原始 body 计算 content-sha256，返回的 reader 用于替换 data：
//   - AuthV4UnsignedPayload：始终 UNSIGNED-PAYLOAD
//   - AuthV4：data 为 io.ReadSeeker 时算真实 sha256 并 Seek 回起始；否则 UNSIGNED-PAYLOAD
func (s v4Signer) computeContentSHA256FromReader(data io.Reader) (string, io.Reader, error) {
	if s.config.AuthVersion == AuthV4UnsignedPayload {
		return unsignedPayload, data, nil
	}
	if data == nil {
		sum := sha256.Sum256(nil)
		return hex.EncodeToString(sum[:]), data, nil
	}
	r, ok := data.(io.ReadSeeker)
	if !ok {
		return unsignedPayload, data, nil
	}
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", nil, err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", nil, err
	}
	return hex.EncodeToString(h.Sum(nil)), r, nil
}

// isSignedHeader 判定请求头是否参与 V4 签名：Host、Content-Type 及命名空间前缀头。
func (s v4Signer) isSignedHeader(name string) bool {
	lower := strings.ToLower(name)
	if lower == "host" || lower == "content-type" {
		return true
	}
	return strings.HasPrefix(lower, ks3V4HeaderPrefix) || strings.HasPrefix(lower, awsV4HeaderPrefix)
}

// canonicalizedHeaderString 构造规范头字符串（仅白名单头）。
func (s v4Signer) canonicalizedHeaderString(req *http.Request) string {
	names := make([]string, 0, len(req.Header))
	for name := range req.Header {
		if !s.isSignedHeader(name) {
			continue
		}
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	var buf bytes.Buffer
	for _, name := range names {
		buf.WriteString(strings.ToLower(name) + ":" + strings.TrimSpace(req.Header.Get(name)) + "\n")
	}
	return buf.String()
}

// signedHeadersString 构造 SignedHeaders 值（仅白名单头）。
func (s v4Signer) signedHeadersString(req *http.Request) string {
	names := make([]string, 0, len(req.Header))
	for name := range req.Header {
		if !s.isSignedHeader(name) {
			continue
		}
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	lowered := make([]string, len(names))
	for i, name := range names {
		lowered[i] = strings.ToLower(name)
	}
	return strings.Join(lowered, ";")
}

// v4CanonicalizedQueryString 构造规范查询串：参数名/值各自编码后，按编码后的名称排序，
// "key=value" 以 & 连接（子资源无值则 value 为空）。
func v4CanonicalizedQueryString(query url.Values) string {
	type kv struct{ key, val string }
	pairs := make([]kv, 0, len(query))
	for k := range query {
		pairs = append(pairs, kv{v4URLEncodeQuery(k), v4URLEncodeQuery(query.Get(k))})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})
	var buf bytes.Buffer
	for i, p := range pairs {
		if i > 0 {
			buf.WriteString("&")
		}
		buf.WriteString(p.key)
		buf.WriteString("=")
		buf.WriteString(p.val)
	}
	return buf.String()
}

// v4URLEncodeQuery 编码查询参数：在 url.QueryEscape 基础上把空格的 "+" 改为 "%20"。
func v4URLEncodeQuery(s string) string {
	encoded := url.QueryEscape(s)
	// 空格：url.QueryEscape 编为 "+"，v4 规范要求 "%20"
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	return encoded
}

// v4CanonicalizedResourcePath 编码资源路径：保留 "/" 不编码，结果以 "/" 开头。
func v4CanonicalizedResourcePath(resourcePath string) string {
	if resourcePath == "" {
		return "/"
	}
	encoded := v4URLEncodePath(resourcePath)
	if !strings.HasPrefix(encoded, "/") {
		encoded = "/" + encoded
	}
	return encoded
}

// v4URLEncodePath 编码资源路径：在 url.QueryEscape 基础上把空格 "+" 改为 "%20"、还原 "/"。
func v4URLEncodePath(s string) string {
	encoded := url.QueryEscape(s)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "%2F", "/")
	return encoded
}

// createCanonicalRequest 构造规范请求：
//
//	METHOD\n
//	CanonicalURI\n
//	CanonicalQueryString\n
//	CanonicalHeaders\n
//	SignedHeaders\n
//	contentSha256
func (s v4Signer) createCanonicalRequest(req *http.Request, contentSha256 string) string {
	// 用未编码的 Path 作为输入，由 v4CanonicalizedResourcePath 单次编码；
	// 若用 EscapedPath()（已编码）会再编码一次，导致双重编码。
	path := req.URL.Path
	if path == "" {
		path = "/"
	}
	canonicalURI := v4CanonicalizedResourcePath(path)
	canonicalQuery := v4CanonicalizedQueryString(req.URL.Query())
	canonicalHeaders := s.canonicalizedHeaderString(req)
	signedHeaders := s.signedHeadersString(req)

	var buf bytes.Buffer
	buf.WriteString(req.Method)
	buf.WriteString("\n")
	buf.WriteString(canonicalURI)
	buf.WriteString("\n")
	buf.WriteString(canonicalQuery)
	buf.WriteString("\n")
	buf.WriteString(canonicalHeaders)
	buf.WriteString("\n")
	buf.WriteString(signedHeaders)
	buf.WriteString("\n")
	buf.WriteString(contentSha256)
	return buf.String()
}

// createStringToSign 构造待签名串：
//
//	Algorithm\n
//	FormattedDateTime(yyyyMMddTHHmmssZ)\n
//	Scope(dateStamp/region/service/terminator)\n
//	hex(sha256(canonicalRequest))
func (s v4Signer) createStringToSign(canonicalRequest string, t time.Time, region string) string {
	dateStamp := t.UTC().Format("20060102")
	dateTime := t.UTC().Format("20060102T150405Z")
	algorithm, terminator, service, _, _, _ := s.namespace()
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, region, service, terminator)
	sum := sha256.Sum256([]byte(canonicalRequest))
	return fmt.Sprintf("%s%s%s%s%s%s%s",
		algorithm, "\n",
		dateTime, "\n",
		scope, "\n",
		hex.EncodeToString(sum[:]))
}

// deriveSigningKey 派生签名密钥（四轮 HMAC-SHA256）：
//
//	kSecret  = secretKeyPrefix + secretKey
//	kDate    = HMAC-SHA256(dateStamp, kSecret)
//	kRegion  = HMAC-SHA256(region, kDate)
//	kService = HMAC-SHA256(service, kRegion)
//	kSigning = HMAC-SHA256(terminator, kService)
func (s v4Signer) deriveSigningKey(secretKey, dateStamp, region string) []byte {
	_, terminator, service, secretPrefix, _, _ := s.namespace()
	kSecret := []byte(secretPrefix + secretKey)
	kDate := hmacSHA256([]byte(dateStamp), kSecret)
	kRegion := hmacSHA256([]byte(region), kDate)
	kService := hmacSHA256([]byte(service), kRegion)
	return hmacSHA256([]byte(terminator), kService)
}

func hmacSHA256(data, key []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// v4ComputeSignature 计算最终签名（HMAC-SHA256 后十六进制编码）。
func v4ComputeSignature(stringToSign string, signingKey []byte) string {
	return hex.EncodeToString(hmacSHA256([]byte(stringToSign), signingKey))
}

// replaceAwsHeaders 改写头名/值的 KS3↔AWS 命名空间常量（仅在 UseAwsSignature 时用）。
const (
	awsAllUsersURI           = "http://acs.amazonaws.com/groups/global/AllUsers"
	ks3HeaderPrefixCanonical = "X-Kss-"
	awsHeaderPrefixCanonical = "X-Amz-"
)

// ks3OnlyHeaders 是即使开启 useAwsSignature 也必须保留 x-kss- 前缀的头。
var ks3OnlyHeaders = map[string]bool{
	"x-kss-retention-id":        true,
	"x-kss-retention-overwrite": true,
	"x-kss-callbackurl":         true,
	"x-kss-callbackbody":        true,
	"x-kss-callbackauth":        true,
	"x-kss-sourceurl":           true,
	"x-kss-force":               true,
	"x-kss-fetchsourceheader":   true,
}

// replaceAwsHeaders 把 x-kss- 请求头改写为 x-amz-（仅在 UseAwsSignature 时由调用方调用）：
//   - ks3OnlyHeaders 中的头保留 x-kss- 前缀
//   - 其余 x-kss- 头改名为 x-amz-（保留规范 key 的其余部分）
//   - 头值若为 ACL AllUsers URI 则在 KS3 与 AWS 两种形态间互换
//
// 原地修改 req.Header。
func replaceAwsHeaders(req *http.Request) {
	ks3AllUsers := `uri="` + ALL_USERS + `"`
	awsAllUsers := `uri="` + awsAllUsersURI + `"`
	newHeaders := make(http.Header, len(req.Header))
	for key, vals := range req.Header {
		lowerKey := strings.ToLower(key)
		if ks3OnlyHeaders[lowerKey] {
			newHeaders[key] = vals
			continue
		}
		if strings.HasPrefix(lowerKey, ks3V4HeaderPrefix) {
			// X-Kss- 与 X-Amz- 等长，按位置切片改名
			newKey := awsHeaderPrefixCanonical + key[len(ks3HeaderPrefixCanonical):]
			for i, v := range vals {
				if v == ks3AllUsers {
					vals[i] = awsAllUsers
				} else if v == awsAllUsers {
					vals[i] = ks3AllUsers
				}
			}
			newHeaders[newKey] = vals
		} else {
			newHeaders[key] = vals
		}
	}
	req.Header = newHeaders
}

// awsHeaderName 在 AWS 模式把 x-kss- 头名改写为 x-amz-（ks3OnlyHeaders 保留）。仅 AWS 模式使用。
func awsHeaderName(key string) string {
	lowerKey := strings.ToLower(key)
	if ks3OnlyHeaders[lowerKey] {
		return key
	}
	if strings.HasPrefix(lowerKey, ks3V4HeaderPrefix) {
		// X-Kss- 与 X-Amz- 等长，按位置切片改名
		return awsHeaderPrefixCanonical + key[len(ks3HeaderPrefixCanonical):]
	}
	return key
}

// buildAuthorizationHeader 构造 Authorization 头：Algorithm Credential=AK/scope, SignedHeaders=..., Signature=hex。
func (s v4Signer) buildAuthorizationHeader(req *http.Request, signature string, accessKeyID string, t time.Time, region string) string {
	dateStamp := t.UTC().Format("20060102")
	algorithm, terminator, service, _, _, _ := s.namespace()
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStamp, region, service, terminator)
	return fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, accessKeyID, scope, s.signedHeadersString(req), signature)
}

// signedHeadersStringForHeaders 为显式头 map 构造 SignedHeaders（预签名用）。
func (s v4Signer) signedHeadersStringForHeaders(headers map[string]string) string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	lowered := make([]string, len(names))
	for i, name := range names {
		lowered[i] = strings.ToLower(name)
	}
	return strings.Join(lowered, ";")
}
