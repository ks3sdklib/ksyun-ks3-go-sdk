package sign

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// SignV2 构造V2请求签名
// ak 您的AccessKeyID，必填
// sk 您的SecretAccessKey，必填
// bucket 访问的存储空间名称，如：test-bucket。若您想ListBuckets，则此处传空字符
// objectKey 访问的对象，如：demo.txt。若您是针对bucket进行的操作，则此处传空字符
// subResource 子资源，如：acl，policy等。若没有则此处传空字符
// req 请求对象，必填
func SignV2(ak string, sk string, bucket string, objectKey string, subResource string, req *http.Request) string {
	if ak == "" || sk == "" {
		return ""
	}
	// 获取规范化请求资源
	canonicalizedResource := getCanonicalizedResource(bucket, objectKey, subResource)
	// 获取规范化x-kss-请求头
	canonicalizedKssHeaders := getCanonicalizedKssHeaders(req)
	// 获取待签名字符串
	stringToSign := getStringToSign(req, canonicalizedResource, canonicalizedKssHeaders)
	// 获取签名
	signature := getSignature(stringToSign, sk)
	// 构造Authorization请求头
	Authorization := "KSS " + ak + ":" + signature

	return Authorization
}

func getCanonicalizedResource(bucketName, objectName, subResource string) string {
	if subResource != "" {
		subResource = "?" + subResource
	}
	if bucketName == "" {
		return fmt.Sprintf("/%s%s", bucketName, subResource)
	}
	objectName = encodeKS3Str(objectName)
	resource := "/" + bucketName + "/" + objectName + subResource
	return resource
}

func getCanonicalizedKssHeaders(req *http.Request) string {
	ks3HeadersMap := make(map[string]string)
	for k, v := range req.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-kss-") {
			ks3HeadersMap[strings.ToLower(k)] = v[0]
		}
	}
	hs := newHeaderSorter(ks3HeadersMap)

	hs.Sort()

	canonicalizedKssHeaders := ""

	for i := range hs.Keys {
		canonicalizedKssHeaders += hs.Keys[i] + ":" + hs.Vals[i] + "\n"
	}

	return canonicalizedKssHeaders
}

func getStringToSign(req *http.Request, canonicalizedResource string, canonicalizedKssHeaders string) string {
	date := req.Header.Get("Date")
	contentType := req.Header.Get("Content-Type")
	contentMd5 := req.Header.Get("Content-MD5")

	stringToSign := req.Method + "\n" + contentMd5 + "\n" + contentType + "\n" + date + "\n" + canonicalizedKssHeaders + canonicalizedResource

	return stringToSign
}

func getSignature(stringToSign string, sk string) string {
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(sk))
	io.WriteString(h, stringToSign)

	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return signature
}

func encodeKS3Str(str string) string {
	objectName := url.QueryEscape(str)
	objectName = strings.ReplaceAll(objectName, "+", "%20")
	objectName = strings.ReplaceAll(objectName, "*", "%2A")
	objectName = strings.ReplaceAll(objectName, "%7E", "~")
	objectName = strings.ReplaceAll(objectName, "%2F", "/")
	if strings.HasPrefix(objectName, "/") {
		objectName = strings.Replace(objectName, "/", "%2F", 1)
	}
	objectName = strings.ReplaceAll(objectName, "//", "/%2F")
	return objectName
}

func newHeaderSorter(m map[string]string) *HeaderSorter {
	hs := &HeaderSorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]string, 0, len(m)),
	}

	for k, v := range m {
		hs.Keys = append(hs.Keys, k)
		hs.Vals = append(hs.Vals, v)
	}
	return hs
}

type HeaderSorter struct {
	Keys []string
	Vals []string
}

func (hs *HeaderSorter) Sort() {
	sort.Sort(hs)
}

func (hs *HeaderSorter) Len() int {
	return len(hs.Vals)
}

func (hs *HeaderSorter) Less(i, j int) bool {
	return bytes.Compare([]byte(hs.Keys[i]), []byte(hs.Keys[j])) < 0
}

func (hs *HeaderSorter) Swap(i, j int) {
	hs.Vals[i], hs.Vals[j] = hs.Vals[j], hs.Vals[i]
	hs.Keys[i], hs.Keys[j] = hs.Keys[j], hs.Keys[i]
}

// GetUrl 构造请求的URL
// endpoint 访问域名，如：ks3-cn-beijing.ksyuncs.com，必填
// bucket 访问的存储空间名称，如：test-bucket。若您想ListBuckets，则此处传空字符
// objectKey 访问的对象，如：demo.txt。若您是针对bucket进行的操作，则此处传空字符
// subResource 子资源，如：acl，policy等。若没有则此处传空字符
// query 查询参数，如：prefix=test&max-keys=100。若没有则此处传空字符
func GetUrl(endpoint string, bucket string, objectKey string, subResource string, query string) string {
	queryString := ""
	if subResource != "" {
		queryString = "?" + subResource
	}
	if query != "" {
		queryString = "?" + query
	}
	if subResource != "" && query != "" {
		queryString = "?" + subResource + "&" + query
	}
	resource := encodeKS3Str(objectKey) + queryString
	if bucket == "" {
		return fmt.Sprintf("https://%s/%s", endpoint, resource)
	}
	return fmt.Sprintf("https://%s.%s/%s", bucket, endpoint, resource)
}