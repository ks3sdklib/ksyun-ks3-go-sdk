// Package ks3 implements functions for access ks3 service.
// It has two main struct Client and Bucket.
package ks3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// Client SDK's entry point. It's for bucket related options such as create/delete/set bucket (such as set/get ACL/lifecycle/referer/logging/website).
// Object related operations are done by Bucket class.
// Users use ks3.New to create Client instance.
//
type (
	// Client KS3 client
	Client struct {
		Config     *Config      // KS3 client configuration
		Conn       *Conn        // Send HTTP request
		HTTPClient *http.Client //http.Client to use - if nil will make its own
	}

	// ClientOption client option such as UseCname, Timeout, SecurityToken.
	ClientOption func(*Client)
)

// New creates a new client.
//
// endpoint    the KS3 datacenter endpoint such as http://ks3-cn-hangzhou.ksyuncs.com .
// accessKeyId    access key Id.
// accessKeySecret    access key secret.
//
// Client    creates the new client instance, the returned value is valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func New(endpoint, accessKeyID, accessKeySecret string, options ...ClientOption) (*Client, error) {
	// Configuration
	config := getDefaultKs3Config()
	config.Endpoint = endpoint
	config.AccessKeyID = accessKeyID
	config.AccessKeySecret = accessKeySecret

	// URL parse
	url := &UrlMaker{}
	err := url.Init(config.Endpoint, config.IsCname, config.IsUseProxy, config.PathStyleAccess)
	if err != nil {
		return nil, err
	}

	// HTTP connect
	conn := &Conn{config: config, Url: url}

	// KS3 client
	client := &Client{
		Config: config,
		Conn:   conn,
	}

	// Client options parse
	for _, option := range options {
		option(client)
	}

	if config.AuthVersion != AuthV1 && config.AuthVersion != AuthV2 {
		return nil, fmt.Errorf("Init client Error, invalid Auth version: %v", config.AuthVersion)
	}

	// Create HTTP connection
	err = conn.init(config, url, client.HTTPClient)

	return client, err
}

// Bucket gets the bucket instance.
//
// bucketName    the bucket name.
// Bucket    the bucket object, when error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) Bucket(bucketName string) (*Bucket, error) {
	err := CheckBucketName(bucketName)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		client,
		bucketName,
	}, nil
}

// CreateBucket creates a bucket.
//
// bucketName    the bucket name, it's globably unique and immutable. The bucket name can only consist of lowercase letters, numbers and dash ('-').
//               It must start with lowercase letter or number and the length can only be between 3 and 255.
// options    options for creating the bucket, with optional ACL. The ACL could be ACLPrivate, ACLPublicRead, and ACLPublicReadWrite. By default it's ACLPrivate.
//            It could also be specified with StorageClass option, which supports StorageStandard, StorageIA(infrequent access), StorageArchive.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) CreateBucket(bucketName string, options ...Option) error {
	headers := make(map[string]string)
	err := handleOptions(headers, options)
	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	contentType := http.DetectContentType(buffer.Bytes())
	headers[HTTPHeaderContentType] = contentType

	if headers[HTTPHeaderBucketType] == "" {
		bType := TypeNormal
		isBucketTypeSet, valBucketType, _ := IsOptionSet(options, bucketType)
		isStorageSet, valStorage, _ := IsOptionSet(options, storageClass)
		if isBucketTypeSet {
			bType = valBucketType.(BucketType)
		} else if isStorageSet {
			bType = valStorage.(BucketType)
		}
		headers[HTTPHeaderBucketType] = string(bType)
	}

	params := map[string]interface{}{}
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// create bucket xml
func (client Client) CreateBucketXml(bucketName string, xmlBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(xmlBody))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// ListBuckets lists buckets of the current account under the given endpoint, with optional filters.
//
// options    specifies the filters such as Prefix, Marker and MaxKeys. Prefix is the bucket name's prefix filter.
//            And marker makes sure the returned buckets' name are greater than it in lexicographic order.
//            Maxkeys limits the max keys to return, and by default it's 100 and up to 1000.
//            For the common usage scenario, please check out list_bucket.go in the sample.
// ListBucketsResponse    the response object if error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) ListBuckets(options ...Option) (ListBucketsResult, error) {
	var out ListBucketsResult

	params, err := GetRawParams(options)
	if err != nil {
		return out, err
	}

	resp, err := client.do("GET", "", params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// IsBucketExist checks if the bucket exists
//
// bucketName    the bucket name.
//
// bool    true if it exists, and it's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) IsBucketExist(bucketName string) (bool, error) {
	listRes, err := client.ListBuckets()
	if err != nil {
		return false, err
	}
	for _, bucketInfo := range listRes.Buckets {
		if bucketInfo.Name == bucketName {
			return true, err
		}
	}
	return false, nil
}

// DeleteBucket deletes the bucket. Only empty bucket can be deleted (no object and parts).
//
// bucketName    the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucket(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// GetBucketLocation gets the bucket location.
//
// Checks out the following link for more information :
// https://help.ksyun.com/document_detail/ks3/user_guide/ks3_concept/endpoint.html
//
// bucketName    the bucket name
//
// string    bucket's datacenter location
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketLocation(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["location"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var LocationConstraint string
	err = xmlUnmarshal(resp.Body, &LocationConstraint)
	return LocationConstraint, err
}

// SetBucketACL sets bucket's ACL.
//
// bucketName    the bucket name
// bucketAcl    the bucket ACL: ACLPrivate, ACLPublicRead and ACLPublicReadWrite.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketACL(bucketName string, bucketACL ACLType, options ...Option) error {
	headers := map[string]string{HTTPHeaderKs3ACL: string(bucketACL)}
	params := map[string]interface{}{}
	params["acl"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketACL gets the bucket ACL.
//
// bucketName    the bucket name.
//
// GetBucketAclResponse    the result object, and it's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketACL(bucketName string, options ...Option) (GetBucketACLResult, error) {
	var out GetBucketACLResult
	params := map[string]interface{}{}
	params["acl"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// SetBucketLifecycle sets the bucket's lifecycle.
//
// For more information, checks out following link:
// https://help.ksyun.com/document_detail/ks3/user_guide/manage_object/object_lifecycle.html
//
// bucketName    the bucket name.
// rules    the lifecycle rules. There're two kind of rules: absolute time expiration and relative time expiration in days and day/month/year respectively.
//          Check out sample/bucket_lifecycle.go for more details.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketLifecycle(bucketName string, rules []LifecycleRule, options ...Option) error {
	err := verifyLifecycleRules(rules)
	if err != nil {
		return err
	}
	lifecycleCfg := LifecycleConfiguration{Rules: rules}
	bs, err := xml.Marshal(lifecycleCfg)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// SetBucketLifecycleXml sets the bucket's lifecycle rule from xml config
func (client Client) SetBucketLifecycleXml(bucketName string, xmlBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(xmlBody))

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}
func (client Client) GetBucketLifecycleXml(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	out := string(body)
	return out, err
}

// DeleteBucketLifecycle deletes the bucket's lifecycle.
//
//
// bucketName    the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketLifecycle(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// GetBucketLifecycle gets the bucket's lifecycle settings.
//
// bucketName    the bucket name.
//
// GetBucketLifecycleResponse    the result object upon successful request. It's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketLifecycle(bucketName string, options ...Option) (GetBucketLifecycleResult, error) {
	var out GetBucketLifecycleResult
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)

	for _, rule := range out.Rules {
		if rule.Expiration != nil && &rule.Expiration.Date != nil && strings.Contains(rule.Expiration.Date, ".000") {
			rule.Expiration.Date = strings.ReplaceAll(rule.Expiration.Date, ".000", "")
		}
	}
	return out, err
}

// SetBucketReferer sets the bucket's referer whitelist and the flag if allowing empty referrer.
//
// To avoid stealing link on KS3 data, KS3 supports the HTTP referrer header. A whitelist referrer could be set either by API or web console, as well as
// the allowing empty referrer flag. Note that this applies to requests from webbrowser only.
// For example, for a bucket os-example and its referrer http://www.ksyun.com, all requests from this URL could access the bucket.
// For more information, please check out this link :
// https://help.ksyun.com/document_detail/ks3/user_guide/security_management/referer.html
//
// bucketName    the bucket name.
// referers    the referrer white list. A bucket could have a referrer list and each referrer supports one '*' and multiple '?' as wildcards.
//             The sample could be found in sample/bucket_referer.go
// allowEmptyReferer    the flag of allowing empty referrer. By default it's true.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketReferer(bucketName string, referers []string, allowEmptyReferer bool, options ...Option) error {
	rxml := RefererXML{}
	rxml.AllowEmptyReferer = allowEmptyReferer
	if referers == nil {
		rxml.RefererList = append(rxml.RefererList, "")
	} else {
		for _, referer := range referers {
			rxml.RefererList = append(rxml.RefererList, referer)
		}
	}

	bs, err := xml.Marshal(rxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["referer"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketReferer gets the bucket's referrer white list.
//
// bucketName    the bucket name.
//
// GetBucketRefererResponse    the result object upon successful request. It's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketReferer(bucketName string, options ...Option) (GetBucketRefererResult, error) {
	var out GetBucketRefererResult
	params := map[string]interface{}{}
	params["referer"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// SetBucketLogging sets the bucket logging settings.
//
// KS3 could automatically store the access log. Only the bucket owner could enable the logging.
// Once enabled, KS3 would save all the access log into hourly log files in a specified bucket.
// For more information, please check out https://help.ksyun.com/document_detail/ks3/user_guide/security_management/logging.html
//
// bucketName    bucket name to enable the log.
// targetBucket    the target bucket name to store the log files.
// targetPrefix    the log files' prefix.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketLogging(bucketName, targetBucket, targetPrefix string,
	isEnable bool, options ...Option) error {
	var err error
	var bs []byte
	if isEnable {
		lxml := LoggingXML{}
		lxml.LoggingEnabled.TargetBucket = targetBucket
		lxml.LoggingEnabled.TargetPrefix = targetPrefix
		lxml.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
		bs, err = xml.Marshal(lxml)
	} else {
		lxml := loggingXMLEmpty{}
		lxml.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
		bs, err = xml.Marshal(lxml)
	}

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// DeleteBucketLogging deletes the logging configuration to disable the logging on the bucket.
//
// bucketName    the bucket name to disable the logging.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketLogging(bucketName string, options ...Option) error {
	var err error
	var bs []byte

	lxml := loggingXMLEmpty{}
	lxml.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	bs, err = xml.Marshal(lxml)

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketLogging gets the bucket's logging settings
//
// bucketName    the bucket name
// GetBucketLoggingResponse    the result object upon successful request. It's only valid when error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketLogging(bucketName string, options ...Option) (GetBucketLoggingResult, error) {
	var out GetBucketLoggingResult
	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// SetBucketWebsite sets the bucket's static website's index and error page.
//
// KS3 supports static web site hosting for the bucket data. When the bucket is enabled with that, you can access the file in the bucket like the way to access a static website.
// For more information, please check out: https://help.ksyun.com/document_detail/ks3/user_guide/static_host_website.html
//
// bucketName    the bucket name to enable static web site.
// indexDocument    index page.
// errorDocument    error page.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketWebsite(bucketName, indexDocument, errorDocument string, options ...Option) error {
	wxml := WebsiteXML{}
	wxml.IndexDocument.Suffix = indexDocument
	wxml.ErrorDocument.Key = errorDocument

	bs, err := xml.Marshal(wxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// SetBucketWebsiteDetail sets the bucket's static website's detail
//
// KS3 supports static web site hosting for the bucket data. When the bucket is enabled with that, you can access the file in the bucket like the way to access a static website.
// For more information, please check out: https://help.ksyun.com/document_detail/ks3/user_guide/static_host_website.html
//
// bucketName the bucket name to enable static web site.
//
// wxml the website's detail
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketWebsiteDetail(bucketName string, wxml WebsiteXML, options ...Option) error {
	bs, err := xml.Marshal(wxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// SetBucketWebsiteXml sets the bucket's static website's rule
//
// KS3 supports static web site hosting for the bucket data. When the bucket is enabled with that, you can access the file in the bucket like the way to access a static website.
// For more information, please check out: https://help.ksyun.com/document_detail/ks3/user_guide/static_host_website.html
//
// bucketName the bucket name to enable static web site.
//
// wxml the website's detail
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketWebsiteXml(bucketName string, webXml string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(webXml))

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// DeleteBucketWebsite deletes the bucket's static web site settings.
//
// bucketName    the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketWebsite(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// GetBucketWebsite gets the bucket's default page (index page) and the error page.
//
// bucketName    the bucket name
//
// GetBucketWebsiteResponse    the result object upon successful request. It's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketWebsite(bucketName string, options ...Option) (GetBucketWebsiteResult, error) {
	var out GetBucketWebsiteResult
	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// GetBucketWebsiteXml gets the bucket's website config xml config.
//
// bucketName    the bucket name
//
// string   the bucket's xml config, It's only valid when error is nil.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketWebsiteXml(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	out := string(body)
	return out, err
}

func computeMD5Hash(input []byte) []byte {
	h := md5.New()
	h.Write(input)
	return h.Sum(nil)
}

func encodeAsString(bytes []byte) string {
	return base64.StdEncoding.EncodeToString(bytes)
}

// SetBucketCORS sets the bucket's CORS rules
//
// For more information, please check out https://help.ksyun.com/document_detail/ks3/user_guide/security_management/cors.html
//
// bucketName    the bucket name
// corsRules    the CORS rules to set. The related sample code is in sample/bucket_cors.go.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketCORS(bucketName string, corsRules []CORSRule, options ...Option) error {
	corsxml := CORSXML{}
	for _, v := range corsRules {
		cr := CORSRule{}
		cr.AllowedMethod = v.AllowedMethod
		cr.AllowedOrigin = v.AllowedOrigin
		cr.AllowedHeader = v.AllowedHeader
		cr.ExposeHeader = v.ExposeHeader
		cr.MaxAgeSeconds = v.MaxAgeSeconds
		corsxml.CORSRules = append(corsxml.CORSRules, cr)
	}
	corsxml.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	bs, err := xml.Marshal(corsxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// DeleteBucketCORS deletes the bucket's static website settings.
//
// bucketName    the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketCORS(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// GetBucketCORS gets the bucket's CORS settings.
//
// bucketName    the bucket name.
// GetBucketCORSResult    the result object upon successful request. It's only valid when error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketCORS(bucketName string, options ...Option) (GetBucketCORSResult, error) {
	var out GetBucketCORSResult
	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// GetBucketInfo gets the bucket information.
//
// bucketName    the bucket name.
// GetBucketInfoResult    the result object upon successful request. It's only valid when error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketInfo(bucketName string, options ...Option) (GetBucketInfoResult, error) {
	var out GetBucketInfoResult
	params := map[string]interface{}{}
	params["bucketInfo"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)

	// convert None to ""
	if err == nil {
		if out.BucketInfo.SseRule.KMSMasterKeyID == "None" {
			out.BucketInfo.SseRule.KMSMasterKeyID = ""
		}

		if out.BucketInfo.SseRule.SSEAlgorithm == "None" {
			out.BucketInfo.SseRule.SSEAlgorithm = ""
		}

		if out.BucketInfo.SseRule.KMSDataEncryption == "None" {
			out.BucketInfo.SseRule.KMSDataEncryption = ""
		}
	}
	return out, err
}

// HeadBucket head the bucket to check exists or not.
//
// bucketName    the bucket name.
// http.header   the response headers upon successful request. It's only valid when error is nil.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) HeadBucket(bucketName string, options ...Option) (http.Header, error) {
	params := map[string]interface{}{}
	resp, err := client.do("HEAD", bucketName, params, nil, nil, options...)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp.Headers, nil
}

// SetBucketVersioning set bucket versioning:Enabled、Suspended
// bucketName    the bucket name.
// error    it's nil if no error, otherwise it's an error object.
func (client Client) SetBucketVersioning(bucketName string, versioningConfig VersioningConfig, options ...Option) error {
	var err error
	var bs []byte
	bs, err = xml.Marshal(versioningConfig)

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["versioning"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketVersioning get bucket versioning status:Enabled、Suspended
// bucketName    the bucket name.
// error    it's nil if no error, otherwise it's an error object.
func (client Client) GetBucketVersioning(bucketName string, options ...Option) (GetBucketVersioningResult, error) {
	var out GetBucketVersioningResult
	params := map[string]interface{}{}
	params["versioning"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)

	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketTagging add tagging to bucket
// bucketName  name of bucket
// tagging    tagging to be added
// error        nil if success, otherwise error
func (client Client) SetBucketTagging(bucketName string, tagging Tagging, options ...Option) error {
	bs, err := xml.Marshal(tagging)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["tagging"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketTagging get tagging of the bucket
// bucketName  name of bucket
// error      nil if success, otherwise error
func (client Client) GetBucketTagging(bucketName string, options ...Option) (GetBucketTaggingResult, error) {
	var out GetBucketTaggingResult
	params := map[string]interface{}{}
	params["tagging"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// DeleteBucketTagging delete bucket tagging
// bucketName  name of bucket
// error      nil if success, otherwise error
//
func (client Client) DeleteBucketTagging(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["tagging"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// GetBucketStat get bucket stat
// bucketName    the bucket name.
// error    it's nil if no error, otherwise it's an error object.
func (client Client) GetBucketStat(bucketName string, options ...Option) (GetBucketStatResult, error) {
	var out GetBucketStatResult
	params := map[string]interface{}{}
	params["stat"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// GetBucketPolicy API operation for Object Storage Service.
//
// Get the policy from the bucket.
//
// bucketName 	 the bucket name.
//
// string		 return the bucket's policy, and it's only valid when error is nil.
//
// error   		 it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketPolicy(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["policy"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	out := string(body)
	return out, err
}

// SetBucketPolicy API operation for Object Storage Service.
//
// Set the policy from the bucket.
//
// bucketName the bucket name.
//
// policy the bucket policy.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketPolicy(bucketName string, policy string, options ...Option) error {
	params := map[string]interface{}{}
	params["policy"] = nil

	buffer := strings.NewReader(policy)

	resp, err := client.do("PUT", bucketName, params, nil, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// DeleteBucketPolicy API operation for Object Storage Service.
//
// Deletes the policy from the bucket.
//
// bucketName the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketPolicy(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["policy"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// SetBucketRequestPayment API operation for Object Storage Service.
//
// Set the requestPayment of bucket
//
// bucketName the bucket name.
//
// paymentConfig the payment configuration
//
// error it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketRequestPayment(bucketName string, paymentConfig RequestPaymentConfiguration, options ...Option) error {
	params := map[string]interface{}{}
	params["requestPayment"] = nil

	var bs []byte
	bs, err := xml.Marshal(paymentConfig)

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketRequestPayment API operation for Object Storage Service.
//
// Get bucket requestPayment
//
// bucketName the bucket name.
//
// RequestPaymentConfiguration the payment configuration
//
// error it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketRequestPayment(bucketName string, options ...Option) (RequestPaymentConfiguration, error) {
	var out RequestPaymentConfiguration
	params := map[string]interface{}{}
	params["requestPayment"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// GetUserQoSInfo API operation for Object Storage Service.
//
// Get user qos.
//
// UserQoSConfiguration the User Qos and range Information.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetUserQoSInfo(options ...Option) (UserQoSConfiguration, error) {
	var out UserQoSConfiguration
	params := map[string]interface{}{}
	params["qosInfo"] = nil

	resp, err := client.do("GET", "", params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// SetBucketQoSInfo API operation for Object Storage Service.
//
// Set Bucket Qos information.
//
// bucketName the bucket name.
//
// qosConf the qos configuration.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketQoSInfo(bucketName string, qosConf BucketQoSConfiguration, options ...Option) error {
	params := map[string]interface{}{}
	params["qosInfo"] = nil

	var bs []byte
	bs, err := xml.Marshal(qosConf)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentTpye := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentTpye

	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketQosInfo API operation for Object Storage Service.
//
// Get Bucket Qos information.
//
// bucketName the bucket name.
//
// BucketQoSConfiguration the  return qos configuration.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketQosInfo(bucketName string, options ...Option) (BucketQoSConfiguration, error) {
	var out BucketQoSConfiguration
	params := map[string]interface{}{}
	params["qosInfo"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// DeleteBucketQosInfo API operation for Object Storage Service.
//
// Delete Bucket QoS information.
//
// bucketName the bucket name.
//
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketQosInfo(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["qosInfo"] = nil

	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//-------------------------------- Bucket Inventory --------------------------------

// PutBucketInventory API operation for Object Storage Service
//
// Put the Bucket inventory.
//
// bucketName tht bucket name.
//
// inventoryConfig the inventory configuration.
//
// error    it's nil if no error, otherwise it's an error.
//
func (client Client) PutBucketInventory(bucketName string, inventoryConfig InventoryConfiguration, options ...Option) error {
	params := map[string]interface{}{}
	params["inventory"] = nil
	params["id"] = inventoryConfig.Id

	var bs []byte
	bs, err := xml.Marshal(inventoryConfig)

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketInventory API operation for Object Storage Service
//
// Get the Bucket inventory.
//
// bucketName tht bucket name.
//
// inventoryId the inventory id.
//
// InventoryConfiguration the inventory configuration.
//
// error    it's nil if no error, otherwise it's an error.
//
func (client Client) GetBucketInventory(bucketName string, inventoryId string, options ...Option) (InventoryConfiguration, error) {
	var out InventoryConfiguration
	params := map[string]interface{}{}
	params["inventory"] = nil
	params["id"] = inventoryId

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// ListBucketInventory API operation for Object Storage Service
//
// List the Bucket inventory.
//
// bucketName tht bucket name.
//
// continuationToken the users token.
//
// ListInventoryConfigurationsResult list all inventory configuration by .
//
// error    it's nil if no error, otherwise it's an error.
//
func (client Client) ListBucketInventory(bucketName, continuationToken string, options ...Option) (ListInventoryConfigurationsResult, error) {
	var out ListInventoryConfigurationsResult
	params := map[string]interface{}{}
	params["inventory"] = nil
	if continuationToken == "" {
		params["continuation-token"] = nil
	} else {
		params["continuation-token"] = continuationToken
	}

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// DeleteBucketInventory API operation for Object Storage Service.
//
// Delete Bucket inventory information.
//
// bucketName tht bucket name.
//
// inventoryId the inventory id.
//
// error    it's nil if no error, otherwise it's an error.
//
func (client Client) DeleteBucketInventory(bucketName, inventoryId string, options ...Option) error {
	params := map[string]interface{}{}
	params["inventory"] = nil
	params["id"] = inventoryId

	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// SetBucketAsyncTask API operation for set async fetch task
//
// bucketName tht bucket name.
//
// asynConf  configruation
//
// error  it's nil if success, otherwise it's an error.
func (client Client) SetBucketAsyncTask(bucketName string, asynConf AsyncFetchTaskConfiguration, options ...Option) (AsyncFetchTaskResult, error) {
	var out AsyncFetchTaskResult
	params := map[string]interface{}{}
	params["asyncFetch"] = nil

	var bs []byte
	bs, err := xml.Marshal(asynConf)

	if err != nil {
		return out, err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	resp, err := client.do("POST", bucketName, params, headers, buffer, options...)

	if err != nil {
		return out, err
	}

	defer resp.Body.Close()
	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// GetBucketAsyncTask API operation for set async fetch task
//
// bucketName tht bucket name.
//
// taskid  returned by SetBucketAsyncTask
//
// error  it's nil if success, otherwise it's an error.
func (client Client) GetBucketAsyncTask(bucketName string, taskID string, options ...Option) (AsynFetchTaskInfo, error) {
	var out AsynFetchTaskInfo
	params := map[string]interface{}{}
	params["asyncFetch"] = nil

	headers := make(map[string]string)
	headers[HTTPHeaderKs3TaskID] = taskID
	resp, err := client.do("GET", bucketName, params, headers, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// InitiateBucketWorm creates bucket worm Configuration
// bucketName the bucket name.
// retentionDays the retention period in days
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) InitiateBucketWorm(bucketName string, retentionDays int, options ...Option) (string, error) {
	var initiateWormConf InitiateWormConfiguration
	initiateWormConf.RetentionPeriodInDays = retentionDays

	var respHeader http.Header
	isOptSet, _, _ := IsOptionSet(options, responseHeader)
	if !isOptSet {
		options = append(options, GetResponseHeader(&respHeader))
	}

	bs, err := xml.Marshal(initiateWormConf)
	if err != nil {
		return "", err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["worm"] = nil

	resp, err := client.do("POST", bucketName, params, headers, buffer, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respOpt, _ := FindOption(options, responseHeader, nil)
	wormID := ""
	err = CheckRespCode(resp.StatusCode, []int{http.StatusOK})
	if err == nil && respOpt != nil {
		wormID = (respOpt.(*http.Header)).Get("x-ks3-worm-id")
	}
	return wormID, err
}

// AbortBucketWorm delete bucket worm Configuration
// bucketName the bucket name.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) AbortBucketWorm(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["worm"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// CompleteBucketWorm complete bucket worm Configuration
// bucketName the bucket name.
// wormID the worm id
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) CompleteBucketWorm(bucketName string, wormID string, options ...Option) error {
	params := map[string]interface{}{}
	params["wormId"] = wormID
	resp, err := client.do("POST", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// ExtendBucketWorm exetend bucket worm Configuration
// bucketName the bucket name.
// retentionDays the retention period in days
// wormID the worm id
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) ExtendBucketWorm(bucketName string, retentionDays int, wormID string, options ...Option) error {
	var extendWormConf ExtendWormConfiguration
	extendWormConf.RetentionPeriodInDays = retentionDays

	bs, err := xml.Marshal(extendWormConf)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["wormId"] = wormID
	params["wormExtend"] = nil

	resp, err := client.do("POST", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketWorm get bucket worm Configuration
// bucketName the bucket name.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketWorm(bucketName string, options ...Option) (WormConfiguration, error) {
	var out WormConfiguration
	params := map[string]interface{}{}
	params["worm"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// SetBucketTransferAcc set bucket transfer acceleration configuration
// bucketName the bucket name.
// accConf bucket transfer acceleration configuration
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) SetBucketTransferAcc(bucketName string, accConf TransferAccConfiguration, options ...Option) error {
	bs, err := xml.Marshal(accConf)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["transferAcceleration"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketTransferAcc get bucket transfer acceleration configuration
// bucketName the bucket name.
// accConf bucket transfer acceleration configuration
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketTransferAcc(bucketName string, options ...Option) (TransferAccConfiguration, error) {
	var out TransferAccConfiguration
	params := map[string]interface{}{}
	params["transferAcceleration"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

// DeleteBucketTransferAcc delete bucket transfer acceleration configuration
// bucketName the bucket name.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketTransferAcc(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["transferAcceleration"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//-------------------------------- Bucket Replication --------------------------------

// PutBucketReplication put bucket replication configuration
// bucketName    the bucket name.
// replication    the replication configuration.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) PutBucketReplication(bucketName string, replication Replication, options ...Option) error {
	bs, err := xml.Marshal(replication)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["crr"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// PutBucketReplicationXml put bucket replication configuration
// bucketName    the bucket name.
// xmlBody    the replication configuration.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) PutBucketReplicationXml(bucketName string, xmlBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(xmlBody))

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["crr"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketReplication get bucket replication configuration
// bucketName    the bucket name.
// GetBucketReplicationResult    the replication configuration.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketReplication(bucketName string, options ...Option) (GetBucketReplicationResult, error) {
	var out GetBucketReplicationResult
	params := map[string]interface{}{}
	params["crr"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)

	return out, err
}

// GetBucketReplicationXml get bucket replication configuration
// bucketName    the bucket name.
// string    the replication configuration.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketReplicationXml(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["crr"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), err
}

// DeleteBucketReplication delete bucket replication configuration
// bucketName    the bucket name.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) DeleteBucketReplication(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["crr"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// GetBucketReplicationLocation get the locations of the target bucket that can be copied to
// bucketName    the bucket name.
// string    the locations of the target bucket that can be copied to.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketReplicationLocation(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["replicationLocation"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), err
}

// GetBucketReplicationProgress get the replication progress of bucket
// bucketName    the bucket name.
// ruleId    the ID of the replication configuration.
// string    the replication progress of bucket.
// error    it's nil if no error, otherwise it's an error object.
//
func (client Client) GetBucketReplicationProgress(bucketName string, ruleId string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["replicationProgress"] = nil
	if ruleId != "" {
		params["rule-id"] = ruleId
	}

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), err
}

// GetBucketCname get bucket's binding cname
// bucketName    the bucket name.
// string    the xml configuration of bucket.
// error    it's nil if no error, otherwise it's an error object.
func (client Client) GetBucketCname(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["cname"] = nil

	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), err
}

//-------------------------------- Bucket Retention --------------------------------

func (client Client) PutBucketRetention(bucketName string, retentionRule RetentionRule, options ...Option) error {
	retentionCfg := RetentionConfiguration{Rule: retentionRule}
	bs, err := xml.Marshal(retentionCfg)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["retention"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

// PutBucketRetentionXml sets the bucket's retention rule from xml config
func (client Client) PutBucketRetentionXml(bucketName string, xmlBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(xmlBody))

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["retention"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

func (client Client) GetBucketRetention(bucketName string, options ...Option) (GetBucketRetentionResult, error) {
	var out GetBucketRetentionResult
	params := map[string]interface{}{}
	params["retention"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)

	return out, err
}

// GetBucketRetentionXml gets the bucket's retention rule in xml format
func (client Client) GetBucketRetentionXml(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["retention"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	out := string(body)
	return out, err
}

//-------------------------------- Bucket Mirror --------------------------------

func (client Client) PutBucketMirror(bucketName string, bucketMirror BucketMirror, options ...Option) error {
	bs, err := json.Marshal(bucketMirror)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["mirror"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

func (client Client) PutBucketMirrorJson(bucketName string, jsonBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(jsonBody))

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["mirror"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

func (client Client) GetBucketMirror(bucketName string, options ...Option) (GetBucketMirrorResult, error) {
	var out GetBucketMirrorResult
	params := map[string]interface{}{}
	params["mirror"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = jsonUnmarshal(resp.Body, &out)
	return out, err
}

func (client Client) GetBucketMirrorJson(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["mirror"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	out := string(body)
	return out, err
}

func (client Client) DeleteBucketMirror(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["mirror"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

// LimitUploadSpeed set upload bandwidth limit speed,default is 0,unlimited
// upSpeed KB/s, 0 is unlimited,default is 0
// error it's nil if success, otherwise failure
func (client Client) LimitUploadSpeed(upSpeed int) error {
	if client.Config == nil {
		return fmt.Errorf("client config is nil")
	}
	return client.Config.LimitUploadSpeed(upSpeed)
}

// LimitDownloadSpeed set download bandwidth limit speed,default is 0,unlimited
// downSpeed KB/s, 0 is unlimited,default is 0
// error it's nil if success, otherwise failure
func (client Client) LimitDownloadSpeed(downSpeed int) error {
	if client.Config == nil {
		return fmt.Errorf("client config is nil")
	}
	return client.Config.LimitDownloadSpeed(downSpeed)
}

// UseCname sets the flag of using CName. By default it's false.
//
// isUseCname    true: the endpoint has the CName, false: the endpoint does not have cname. Default is false.
//
func UseCname(isUseCname bool) ClientOption {
	return func(client *Client) {
		client.Config.IsCname = isUseCname
		client.Conn.Url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy, client.Config.PathStyleAccess)
	}
}

// PathStyleAccess sets the flag of using path style. By default, it's false
//
// pathStyleAccess true: use second level domain, false: use third level domain, Default is false.
//
func PathStyleAccess(pathStyleAccess bool) ClientOption {
	return func(client *Client) {
		client.Config.PathStyleAccess = pathStyleAccess
		client.Conn.Url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy, client.Config.PathStyleAccess)
	}
}

// Timeout sets the HTTP timeout in seconds.
//
// connectTimeoutSec    HTTP timeout in seconds. Default is 10 seconds. 0 means infinite (not recommended)
// readWriteTimeout    HTTP read or write's timeout in seconds. Default is 20 seconds. 0 means infinite.
//
func Timeout(connectTimeoutSec, readWriteTimeout int64) ClientOption {
	return func(client *Client) {
		client.Config.HTTPTimeout.ConnectTimeout =
			time.Second * time.Duration(connectTimeoutSec)
		client.Config.HTTPTimeout.ReadWriteTimeout =
			time.Second * time.Duration(readWriteTimeout)
		client.Config.HTTPTimeout.HeaderTimeout =
			time.Second * time.Duration(readWriteTimeout)
		client.Config.HTTPTimeout.IdleConnTimeout =
			time.Second * time.Duration(readWriteTimeout)
		client.Config.HTTPTimeout.LongTimeout =
			time.Second * time.Duration(readWriteTimeout*10)
	}
}

// SecurityToken sets the temporary user's SecurityToken.
//
// token    STS token
//
func SecurityToken(token string) ClientOption {
	return func(client *Client) {
		client.Config.SecurityToken = strings.TrimSpace(token)
	}
}

// EnableMD5 enables MD5 validation.
//
// isEnableMD5    true: enable MD5 validation; false: disable MD5 validation.
//
func EnableMD5(isEnableMD5 bool) ClientOption {
	return func(client *Client) {
		client.Config.IsEnableMD5 = isEnableMD5
	}
}

// MD5ThresholdCalcInMemory sets the memory usage threshold for computing the MD5, default is 16MB.
//
// threshold    the memory threshold in bytes. When the uploaded content is more than 16MB, the temp file is used for computing the MD5.
//
func MD5ThresholdCalcInMemory(threshold int64) ClientOption {
	return func(client *Client) {
		client.Config.MD5Threshold = threshold
	}
}

// EnableCRC enables the CRC checksum. Default is true.
//
// isEnableCRC    true: enable CRC checksum; false: disable the CRC checksum.
//
func EnableCRC(isEnableCRC bool) ClientOption {
	return func(client *Client) {
		client.Config.IsEnableCRC = isEnableCRC
	}
}

// UserAgent specifies UserAgent. The default is ksyun-sdk-go/1.2.0 (windows/-/amd64;go1.5.2).
//
// userAgent    the user agent string.
//
func UserAgent(userAgent string) ClientOption {
	return func(client *Client) {
		client.Config.UserAgent = userAgent
		client.Config.UserSetUa = true
	}
}

// ProxyFromEnvironment sets the proxy from the environment.
func ProxyFromEnvironment() ClientOption {
	return func(client *Client) {
		client.Config.IsUseProxy = true
		client.Config.ProxyFromEnvironment = true
		client.Conn.Url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy, client.Config.PathStyleAccess)
	}
}

// Proxy sets the proxy (optional). The default is not using proxy.
//
// proxyHost    the proxy host in the format "host:port". For example, proxy.com:80 .
//
func Proxy(proxyHost string) ClientOption {
	return func(client *Client) {
		client.Config.IsUseProxy = true
		client.Config.ProxyHost = proxyHost
		client.Conn.Url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy, client.Config.PathStyleAccess)
	}
}

// AuthProxy sets the proxy information with user name and password.
//
// proxyHost    the proxy host in the format "host:port". For example, proxy.com:80 .
// proxyUser    the proxy user name.
// proxyPassword    the proxy password.
//
func AuthProxy(proxyHost, proxyUser, proxyPassword string) ClientOption {
	return func(client *Client) {
		client.Config.IsUseProxy = true
		client.Config.ProxyHost = proxyHost
		client.Config.IsAuthProxy = true
		client.Config.ProxyUser = proxyUser
		client.Config.ProxyPassword = proxyPassword
		client.Conn.Url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy, client.Config.PathStyleAccess)
	}
}

//
// HTTPClient sets the http.Client in use to the one passed in
//
func HTTPClient(HTTPClient *http.Client) ClientOption {
	return func(client *Client) {
		client.HTTPClient = HTTPClient
	}
}

//
// SetLogLevel sets the ks3 sdk log level
//
func SetLogLevel(LogLevel int) ClientOption {
	return func(client *Client) {
		client.Config.LogLevel = LogLevel
	}
}

//
// SetLogger sets the ks3 sdk logger
//
func SetLogger(Logger *log.Logger) ClientOption {
	return func(client *Client) {
		client.Config.Logger = Logger
	}
}

// SetCredentialsProvider sets funciton for get the user's ak
func SetCredentialsProvider(provider CredentialsProvider) ClientOption {
	return func(client *Client) {
		client.Config.CredentialsProvider = provider
	}
}

// SetLocalAddr sets funciton for local addr
func SetLocalAddr(localAddr net.Addr) ClientOption {
	return func(client *Client) {
		client.Config.LocalAddr = localAddr
	}
}

// AuthVersion  sets auth version: v1 or v2 signature which ks3_server needed
func AuthVersion(authVersion AuthVersionType) ClientOption {
	return func(client *Client) {
		client.Config.AuthVersion = authVersion
	}
}

// AdditionalHeaders sets special http headers needed to be signed
func AdditionalHeaders(headers []string) ClientOption {
	return func(client *Client) {
		client.Config.AdditionalHeaders = headers
	}
}

// only effective from go1.7 onward,RedirectEnabled set http redirect enabled or not
func RedirectEnabled(enabled bool) ClientOption {
	return func(client *Client) {
		client.Config.RedirectEnabled = enabled
	}
}

// skip verifying tls certificate file
func InsecureSkipVerify(enabled bool) ClientOption {
	return func(client *Client) {
		client.Config.InsecureSkipVerify = enabled
	}
}

// Private
func (client Client) do(method, bucketName string, params map[string]interface{},
	headers map[string]string, data io.Reader, options ...Option) (*Response, error) {
	err := CheckBucketName(bucketName)
	if len(bucketName) > 0 && err != nil {
		return nil, err
	}

	// option headers
	addHeaders := make(map[string]string)
	err = handleOptions(addHeaders, options)
	if err != nil {
		return nil, err
	}

	// merge header
	if headers == nil {
		headers = make(map[string]string)
	}

	for k, v := range addHeaders {
		if _, ok := headers[k]; !ok {
			headers[k] = v
		}
	}

	resp, err := client.Conn.Do(method, bucketName, "", params, headers, data, 0, nil)

	// get response header
	respHeader, _ := FindOption(options, responseHeader, nil)
	if respHeader != nil {
		pRespHeader := respHeader.(*http.Header)
		if resp != nil {
			*pRespHeader = resp.Headers
		}
	}

	return resp, err
}
