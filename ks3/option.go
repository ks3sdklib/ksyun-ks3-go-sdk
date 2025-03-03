package ks3

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type optionType string

const (
	optionParam   optionType = "HTTPParameter" // URL parameter
	optionHTTP    optionType = "HTTPHeader"    // HTTP header
	optionContext optionType = "HTTPContext"   // context
	optionArg     optionType = "FuncArgument"  // Function argument
)

const (
	deleteObjectsQuiet = "delete-objects-quiet"
	routineNum         = "x-routine-num"
	checkpointConfig   = "x-cp-config"
	initCRC64          = "init-crc64"
	progressListener   = "x-progress-listener"
	storageClass       = "storage-class"
	bucketType         = "bucket-type"
	responseHeader     = "x-response-header"
	redundancyType     = "redundancy-type"
	objectHashFunc     = "object-hash-func"
	contextArg         = "x-context-arg"
	disableTempFileFlag = "disable-temp-file"
)

type (
	optionValue struct {
		Value interface{}
		Type  optionType
	}

	// Option HTTP option
	Option func(map[string]optionValue) error
)

// ACL is an option to set X-Kss-Acl header
func ACL(acl ACLType) Option {
	return setHeader(HTTPHeaderKs3ACL, string(acl))
}

// ContentType is an option to set Content-Type header
func ContentType(value string) Option {
	return setHeader(HTTPHeaderContentType, value)
}

// ContentLength is an option to set Content-Length header
func ContentLength(length int64) Option {
	return setHeader(HTTPHeaderContentLength, strconv.FormatInt(length, 10))
}

// CacheControl is an option to set Cache-Control header
func CacheControl(value string) Option {
	return setHeader(HTTPHeaderCacheControl, value)
}

// ContentDisposition is an option to set Content-Disposition header
func ContentDisposition(value string) Option {
	return setHeader(HTTPHeaderContentDisposition, value)
}

// ContentEncoding is an option to set Content-Encoding header
func ContentEncoding(value string) Option {
	return setHeader(HTTPHeaderContentEncoding, value)
}

// ContentLanguage is an option to set Content-Language header
func ContentLanguage(value string) Option {
	return setHeader(HTTPHeaderContentLanguage, value)
}

// ContentMD5 is an option to set Content-MD5 header
func ContentMD5(value string) Option {
	return setHeader(HTTPHeaderContentMD5, value)
}

// Expires is an option to set Expires header
func Expires(t time.Time) Option {
	return setHeader(HTTPHeaderExpires, t.Format(http.TimeFormat))
}

// Meta is an option to set Meta header
func Meta(key, value string) Option {
	return setHeader(HTTPHeaderKs3MetaPrefix+key, value)
}

// Meta is an option to set Meta header
func NoMeta(key, value string) Option {
	return setHeader(HTTPHeaderKs3Prefix+key, value)
}

// Range is an option to set Range header, [start, end]
func Range(start, end int64) Option {
	return setHeader(HTTPHeaderRange, fmt.Sprintf("bytes=%d-%d", start, end))
}

// NormalizedRange is an option to set Range header, such as 1024-2048 or 1024- or -2048
func NormalizedRange(nr string) Option {
	return setHeader(HTTPHeaderRange, fmt.Sprintf("bytes=%s", strings.TrimSpace(nr)))
}

// NormalizedRange is an option to set Range header, such as 1024-2048 or 1024- or -2048
func NormalizedFixRange(nr string) Option {
	return setHeader(HTTPHeaderRange, nr)
}

// AcceptEncoding is an option to set Accept-Encoding header
func AcceptEncoding(value string) Option {
	return setHeader(HTTPHeaderAcceptEncoding, value)
}

// IfModifiedSince is an option to set If-Modified-Since header
func IfModifiedSince(t time.Time) Option {
	return setHeader(HTTPHeaderIfModifiedSince, t.Format(http.TimeFormat))
}

// IfUnmodifiedSince is an option to set If-Unmodified-Since header
func IfUnmodifiedSince(t time.Time) Option {
	return setHeader(HTTPHeaderIfUnmodifiedSince, t.Format(http.TimeFormat))
}

// IfMatch is an option to set If-Match header
func IfMatch(value string) Option {
	return setHeader(HTTPHeaderIfMatch, value)
}

// IfNoneMatch is an option to set IfNoneMatch header
func IfNoneMatch(value string) Option {
	return setHeader(HTTPHeaderIfNoneMatch, value)
}

// CopySource is an option to set X-Kss-Copy-Source header
func CopySource(sourceBucket, sourceObject string) Option {
	return setHeader(HTTPHeaderKs3CopySource, "/"+sourceBucket+"/"+encodeKS3Str(sourceObject))
}

// CopySourceVersion is an option to set X-Kss-Copy-Source header,include versionId
func CopySourceVersion(sourceBucket, sourceObject string, versionId string) Option {
	return setHeader(HTTPHeaderKs3CopySource, "/"+sourceBucket+"/"+encodeKS3Str(sourceObject)+"?"+"versionId="+versionId)
}

// CopySourceRange is an option to set X-Kss-Copy-Source header
func CopySourceRange(startPosition, partSize int64) Option {
	val := "bytes=" + strconv.FormatInt(startPosition, 10) + "-" +
		strconv.FormatInt((startPosition+partSize-1), 10)
	return setHeader(HTTPHeaderKs3CopySourceRange, val)
}

// CopySourceIfMatch is an option to set X-Kss-Copy-Source-If-Match header
func CopySourceIfMatch(value string) Option {
	return setHeader(HTTPHeaderKs3CopySourceIfMatch, value)
}

// CopySourceIfNoneMatch is an option to set X-Kss-Copy-Source-If-None-Match header
func CopySourceIfNoneMatch(value string) Option {
	return setHeader(HTTPHeaderKs3CopySourceIfNoneMatch, value)
}

// CopySourceIfModifiedSince is an option to set X-Kss-CopySource-If-Modified-Since header
func CopySourceIfModifiedSince(t time.Time) Option {
	return setHeader(HTTPHeaderKs3CopySourceIfModifiedSince, t.Format(http.TimeFormat))
}

// CopySourceIfUnmodifiedSince is an option to set X-Kss-Copy-Source-If-Unmodified-Since header
func CopySourceIfUnmodifiedSince(t time.Time) Option {
	return setHeader(HTTPHeaderKs3CopySourceIfUnmodifiedSince, t.Format(http.TimeFormat))
}

// MetadataDirective is an option to set X-Kss-Metadata-Directive header
func MetadataDirective(directive MetadataDirectiveType) Option {
	return setHeader(HTTPHeaderKs3MetadataDirective, string(directive))
}

// ServerSideEncryption is an option to set X-Kss-Server-Side-Encryption header
func ServerSideEncryption(value string) Option {
	return setHeader(HTTPHeaderKs3ServerSideEncryption, value)
}

// ServerSideEncryptionKeyID is an option to set X-Kss-Server-Side-Encryption-Key-Id header
func ServerSideEncryptionKeyID(value string) Option {
	return setHeader(HTTPHeaderKs3ServerSideEncryptionKeyID, value)
}

// ServerSideDataEncryption is an option to set X-Kss-Server-Side-Data-Encryption header
func ServerSideDataEncryption(value string) Option {
	return setHeader(HTTPHeaderKs3ServerSideDataEncryption, value)
}

// SSECAlgorithm is an option to set X-Kss-Server-Side-Encryption-Customer-Algorithm header
func SSECAlgorithm(value string) Option {
	return setHeader(HTTPHeaderSSECAlgorithm, value)
}

// SSECKey is an option to set X-Kss-Server-Side-Encryption-Customer-Key header
func SSECKey(value string) Option {
	return setHeader(HTTPHeaderSSECKey, value)
}

// SSECKeyMd5 is an option to set X-Kss-Server-Side-Encryption-Customer-Key-Md5 header
func SSECKeyMd5(value string) Option {
	return setHeader(HTTPHeaderSSECKeyMd5, value)
}

// ObjectACL is an option to set X-Kss-Object-Acl header
func ObjectACL(acl ACLType) Option {
	return setHeader(HTTPHeaderKs3ObjectACL, string(acl))
}

// symlinkTarget is an option to set X-Kss-Symlink-Target
func symlinkTarget(targetObjectKey string) Option {
	return setHeader(HTTPHeaderKs3SymlinkTarget, targetObjectKey)
}

// Origin is an option to set Origin header
func Origin(value string) Option {
	return setHeader(HTTPHeaderOrigin, value)
}

// ObjectStorageClass is an option to set the storage class of object
func ObjectStorageClass(storageClass StorageClassType) Option {
	return setHeader(HTTPHeaderKs3StorageClass, string(storageClass))
}

// Callback is an option to set callback values
func Callback(callback string) Option {
	return setHeader(HTTPHeaderKs3Callback, callback)
}

// CallbackVar is an option to set callback user defined values
func CallbackVar(callbackVar string) Option {
	return setHeader(HTTPHeaderKs3CallbackVar, callbackVar)
}

// RequestPayer is an option to set payer who pay for the request
func RequestPayer(payerType PayerType) Option {
	return setHeader(HTTPHeaderKs3Requester, strings.ToLower(string(payerType)))
}

// RequestPayerParam is an option to set payer who pay for the request
func RequestPayerParam(payerType PayerType) Option {
	return addParam(strings.ToLower(HTTPHeaderKs3Requester), strings.ToLower(string(payerType)))
}

// SetTagging is an option to set object tagging
func SetTagging(tagging Tagging) Option {
	if len(tagging.Tags) == 0 {
		return nil
	}

	taggingValue := ""
	for index, tag := range tagging.Tags {
		if index != 0 {
			taggingValue += "&"
		}
		taggingValue += url.QueryEscape(tag.Key) + "=" + url.QueryEscape(tag.Value)
	}
	return setHeader(HTTPHeaderKs3Tagging, taggingValue)
}

// TaggingDirective is an option to set X-Kss-Metadata-Directive header
func TaggingDirective(directive TaggingDirectiveType) Option {
	return setHeader(HTTPHeaderKs3TaggingDirective, string(directive))
}

// ACReqMethod is an option to set Access-Control-Request-Method header
func ACReqMethod(value string) Option {
	return setHeader(HTTPHeaderACReqMethod, value)
}

// ACReqHeaders is an option to set Access-Control-Request-Headers header
func ACReqHeaders(value string) Option {
	return setHeader(HTTPHeaderACReqHeaders, value)
}

// TrafficLimitHeader is an option to set X-Kss-Traffic-Limit
func TrafficLimitHeader(value int64) Option {
	return setHeader(HTTPHeaderKs3TrafficLimit, strconv.FormatInt(value, 10))
}

// UserAgentHeader is an option to set HTTPHeaderUserAgent
func UserAgentHeader(ua string) Option {
	return setHeader(HTTPHeaderUserAgent, ua)
}

// ForbidOverWrite  is an option to set X-Kss-Forbid-Overwrite
func ForbidOverWrite(forbidWrite bool) Option {
	if forbidWrite {
		return setHeader(HTTPHeaderKs3ForbidOverWrite, "true")
	} else {
		return setHeader(HTTPHeaderKs3ForbidOverWrite, "false")
	}
}

// RangeBehavior  is an option to set Range value, such as "standard"
func RangeBehavior(value string) Option {
	return setHeader(HTTPHeaderKs3RangeBehavior, value)
}

func PartHashCtxHeader(value string) Option {
	return setHeader(HTTPHeaderKs3HashCtx, value)
}

func PartMd5CtxHeader(value string) Option {
	return setHeader(HTTPHeaderKs3Md5Ctx, value)
}

func PartHashCtxParam(value string) Option {
	return addParam("x-kss-hash-ctx", value)
}

func PartMd5CtxParam(value string) Option {
	return addParam("x-kss-md5-ctx", value)
}

// Delimiter is an option to set delimiler parameter
func Delimiter(value string) Option {
	return addParam("delimiter", value)
}

// Marker is an option to set marker parameter
func Marker(value string) Option {
	return addParam("marker", value)
}

// MaxKeys is an option to set maxkeys parameter
func MaxKeys(value int) Option {
	return addParam("max-keys", strconv.Itoa(value))
}

// Prefix is an option to set prefix parameter
func Prefix(value string) Option {
	return addParam("prefix", value)
}

// EncodingType is an option to set encoding-type parameter
func EncodingType(value string) Option {
	return addParam("encoding-type", value)
}

// MaxUploads is an option to set max-uploads parameter
func MaxUploads(value int) Option {
	return addParam("max-uploads", strconv.Itoa(value))
}

// KeyMarker is an option to set key-marker parameter
func KeyMarker(value string) Option {
	return addParam("key-marker", value)
}

// VersionIdMarker is an option to set version-id-marker parameter
func VersionIdMarker(value string) Option {
	return addParam("version-id-marker", value)
}

// VersionId is an option to set versionId parameter
func VersionId(value string) Option {
	return addParam("versionId", value)
}

// TagKey is an option to set tag key parameter
func TagKey(value string) Option {
	return addParam("tag-key", value)
}

// TagValue is an option to set tag value parameter
func TagValue(value string) Option {
	return addParam("tag-value", value)
}

// UploadIDMarker is an option to set upload-id-marker parameter
func UploadIDMarker(value string) Option {
	return addParam("upload-id-marker", value)
}

// MaxParts is an option to set max-parts parameter
func MaxParts(value int) Option {
	return addParam("max-parts", strconv.Itoa(value))
}

// PartNumberMarker is an option to set part-number-marker parameter
func PartNumberMarker(value int) Option {
	return addParam("part-number-marker", strconv.Itoa(value))
}

// Sequential is an option to set sequential parameter for InitiateMultipartUpload
func Sequential() Option {
	return addParam("sequential", "")
}

// WithHashContext is an option to set withHashContext parameter for InitiateMultipartUpload
func WithHashContext() Option {
	return addParam("withHashContext", "")
}

// EnableMd5 is an option to set x-kss-enable-md5 parameter for InitiateMultipartUpload
func EnableMd5() Option {
	return addParam("x-kss-enable-md5", "")
}

// EnableSha1 is an option to set x-kss-enable-sha1 parameter for InitiateMultipartUpload
func EnableSha1() Option {
	return addParam("x-kss-enable-sha1", "")
}

// EnableSha256 is an option to set x-kss-enable-sha256 parameter for InitiateMultipartUpload
func EnableSha256() Option {
	return addParam("x-kss-enable-sha256", "")
}

// ListType is an option to set List-type parameter for ListObjectsV2
func ListType(value int) Option {
	return addParam("list-type", strconv.Itoa(value))
}

// StartAfter is an option to set start-after parameter for ListObjectsV2
func StartAfter(value string) Option {
	return addParam("start-after", value)
}

// ContinuationToken is an option to set Continuation-token parameter for ListObjectsV2
func ContinuationToken(value string) Option {
	if value == "" {
		return addParam("continuation-token", nil)
	}
	return addParam("continuation-token", value)
}

// FetchOwner is an option to set Fetch-owner parameter for ListObjectsV2
func FetchOwner(value bool) Option {
	if value {
		return addParam("fetch-owner", "true")
	}
	return addParam("fetch-owner", "false")
}

// DeleteObjectsQuiet false:DeleteObjects in verbose mode; true:DeleteObjects in quite mode. Default is false.
func DeleteObjectsQuiet(isQuiet bool) Option {
	return addArg(deleteObjectsQuiet, isQuiet)
}

// StorageClass bucket storage class
func StorageClass(value StorageClassType) Option {
	return addArg(storageClass, value)
}

// StorageClass bucket storage class
func BucketTypeClass(value BucketType) Option {
	return addArg(bucketType, value)
}

// RedundancyType bucket data redundancy type
func ObjectHashFunc(value ObjecthashFuncType) Option {
	return addArg(objectHashFunc, value)
}

// WithContext returns an option that sets the context for requests.
func WithContext(ctx context.Context) Option {
	return addArg(contextArg, ctx)
}

// Checkpoint configuration
type cpConfig struct {
	IsEnable bool
	FilePath string
	DirPath  string
}

// Checkpoint sets the isEnable flag and checkpoint file path for DownloadFile/UploadFile.
func Checkpoint(isEnable bool, filePath string) Option {
	return addArg(checkpointConfig, &cpConfig{IsEnable: isEnable, FilePath: filePath})
}

// CheckpointDir sets the isEnable flag and checkpoint dir path for DownloadFile/UploadFile.
func CheckpointDir(isEnable bool, dirPath string) Option {
	return addArg(checkpointConfig, &cpConfig{IsEnable: isEnable, DirPath: dirPath})
}

// Routines DownloadFile/UploadFile routine count
func Routines(n int) Option {
	return addArg(routineNum, n)
}

// InitCRC Init AppendObject CRC
func InitCRC(initCRC uint64) Option {
	return addArg(initCRC64, initCRC)
}

// Progress set progress listener
func Progress(listener ProgressListener) Option {
	return addArg(progressListener, listener)
}

// GetResponseHeader for get response http header
func GetResponseHeader(respHeader *http.Header) Option {
	return addArg(responseHeader, respHeader)
}

// DisableTempFile is an option to disable temp file
func DisableTempFile(value bool) Option {
	if value {
		return addArg(disableTempFileFlag, "true")
	}
	return addArg(disableTempFileFlag, "false")
}

// ResponseContentType is an option to set response-content-type param
func ResponseContentType(value string) Option {
	return addParam("response-content-type", value)
}

// ResponseContentLanguage is an option to set response-content-language param
func ResponseContentLanguage(value string) Option {
	return addParam("response-content-language", value)
}

// ResponseExpires is an option to set response-expires param
func ResponseExpires(value string) Option {
	return addParam("response-expires", value)
}

// ResponseCacheControl is an option to set response-cache-control param
func ResponseCacheControl(value string) Option {
	return addParam("response-cache-control", value)
}

// ResponseContentDisposition is an option to set response-content-disposition param
func ResponseContentDisposition(value string) Option {
	return addParam("response-content-disposition", value)
}

// ResponseContentEncoding is an option to set response-content-encoding param
func ResponseContentEncoding(value string) Option {
	return addParam("response-content-encoding", value)
}

// Process is an option to set x-kss-process param
func Process(value string) Option {
	return addParam("x-kss-process", value)
}

// TrafficLimitParam is a option to set x-kss-traffic-limit
func TrafficLimitParam(value int64) Option {
	return addParam("x-kss-traffic-limit", strconv.FormatInt(value, 10))
}

// SetHeader Allow users to set personalized http headers
func SetHeader(key string, value interface{}) Option {
	return setHeader(key, value)
}

// AddParam Allow users to set personalized http params
func AddParam(key string, value interface{}) Option {
	return addParam(key, value)
}

func setHeader(key string, value interface{}) Option {
	return func(params map[string]optionValue) error {
		if value == nil {
			return nil
		}
		params[key] = optionValue{value, optionHTTP}
		return nil
	}
}

func addParam(key string, value interface{}) Option {
	return func(params map[string]optionValue) error {
		if value == nil {
			return nil
		}
		params[key] = optionValue{value, optionParam}
		return nil
	}
}

func addArg(key string, value interface{}) Option {
	return func(params map[string]optionValue) error {
		if value == nil {
			return nil
		}
		params[key] = optionValue{value, optionArg}
		return nil
	}
}

func handleOptions(headers map[string]string, options []Option) error {
	params := map[string]optionValue{}
	for _, option := range options {
		if option != nil {
			if err := option(params); err != nil {
				return err
			}
		}
	}

	for k, v := range params {
		if v.Type == optionHTTP {
			headers[k] = v.Value.(string)
		}
	}
	return nil
}

func GetRawParams(options []Option) (map[string]interface{}, error) {
	// Option
	params := map[string]optionValue{}
	for _, option := range options {
		if option != nil {
			if err := option(params); err != nil {
				return nil, err
			}
		}
	}

	paramsm := map[string]interface{}{}
	// Serialize
	for k, v := range params {
		if v.Type == optionParam {
			vs := params[k]
			paramsm[k] = vs.Value.(string)
		}
	}

	return paramsm, nil
}

func FindOption(options []Option, param string, defaultVal interface{}) (interface{}, error) {
	params := map[string]optionValue{}
	for _, option := range options {
		if option != nil {
			if err := option(params); err != nil {
				return nil, err
			}
		}
	}

	if val, ok := params[param]; ok {
		return val.Value, nil
	}
	return defaultVal, nil
}

func IsOptionSet(options []Option, option string) (bool, interface{}, error) {
	params := map[string]optionValue{}
	for _, option := range options {
		if option != nil {
			if err := option(params); err != nil {
				return false, nil, err
			}
		}
	}

	if val, ok := params[option]; ok {
		return true, val.Value, nil
	}
	return false, nil, nil
}

func DeleteOption(options []Option, strKey string) []Option {
	var outOption []Option
	params := map[string]optionValue{}
	for _, option := range options {
		if option != nil {
			option(params)
			_, exist := params[strKey]
			if !exist {
				outOption = append(outOption, option)
			} else {
				delete(params, strKey)
			}
		}
	}
	return outOption
}

func GetRequestId(header http.Header) string {
	return header.Get("x-kss-request-id")
}

func GetVersionId(header http.Header) string {
	return header.Get("x-kss-version-id")
}

func GetCopySrcVersionId(header http.Header) string {
	return header.Get("x-kss-copy-source-version-id")
}

func GetDeleteMark(header http.Header) bool {
	value := header.Get("x-kss-delete-marker")
	if strings.ToUpper(value) == "TRUE" {
		return true
	}
	return false
}

func GetQosDelayTime(header http.Header) string {
	return header.Get("x-kss-qos-delay-time")
}

// AllowSameActionOverLap is an option to set X-Kss-Allow-Same-Action-Overlap
func AllowSameActionOverLap(enabled bool) Option {
	if enabled {
		return setHeader(HTTPHeaderAllowSameActionOverLap, "true")
	} else {
		return setHeader(HTTPHeaderAllowSameActionOverLap, "false")
	}
}

// RetentionId is an option to set X-Kss-Retention-Id header
func RetentionId(value string) Option {
	return setHeader(HTTPHeaderKs3RetentionId, value)
}

// RetentionOverwrite is an option to set X-Kss-Retention-Overwrite header
func RetentionOverwrite(value bool) Option {
	if value {
		return setHeader(HTTPHeaderKs3RetentionOverwrite, "true")
	} else {
		return setHeader(HTTPHeaderKs3RetentionOverwrite, "false")
	}
}

// getDisableTempFile get disable temp file value
func getDisableTempFile(options []Option) bool {
	val, err := FindOption(options, disableTempFileFlag, "false")
	if err != nil || val == nil {
		return false
	}

	res, _ := strconv.ParseBool(val.(string))

	return res
}