package ks3

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	. "gopkg.in/check.v1"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Ks3BucketSuite struct {
	client        *Client
	bucket        *Bucket
	archiveBucket *Bucket
}

var _ = Suite(&Ks3BucketSuite{})

var (
	pastDate   = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	futureDate = time.Date(2049, time.January, 10, 23, 0, 0, 0, time.UTC)
)

// SetUpSuite runs once when the suite starts running.
func (s *Ks3BucketSuite) SetUpSuite(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	s.client = client

	s.client.CreateBucket(bucketName)

	err = s.client.CreateBucket(archiveBucketName, StorageClass(StorageArchive))
	c.Assert(err, IsNil)

	bucket, err := s.client.Bucket(bucketName)
	c.Assert(err, IsNil)
	s.bucket = bucket

	archiveBucket, err := s.client.Bucket(archiveBucketName)
	c.Assert(err, IsNil)
	s.archiveBucket = archiveBucket

	testLogger.Println("test bucket started")
}

// TearDownSuite runs before each test or benchmark starts running.
func (s *Ks3BucketSuite) TearDownSuite(c *C) {
	for _, bucket := range []*Bucket{s.bucket, s.archiveBucket} {
		// Delete multipart
		keyMarker := KeyMarker("")
		uploadIDMarker := UploadIDMarker("")
		for {
			lmu, err := bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
			c.Assert(err, IsNil)
			for _, upload := range lmu.Uploads {
				imur := InitiateMultipartUploadResult{Bucket: bucketName, Key: upload.Key, UploadID: upload.UploadID}
				err = bucket.AbortMultipartUpload(imur)
				c.Assert(err, IsNil)
			}
			keyMarker = KeyMarker(lmu.NextKeyMarker)
			uploadIDMarker = UploadIDMarker(lmu.NextUploadIDMarker)
			if !lmu.IsTruncated {
				break
			}
		}

		// Delete objects
		marker := Marker("")
		for {
			lor, err := bucket.ListObjects(marker)
			c.Assert(err, IsNil)
			for _, object := range lor.Objects {
				err = bucket.DeleteObject(object.Key)
				c.Assert(err, IsNil)
			}
			marker = Marker(lor.NextMarker)
			if !lor.IsTruncated {
				break
			}
		}

		// Delete bucket
		err := s.client.DeleteBucket(bucket.BucketName)
		c.Assert(err, IsNil)
	}

	testLogger.Println("test bucket completed")
}

// SetUpTest runs after each test or benchmark runs.
func (s *Ks3BucketSuite) SetUpTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)
}

// TearDownTest runs once after all tests or benchmarks have finished running.
func (s *Ks3BucketSuite) TearDownTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".temp")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt1")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt2")
	c.Assert(err, IsNil)
}

// TestPutObject
func (s *Ks3BucketSuite) TestPutObjectOnly(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "大江东去，浪淘尽，千古风流人物。 故垒西边，人道是、三国周郎赤壁。 乱石穿空，惊涛拍岸，卷起千堆雪。 江山如画，一时多少豪杰。" +
		"遥想公谨当年，小乔初嫁了，雄姿英发。 羽扇纶巾，谈笑间、樯橹灰飞烟灭。故国神游，多情应笑我，早生华发，人生如梦，一尊还酹江月。"

	// Put string
	var respHeader http.Header
	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue), GetResponseHeader(&respHeader))
	c.Assert(err, IsNil)

	// Check
	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("aclRes:", acl)
	c.Assert(acl.ACL, Equals, "default")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put bytes
	err = s.bucket.PutObject(objectName, bytes.NewReader([]byte(objectValue)))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put file
	err = CreateFileAndWrite(objectName+".txt", []byte(objectValue))
	c.Assert(err, IsNil)
	fd, err := os.Open(objectName + ".txt")
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(objectName, fd)
	c.Assert(err, IsNil)
	os.Remove(objectName + ".txt")

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put with properties
	objectName = objectNamePrefix + RandStr(8)
	options := []Option{
		Expires(futureDate),
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval"),
	}
	err = s.bucket.PutObject(objectName, strings.NewReader(objectValue), options...)
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectACL:", acl)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketSuite) SignURLTestFunc(c *C, authVersion AuthVersionType, extraHeaders []string) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := RandStr(20)

	filePath := RandLowStr(10)
	content := "复写object"
	CreateFile(filePath, content, c)

	notExistfilePath := RandLowStr(10)
	os.Remove(notExistfilePath)

	oldType := s.bucket.Client.Config.AuthVersion
	oldHeaders := s.bucket.Client.Config.AdditionalHeaders

	s.bucket.Client.Config.AuthVersion = authVersion
	s.bucket.Client.Config.AdditionalHeaders = extraHeaders

	// Sign URL for put
	str, err := s.bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)

	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Error put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue), ContentType("image/tiff"))
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")

	err = s.bucket.PutObjectFromFileWithURL(str, filePath, ContentType("image/tiff"))
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")

	// Put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, "default")

	// Get object meta
	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get(HTTPHeaderContentType), Equals, "application/octet-stream")
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "")

	// Sign URL for function GetObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Get object with URL
	body, err := s.bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Sign URL for function PutObjectWithURL
	options := []Option{
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval"),
		ContentType("image/tiff"),
		ResponseContentEncoding("deflate"),
	}
	str, err = s.bucket.SignURL(objectName, HTTPPut, 60, options...)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Put object with URL from file
	// Without option, error
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")

	err = s.bucket.PutObjectFromFileWithURL(str, filePath)
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")

	// With option, error file
	err = s.bucket.PutObjectFromFileWithURL(str, notExistfilePath, options...)
	c.Assert(err, NotNil)

	// With option
	err = s.bucket.PutObjectFromFileWithURL(str, filePath, options...)
	c.Assert(err, IsNil)

	// Get object meta
	meta, err = s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")
	c.Assert(meta.Get(HTTPHeaderContentType), Equals, "image/tiff")

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	// Sign URL for function GetObjectToFileWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)

	// Get object to file with URL
	newFile := RandStr(10)
	err = s.bucket.GetObjectToFileWithURL(str, newFile)
	c.Assert(err, IsNil)
	eq, err := compareFiles(filePath, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	// Get object to file error
	err = s.bucket.GetObjectToFileWithURL(str, newFile, options...)
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")
	_, err = os.Stat(newFile)
	c.Assert(err, NotNil)

	// Get object error
	body, err = s.bucket.GetObjectWithURL(str, options...)
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")
	c.Assert(body, IsNil)

	// Sign URL for function GetObjectToFileWithURL
	options = []Option{
		Expires(futureDate),
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval"),
		ContentType("image/tiff"),
		ResponseContentEncoding("deflate"),
	}
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60, options...)
	c.Assert(err, IsNil)

	// Get object to file with URL and options
	err = s.bucket.GetObjectToFileWithURL(str, newFile, options...)
	c.Assert(err, IsNil)
	eq, err = compareFiles(filePath, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	// Get object to file error
	err = s.bucket.GetObjectToFileWithURL(str, newFile)
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")
	_, err = os.Stat(newFile)
	c.Assert(err, NotNil)

	// Get object error
	body, err = s.bucket.GetObjectWithURL(str)
	c.Assert(err, NotNil)
	c.Assert(err.(ServiceError).Code, Equals, "SignatureDoesNotMatch")
	c.Assert(body, IsNil)

	err = s.bucket.PutObjectFromFile(objectName, "../sample/The Go Programming Language.html")
	c.Assert(err, IsNil)
	str, err = s.bucket.SignURL(objectName, HTTPGet, 3600, AcceptEncoding("gzip"))
	c.Assert(err, IsNil)
	s.bucket.GetObjectToFileWithURL(str, newFile)
	c.Assert(err, IsNil)

	os.Remove(filePath)
	os.Remove(newFile)

	// Sign URL error
	str, err = s.bucket.SignURL(objectName, HTTPGet, -1)
	c.Assert(err, NotNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Invalid URL parse
	str = RandStr(20)

	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, NotNil)

	err = s.bucket.GetObjectToFileWithURL(str, newFile)
	c.Assert(err, NotNil)

	s.bucket.Client.Config.AuthVersion = oldType
	s.bucket.Client.Config.AdditionalHeaders = oldHeaders
}

func (s *Ks3BucketSuite) TestSignURL(c *C) {
	s.SignURLTestFunc(c, AuthV1, []string{})
	s.SignURLTestFunc(c, AuthV2, []string{})
	s.SignURLTestFunc(c, AuthV2, []string{"host", "range", "user-agent"})
}

func (s *Ks3BucketSuite) SignURLWithEscapedKeyTestFunc(c *C, authVersion AuthVersionType, extraHeaders []string) {
	// Key with '/'
	objectName := "zyimg/86/e8/zzzxxf"
	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	oldType := s.bucket.Client.Config.AuthVersion
	oldHeaders := s.bucket.Client.Config.AdditionalHeaders

	s.bucket.Client.Config.AuthVersion = authVersion
	s.bucket.Client.Config.AdditionalHeaders = extraHeaders

	// Sign URL for function PutObjectWithURL
	str, err := s.bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for function GetObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Get object with URL
	body, err := s.bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Key with escaped chars
	objectName = "<>[]()`?.,!@#$%^&'/*-_=+~:;"

	// Sign URL for funciton PutObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for function GetObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Get object with URL
	body, err = s.bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Key with Chinese chars
	objectName = "风吹柳花满店香，吴姬压酒劝客尝。金陵子弟来相送，欲行不行各尽觞。请君试问东流水，别意与之谁短长。"

	// Sign URL for function PutObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for get function GetObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)
	if s.bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Get object with URL
	body, err = s.bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Key
	objectName = "test/此情无计可消除/才下眉头/却上 心头/。，；：‘’“”？（）『』【】《》！@#￥%……&×/test+ =-_*&^%$#@!`~[]{}()<>|\\/?.,;.txt"

	// Sign URL for function PutObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)

	// Put object with URL
	err = s.bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for function GetObjectWithURL
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)

	// Get object with URL
	body, err = s.bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Put object
	err = s.bucket.PutObject(objectName, bytes.NewReader([]byte(objectValue)))
	c.Assert(err, IsNil)

	// Get object
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Delete object
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	s.bucket.Client.Config.AuthVersion = oldType
	s.bucket.Client.Config.AdditionalHeaders = oldHeaders
}

func (s *Ks3BucketSuite) TestSignURLWithEscapedKey(c *C) {
	s.SignURLWithEscapedKeyTestFunc(c, AuthV1, []string{})
	s.SignURLWithEscapedKeyTestFunc(c, AuthV2, []string{})
	s.SignURLWithEscapedKeyTestFunc(c, AuthV2, []string{"host", "range", "user-agent"})
}

func (s *Ks3BucketSuite) SignURLWithEscapedKeyAndPorxyTestFunc(c *C, authVersion AuthVersionType, extraHeaders []string) {
	// Key with '/'
	objectName := "zyimg/86/e8/653b5dc97bb0022051a84c632bc4"
	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	options := []ClientOption{
		AuthProxy(proxyHost, proxyUser, proxyPasswd),
		AuthVersion(authVersion),
		AdditionalHeaders(extraHeaders),
	}

	client, err := New(endpoint, accessID, accessKey, options...)
	bucket, err := client.Bucket(bucketName)

	// Sign URL for put
	str, err := bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)
	if bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Put object with URL
	err = bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for function GetObjectWithURL
	str, err = bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)
	if bucket.Client.Config.AuthVersion == AuthV1 {
		c.Assert(strings.Contains(str, HTTPParamExpires+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyID+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignature+"="), Equals, true)
	} else {
		c.Assert(strings.Contains(str, HTTPParamSignatureVersion+"=KS32"), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamExpiresV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamAccessKeyIDV2+"="), Equals, true)
		c.Assert(strings.Contains(str, HTTPParamSignatureV2+"="), Equals, true)
	}

	// Get object with URL
	body, err := bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Key with Chinese chars
	objectName = "test/此情无计可消除/才下眉头/却上 心头/。，；：‘’“”？（）『』【】《》！@#￥%……&×/test+ =-_*&^%$#@!`~[]{}()<>|\\/?.,;.txt"

	// Sign URL for function PutObjectWithURL
	str, err = bucket.SignURL(objectName, HTTPPut, 60)
	c.Assert(err, IsNil)

	// Put object with URL
	err = bucket.PutObjectWithURL(str, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Sign URL for function GetObjectWithURL
	str, err = bucket.SignURL(objectName, HTTPGet, 60)
	c.Assert(err, IsNil)

	// Get object with URL
	body, err = bucket.GetObjectWithURL(str)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Put object
	err = bucket.PutObject(objectName, bytes.NewReader([]byte(objectValue)))
	c.Assert(err, IsNil)

	// Get object
	body, err = bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Delete object
	err = bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketSuite) TestSignURLWithEscapedKeyAndPorxy(c *C) {
	s.SignURLWithEscapedKeyAndPorxyTestFunc(c, AuthV1, []string{})
	s.SignURLWithEscapedKeyAndPorxyTestFunc(c, AuthV2, []string{})
	s.SignURLWithEscapedKeyAndPorxyTestFunc(c, AuthV2, []string{"host", "range", "user-agent"})
}

func (s *Ks3BucketSuite) TestQueryStringAuthV2(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// set ks3 v2 signatrue
	client.Config.AuthVersion = AuthV2
	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	// build QueryString
	QueryKey1 := "abc"
	QueryKey2 := "|abc"
	c.Assert(strings.Compare(QueryKey1, QueryKey2) < 0, Equals, true)
	c.Assert(strings.Compare(url.QueryEscape(QueryKey1), url.QueryEscape(QueryKey2)) > 0, Equals, true)

	options := []Option{}
	params := map[string]interface{}{}
	params[QueryKey1] = "queryValue1"
	params[QueryKey2] = "queryValue2"
	objectKey := objectNamePrefix + RandStr(8)
	resp, _ := bucket.do("HEAD", objectKey, params, options, nil, nil)

	// object not exist,no signature error
	c.Assert(resp.StatusCode, Equals, 404)
	ForceDeleteBucket(client, bucketName, c)
}

// TestPutObjectType
func (s *Ks3BucketSuite) TestPutObjectType(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "乱石穿空，惊涛拍岸，卷起千堆雪。 江山如画，一时多少豪杰。"

	// Put
	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Content-Type"), Equals, "application/octet-stream")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put
	err = s.bucket.PutObject(objectName+".txt", strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	meta, err = s.bucket.GetObjectDetailedMeta(objectName + ".txt")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(meta.Get("Content-Type"), "text/plain"), Equals, true)

	err = s.bucket.DeleteObject(objectName + ".txt")
	c.Assert(err, IsNil)

	// Put
	err = s.bucket.PutObject(objectName+".apk", strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	meta, err = s.bucket.GetObjectDetailedMeta(objectName + ".apk")
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Content-Type"), Equals, "application/vnd.android.package-archive")

	err = s.bucket.DeleteObject(objectName + ".txt")
	c.Assert(err, IsNil)
}

// TestPutObject
func (s *Ks3BucketSuite) TestPutObjectKeyChars(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "白日依山尽，黄河入海流。欲穷千里目，更上一层楼。"

	// Put
	objectKey := objectName + "十步杀一人，千里不留行。事了拂衣去，深藏身与名"
	err := s.bucket.PutObject(objectKey, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err := s.bucket.GetObject(objectKey)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)

	// Put
	objectKey = objectName + "ごきげん如何ですかおれの顔をよく拝んでおけ"
	err = s.bucket.PutObject(objectKey, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectKey)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)

	// Put
	objectKey = objectName + "~!@#$%^&*()_-+=|\\[]{}<>,./?"
	err = s.bucket.PutObject(objectKey, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectKey)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)

	// Put
	objectKey = "go/中国 日本 +-#&=*"
	err = s.bucket.PutObject(objectKey, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectKey)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)
}

// TestPutObjectNegative
func (s *Ks3BucketSuite) TestPutObjectNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "大江东去，浪淘尽，千古风流人物。 "

	// Put
	objectName = objectNamePrefix + RandStr(8)
	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue),
		Meta("meta-my", "myprop"))
	c.Assert(err, IsNil)

	// Check meta
	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-My"), Not(Equals), "myprop")
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Invalid option
	err = s.bucket.PutObject(objectName, strings.NewReader(objectValue),
		IfModifiedSince(pastDate))
	c.Assert(err, NotNil)

	err = s.bucket.PutObjectFromFile(objectName, "bucket.go", IfModifiedSince(pastDate))
	c.Assert(err, NotNil)

	err = s.bucket.PutObjectFromFile(objectName, "/tmp/xxx")
	c.Assert(err, NotNil)
}

// TestPutObjectFromFile
func (s *Ks3BucketSuite) TestPutObjectFromFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := "newpic11.jpg"

	// Put
	err := s.bucket.PutObjectFromFile(objectName, localFile)
	c.Assert(err, IsNil)

	// Check
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err := compareFiles(localFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("aclRes:", acl)
	c.Assert(acl.ACL, Equals, "default")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put with properties
	options := []Option{
		Expires(futureDate),
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval"),
	}
	os.Remove(newFile)
	err = s.bucket.PutObjectFromFile(objectName, localFile, options...)
	c.Assert(err, IsNil)

	// Check
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err = compareFiles(localFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectACL:", acl)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)
}

// TestPutObjectFromFile
func (s *Ks3BucketSuite) TestPutObjectFromFileType(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	// Put
	err := s.bucket.PutObjectFromFile(objectName, localFile)
	c.Assert(err, IsNil)

	// Check
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err := compareFiles(localFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Content-Type"), Equals, "image/jpeg")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)
}

// TestGetObject
func (s *Ks3BucketSuite) TestGetObjectNormal(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	// Put
	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(body)
	body.Close()
	str := string(data)
	c.Assert(str, Equals, objectValue)
	testLogger.Println("GetObjec:", str)

	// Range
	var subObjectValue = string(([]byte(objectValue))[15:36])
	body, err = s.bucket.GetObject(objectName, Range(15, 35))
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(body)
	body.Close()
	str = string(data)
	c.Assert(str, Equals, subObjectValue)
	testLogger.Println("GetObject:", str, ",", subObjectValue)

	// If-Modified-Since
	_, err = s.bucket.GetObject(objectName, IfModifiedSince(futureDate))
	c.Assert(err, NotNil)

	// If-Unmodified-Since
	body, err = s.bucket.GetObject(objectName, IfUnmodifiedSince(futureDate))
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(body)
	body.Close()
	c.Assert(string(data), Equals, objectValue)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)

	// If-Match
	body, err = s.bucket.GetObject(objectName, IfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(body)
	body.Close()
	c.Assert(string(data), Equals, objectValue)

	// If-None-Match
	_, err = s.bucket.GetObject(objectName, IfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	// process
	err = s.bucket.PutObjectFromFile(objectName, "../sample/BingWallpaper-2015-11-07.jpg")
	c.Assert(err, IsNil)
	_, err = s.bucket.GetObject(objectName, Process("image/format,png"))
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestGetObjectNegative
func (s *Ks3BucketSuite) TestGetObjectToWriterNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	// Object not exist
	_, err := s.bucket.GetObject("NotExist")
	c.Assert(err, NotNil)

	// Constraint invalid
	err = s.bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Out of range
	_, err = s.bucket.GetObject(objectName, Range(15, 1000))
	c.Assert(err, IsNil)

	// Not exist
	err = s.bucket.GetObjectToFile(objectName, "/root1/123abc9874")
	c.Assert(err, NotNil)

	// Invalid option
	_, err = s.bucket.GetObject(objectName, ACL(ACLPublicRead))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(objectName, "newpic15.jpg", ACL(ACLPublicRead))
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestGetObjectToFile
func (s *Ks3BucketSuite) TestGetObjectToFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "江南好，风景旧曾谙；日出江花红胜火，春来江水绿如蓝。能不忆江南？江南忆，最忆是杭州；山寺月中寻桂子，郡亭枕上看潮头。何日更重游！"
	newFile := RandStr(8) + ".jpg"

	// Put
	var val = []byte(objectValue)
	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Check
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err := compareFileData(newFile, val)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	// Range
	err = s.bucket.GetObjectToFile(objectName, newFile, Range(15, 35))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val[15:36])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	err = s.bucket.GetObjectToFile(objectName, newFile, NormalizedRange("15-35"))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val[15:36])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	err = s.bucket.GetObjectToFile(objectName, newFile, NormalizedRange("15-"))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val[15:])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	err = s.bucket.GetObjectToFile(objectName, newFile, NormalizedRange("-10"))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val[(len(val)-10):len(val)])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	// If-Modified-Since
	err = s.bucket.GetObjectToFile(objectName, newFile, IfModifiedSince(futureDate))
	c.Assert(err, NotNil)

	// If-Unmodified-Since
	err = s.bucket.GetObjectToFile(objectName, newFile, IfUnmodifiedSince(futureDate))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	os.Remove(newFile)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)

	// If-Match
	err = s.bucket.GetObjectToFile(objectName, newFile, IfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)
	eq, err = compareFileData(newFile, val)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// If-None-Match
	err = s.bucket.GetObjectToFile(objectName, newFile, IfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	// Accept-Encoding:gzip
	err = s.bucket.PutObjectFromFile(objectName, "../sample/The Go Programming Language.html")
	c.Assert(err, IsNil)
	err = s.bucket.GetObjectToFile(objectName, newFile, AcceptEncoding("gzip"))
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestListObjects
func (s *Ks3BucketSuite) TestListObjects(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// List empty bucket
	lor, err := s.bucket.ListObjects()
	c.Assert(err, IsNil)
	left := len(lor.Objects)

	// Put three objects
	err = s.bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = s.bucket.PutObject(objectName+"2", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = s.bucket.PutObject(objectName+"3", strings.NewReader(""))
	c.Assert(err, IsNil)

	// List
	lor, err = s.bucket.ListObjects()
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, left+3)

	// List with prefix
	lor, err = s.bucket.ListObjects(Prefix(objectName + "2"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)

	lor, err = s.bucket.ListObjects(Prefix(objectName + "22"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// List with max keys
	lor, err = s.bucket.ListObjects(Prefix(objectName), MaxKeys(2))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 2)

	// List with marker
	lor, err = s.bucket.ListObjects(Marker(objectName+"1"), MaxKeys(1))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)

	err = s.bucket.DeleteObject(objectName + "1")
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectName + "2")
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectName + "3")
	c.Assert(err, IsNil)
}

// TestListObjects
func (s *Ks3BucketSuite) TestListObjectsV2NotBatch(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	// List empty bucket
	lor, err := bucket.ListObjectsV2(StartAfter(""))
	c.Assert(err, IsNil)
	left := len(lor.Objects)

	// Put three objects
	err = bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = bucket.PutObject(objectName+"2", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = bucket.PutObject(objectName+"3", strings.NewReader(""))
	c.Assert(err, IsNil)

	// List
	lor, err = bucket.ListObjectsV2(FetchOwner(true))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, left+3)
	c.Assert(len(lor.Objects[0].Owner.ID) > 0, Equals, true)
	c.Assert(len(lor.Objects[0].Owner.DisplayName) > 0, Equals, true)

	// List with prefix
	lor, err = bucket.ListObjectsV2(Prefix(objectName + "2"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)
	c.Assert(lor.Objects[0].Key, Equals, objectName+"2")

	lor, err = bucket.ListObjectsV2(Prefix(objectName + "22"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// List with max keys
	lor, err = bucket.ListObjectsV2(Prefix(objectName), MaxKeys(2))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 2)

	// List with marker
	lor, err = bucket.ListObjectsV2(StartAfter(objectName+"1"), MaxKeys(1))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)
	c.Assert(lor.IsTruncated, Equals, true)
	c.Assert(len(lor.NextContinuationToken) > 0, Equals, true)
	c.Assert(lor.Objects[0].Key, Equals, objectName+"2")

	lor, err = bucket.ListObjectsV2(Prefix(objectName), StartAfter(objectName+"1"), MaxKeys(2))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 2)
	c.Assert(lor.IsTruncated, Equals, false)
	c.Assert(lor.NextContinuationToken, Equals, "")
	ForceDeleteBucket(client, bucketName, c)
	c.Assert(lor.Objects[0].Key, Equals, objectName+"2")
	c.Assert(lor.Objects[1].Key, Equals, objectName+"3")
}

// TestListObjects
func (s *Ks3BucketSuite) TestListObjectsV2BatchList(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	// Put three objects
	count := 17
	objectName := "testobject-" + RandLowStr(6)
	for i := 0; i < count; i++ {
		err = bucket.PutObject(objectName+strconv.Itoa(i), strings.NewReader(""))
		c.Assert(err, IsNil)
	}

	Objects := []ObjectProperties{}

	// List Object
	continuationToken := ""
	prefix := ""
	for {
		lor, err := bucket.ListObjectsV2(Prefix(prefix), ContinuationToken(continuationToken), MaxKeys(3))
		c.Assert(err, IsNil)
		Objects = append(Objects, lor.Objects...)
		continuationToken = lor.NextContinuationToken
		if !lor.IsTruncated {
			break
		}
	}
	c.Assert(len(Objects), Equals, count)
	ForceDeleteBucket(client, bucketName, c)
}

// TestListObjects
func (s *Ks3BucketSuite) TestListObjectsEncodingType(c *C) {
	prefix := objectNamePrefix + "床前明月光，疑是地上霜。举头望明月，低头思故乡。"

	for i := 0; i < 10; i++ {
		err := s.bucket.PutObject(prefix+strconv.Itoa(i), strings.NewReader(""))
		c.Assert(err, IsNil)
	}

	lor, err := s.bucket.ListObjects(Prefix(objectNamePrefix + "床前明月光，"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 10)

	lor, err = s.bucket.ListObjects(Marker(objectNamePrefix + "床前明月光，疑是地上霜。举头望明月，低头思故乡。"))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 10)

	lor, err = s.bucket.ListObjects(Prefix(objectNamePrefix + "床前明月光"))
	c.Assert(err, IsNil)
	for i, obj := range lor.Objects {
		c.Assert(obj.Key, Equals, prefix+strconv.Itoa(i))
	}

	for i := 0; i < 10; i++ {
		err = s.bucket.DeleteObject(prefix + strconv.Itoa(i))
		c.Assert(err, IsNil)
	}

	// Special characters
	objectName := objectNamePrefix + "` ~ ! @ # $ % ^ & * () - _ + =[] {} \\ | < > , . ? / 0"
	err = s.bucket.PutObject(objectName, strings.NewReader("明月几时有，把酒问青天"))
	c.Assert(err, IsNil)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	objectName = objectNamePrefix + "中国  日本  +-#&=*"
	err = s.bucket.PutObject(objectName, strings.NewReader("明月几时有，把酒问青天"))
	c.Assert(err, IsNil)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestIsBucketExist
func (s *Ks3BucketSuite) TestIsObjectExist(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Put three objects
	err := s.bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = s.bucket.PutObject(objectName+"11", strings.NewReader(""))
	c.Assert(err, IsNil)
	err = s.bucket.PutObject(objectName+"111", strings.NewReader(""))
	c.Assert(err, IsNil)

	// Exist
	exist, err := s.bucket.IsObjectExist(objectName + "11")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	exist, err = s.bucket.IsObjectExist(objectName + "1")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	exist, err = s.bucket.IsObjectExist(objectName + "111")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	// Not exist
	exist, err = s.bucket.IsObjectExist(objectName + "1111")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	exist, err = s.bucket.IsObjectExist(objectName)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	err = s.bucket.DeleteObject(objectName + "1")
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectName + "11")
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectName + "111")
	c.Assert(err, IsNil)
}

// TestDeleteObject
func (s *Ks3BucketSuite) TestDeleteObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	err := s.bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	lor, err := s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 1)

	// Delete
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Duplicate delete
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)
}

// TestDeleteObjects
func (s *Ks3BucketSuite) TestDeleteObjectsNormal(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Delete objects
	err := s.bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err := s.bucket.DeleteObjects([]string{objectName})
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 1)

	lor, err := s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// Delete objects
	err = s.bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(objectName+"2", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{objectName + "1", objectName + "2"})
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 2)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// Delete 0
	_, err = s.bucket.DeleteObjects([]string{})
	c.Assert(err, NotNil)

	// DeleteObjectsQuiet
	err = s.bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(objectName+"2", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{objectName + "1", objectName + "2"},
		DeleteObjectsQuiet(false))
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 2)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// DeleteObjectsQuiet
	err = s.bucket.PutObject(objectName+"1", strings.NewReader(""))
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(objectName+"2", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{objectName + "1", objectName + "2"},
		DeleteObjectsQuiet(true))
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 0)

	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, 0)

	// EncodingType
	err = s.bucket.PutObject("中国人", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{"中国人"})
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 1)
	c.Assert(res.DeletedObjects[0], Equals, "中国人")

	// EncodingType
	err = s.bucket.PutObject("中国人", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{"中国人"}, DeleteObjectsQuiet(false))
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 1)
	c.Assert(res.DeletedObjects[0], Equals, "中国人")

	// EncodingType
	err = s.bucket.PutObject("中国人", strings.NewReader(""))
	c.Assert(err, IsNil)

	res, err = s.bucket.DeleteObjects([]string{"中国人"}, DeleteObjectsQuiet(true))
	c.Assert(err, IsNil)
	c.Assert(len(res.DeletedObjects), Equals, 0)

	// Special characters
	key := "A ' < > \" & ~ ` ! @ # $ % ^ & * ( ) [] {} - _ + = / | \\ ? . , : ; A"
	err = s.bucket.PutObject(key, strings.NewReader("value"))
	c.Assert(err, IsNil)

	_, err = s.bucket.DeleteObjects([]string{key})
	c.Assert(err, IsNil)

	ress, err := s.bucket.ListObjects(Prefix(key))
	c.Assert(err, IsNil)
	c.Assert(len(ress.Objects), Equals, 0)

	// Not exist
	_, err = s.bucket.DeleteObjects([]string{"NotExistObject"})
	c.Assert(err, IsNil)
}

// TestSetObjectMeta
func (s *Ks3BucketSuite) TestSetObjectMeta(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	err := s.bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	err = s.bucket.SetObjectMeta(objectName,
		Expires(futureDate),
		Meta("myprop", "mypropval"))
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("Meta:", meta)
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, "default")

	// Invalid option
	err = s.bucket.SetObjectMeta(objectName, AcceptEncoding("url"))
	c.Assert(err, IsNil)

	// Invalid option value
	err = s.bucket.SetObjectMeta(objectName, ServerSideEncryption("invalid"))
	c.Assert(err, NotNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Not exist
	err = s.bucket.SetObjectMeta(objectName, Expires(futureDate))
	c.Assert(err, NotNil)
}

// TestGetObjectMeta
func (s *Ks3BucketSuite) TestGetObjectMeta(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Put
	err := s.bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetObjectMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(len(meta) > 0, Equals, true)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	_, err = s.bucket.GetObjectMeta("NotExistObject")
	c.Assert(err, NotNil)
}

// TestGetObjectDetailedMeta
func (s *Ks3BucketSuite) TestGetObjectDetailedMeta(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Put
	err := s.bucket.PutObject(objectName, strings.NewReader(""),
		Expires(futureDate), Meta("myprop", "mypropval"))
	c.Assert(err, IsNil)

	// Check
	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")
	c.Assert(meta.Get("Content-Length"), Equals, "0")
	c.Assert(len(meta.Get("Date")) > 0, Equals, true)
	c.Assert(len(meta.Get("X-Kss-Request-Id")) > 0, Equals, true)
	c.Assert(len(meta.Get("Last-Modified")) > 0, Equals, true)

	// IfModifiedSince/IfModifiedSince
	_, err = s.bucket.GetObjectDetailedMeta(objectName, IfModifiedSince(futureDate))
	c.Assert(err, NotNil)

	meta, err = s.bucket.GetObjectDetailedMeta(objectName, IfUnmodifiedSince(futureDate))
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	// IfMatch/IfNoneMatch
	_, err = s.bucket.GetObjectDetailedMeta(objectName, IfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	meta, err = s.bucket.GetObjectDetailedMeta(objectName, IfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	_, err = s.bucket.GetObjectDetailedMeta("NotExistObject")
	c.Assert(err, NotNil)
}

// TestSetAndGetObjectAcl
func (s *Ks3BucketSuite) TestSetAndGetObjectAcl(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	err := s.bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	// Default
	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, "default")

	// Set ACL_PUBLIC_RW
	err = s.bucket.SetObjectACL(objectName, ACLPublicReadWrite)
	c.Assert(err, IsNil)

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicReadWrite))

	// Set ACL_PRIVATE
	err = s.bucket.SetObjectACL(objectName, ACLPrivate)
	c.Assert(err, IsNil)

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPrivate))

	// Set ACL_PUBLIC_R
	err = s.bucket.SetObjectACL(objectName, ACLPublicRead)
	c.Assert(err, IsNil)

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestSetAndGetObjectAclNegative
func (s *Ks3BucketSuite) TestSetAndGetObjectAclNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Object not exist
	err := s.bucket.SetObjectACL(objectName, ACLPublicRead)
	c.Assert(err, NotNil)
}

// TestCopyObject
func (s *Ks3BucketSuite) TestCopyObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "男儿何不带吴钩，收取关山五十州。请君暂上凌烟阁，若个书生万户侯？"

	err := s.bucket.PutObject(objectName, strings.NewReader(objectValue),
		ACL(ACLPublicRead), Meta("my", "myprop"))
	c.Assert(err, IsNil)

	// Copy
	var objectNameDest = objectName + "dest"
	_, err = s.bucket.CopyObject(objectName, objectNameDest)
	c.Assert(err, IsNil)

	// Check
	lor, err := s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	testLogger.Println("objects:", lor.Objects)
	c.Assert(len(lor.Objects), Equals, 2)

	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-ks3-copy-source-if-modified-since
	_, err = s.bucket.CopyObject(objectName, objectNameDest, CopySourceIfModifiedSince(futureDate))
	c.Assert(err, NotNil)
	testLogger.Println("CopyObject:", err)

	// Copy with constraints x-ks3-copy-source-if-unmodified-since
	_, err = s.bucket.CopyObject(objectName, objectNameDest, CopySourceIfUnmodifiedSince(futureDate))
	c.Assert(err, IsNil)

	// Check
	lor, err = s.bucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	testLogger.Println("objects:", lor.Objects)
	c.Assert(len(lor.Objects), Equals, 2)

	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-ks3-copy-source-if-match
	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)

	_, err = s.bucket.CopyObject(objectName, objectNameDest, CopySourceIfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-ks3-copy-source-if-none-match
	_, err = s.bucket.CopyObject(objectName, objectNameDest, CopySourceIfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	// Copy with constraints x-ks3-metadata-directive
	_, err = s.bucket.CopyObject(objectName, objectNameDest, Meta("my", "mydestprop"),
		MetadataDirective(MetaCopy))
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	destMeta, err := s.bucket.GetObjectDetailedMeta(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")

	acl, err := s.bucket.GetObjectACL(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, "default")

	err = s.bucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-ks3-metadata-directive and self defined dest object meta
	options := []Option{
		ObjectACL(ACLPublicReadWrite),
		Meta("my", "mydestprop"),
		MetadataDirective(MetaReplace),
	}
	_, err = s.bucket.CopyObject(objectName, objectNameDest, options...)
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	destMeta, err = s.bucket.GetObjectDetailedMeta(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(destMeta.Get("X-Kss-Meta-My"), Equals, "mydestprop")

	acl, err = s.bucket.GetObjectACL(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicReadWrite))

	err = s.bucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestCopyObjectToOrFrom
func (s *Ks3BucketSuite) TestCopyObjectToOrFrom(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "男儿何不带吴钩，收取关山五十州。请君暂上凌烟阁，若个书生万户侯？"
	destBucketName := bucketName + "-dest"
	objectNameDest := objectName + "-dest"

	err := s.client.CreateBucket(destBucketName)
	c.Assert(err, IsNil)

	destBucket, err := s.client.Bucket(destBucketName)
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Copy from
	_, err = destBucket.CopyObjectFrom(bucketName, objectName, objectNameDest)
	c.Assert(err, IsNil)

	// Check
	body, err := destBucket.GetObject(objectNameDest)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Copy to
	_, err = destBucket.CopyObjectTo(bucketName, objectName, objectNameDest)
	c.Assert(err, IsNil)

	// Check
	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Clean
	err = destBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = s.client.DeleteBucket(destBucketName)
	c.Assert(err, IsNil)
}

// TestCopyObjectToOrFromNegative
func (s *Ks3BucketSuite) TestCopyObjectToOrFromNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	destBucket := bucketName + "-dest"
	objectNameDest := objectName + "-dest"

	// Object not exist
	_, err := s.bucket.CopyObjectTo(bucketName, objectName, objectNameDest)
	c.Assert(err, NotNil)

	// Bucket not exist
	_, err = s.bucket.CopyObjectFrom(destBucket, objectNameDest, objectName)
	c.Assert(err, NotNil)
}

// TestAppendObject
func (s *Ks3BucketSuite) TestAppendObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "昨夜雨疏风骤，浓睡不消残酒。试问卷帘人，却道海棠依旧。知否？知否？应是绿肥红瘦。"
	var val = []byte(objectValue)
	var localFile = RandStr(8) + ".txt"
	var nextPos int64
	var midPos = 1 + rand.Intn(len(val)-1)

	var err = CreateFileAndWrite(localFile+"1", val[0:midPos])
	c.Assert(err, IsNil)
	err = CreateFileAndWrite(localFile+"2", val[midPos:])
	c.Assert(err, IsNil)

	// String append
	nextPos, err = s.bucket.AppendObject(objectName, strings.NewReader("昨夜雨疏风骤，浓睡不消残酒。试问卷帘人，"), nextPos)
	c.Assert(err, IsNil)
	nextPos, err = s.bucket.AppendObject(objectName, strings.NewReader("却道海棠依旧。知否？知否？应是绿肥红瘦。"), nextPos)
	c.Assert(err, IsNil)

	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Byte append
	nextPos = 0
	nextPos, err = s.bucket.AppendObject(objectName, bytes.NewReader(val[0:midPos]), nextPos)
	c.Assert(err, IsNil)
	nextPos, err = s.bucket.AppendObject(objectName, bytes.NewReader(val[midPos:]), nextPos)
	c.Assert(err, IsNil)

	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// File append
	options := []Option{
		ObjectACL(ACLPublicReadWrite),
		Meta("my", "myprop"),
	}

	fd, err := os.Open(localFile + "1")
	c.Assert(err, IsNil)
	defer fd.Close()
	nextPos = 0
	nextPos, err = s.bucket.AppendObject(objectName, fd, nextPos, options...)
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta, ",", nextPos)
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Appendable")
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("x-ks3-Meta-Mine"), Equals, "")
	c.Assert(meta.Get("X-Kss-Next-Append-Position"), Equals, strconv.FormatInt(nextPos, 10))

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectACL:", acl)
	c.Assert(acl.ACL, Equals, string(ACLPublicReadWrite))

	// Second append
	options = []Option{
		ObjectACL(ACLPublicRead),
		Meta("my", "myproptwo"),
		Meta("mine", "mypropmine"),
	}
	fd, err = os.Open(localFile + "2")
	c.Assert(err, IsNil)
	defer fd.Close()
	nextPos, err = s.bucket.AppendObject(objectName, fd, nextPos, options...)
	c.Assert(err, IsNil)

	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	meta, err = s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta xxx:", meta)
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Appendable")
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("x-Kss-Meta-Mine"), Equals, "")
	c.Assert(meta.Get("X-Kss-Next-Append-Position"), Equals, strconv.FormatInt(nextPos, 10))

	acl, err = s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestAppendObjectNegative
func (s *Ks3BucketSuite) TestAppendObjectNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	nextPos := int64(0)

	nextPos, err := s.bucket.AppendObject(objectName, strings.NewReader("ObjectValue"), nextPos)
	c.Assert(err, IsNil)

	nextPos, err = s.bucket.AppendObject(objectName, strings.NewReader("ObjectValue"), 0)
	c.Assert(err, NotNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestContentType
func (s *Ks3BucketSuite) TestAddContentType(c *C) {
	opts := AddContentType(nil, "abc.txt")
	typ, err := FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(typ.(string), "text/plain"), Equals, true)

	opts = AddContentType(nil)
	typ, err = FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(len(opts), Equals, 1)
	c.Assert(strings.Contains(typ.(string), "application/octet-stream"), Equals, true)

	opts = AddContentType(nil, "abc.txt", "abc.pdf")
	typ, err = FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(typ.(string), "text/plain"), Equals, true)

	opts = AddContentType(nil, "abc", "abc.txt", "abc.pdf")
	typ, err = FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(typ.(string), "text/plain"), Equals, true)

	opts = AddContentType(nil, "abc", "abc", "edf")
	typ, err = FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(typ.(string), "application/octet-stream"), Equals, true)

	opts = AddContentType([]Option{Meta("meta", "my")}, "abc", "abc.txt", "abc.pdf")
	typ, err = FindOption(opts, HTTPHeaderContentType, "")
	c.Assert(err, IsNil)
	c.Assert(len(opts), Equals, 2)
	c.Assert(strings.Contains(typ.(string), "text/plain"), Equals, true)
}

func (s *Ks3BucketSuite) TestGetConfig(c *C) {
	client, err := New(endpoint, accessID, accessKey, UseCname(true),
		Timeout(11, 12), SecurityToken("token"), EnableMD5(false))
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	c.Assert(bucket.GetConfig().HTTPTimeout.ConnectTimeout, Equals, time.Second*11)
	c.Assert(bucket.GetConfig().HTTPTimeout.ReadWriteTimeout, Equals, time.Second*12)
	c.Assert(bucket.GetConfig().HTTPTimeout.HeaderTimeout, Equals, time.Second*12)
	c.Assert(bucket.GetConfig().HTTPTimeout.IdleConnTimeout, Equals, time.Second*12)
	c.Assert(bucket.GetConfig().HTTPTimeout.LongTimeout, Equals, time.Second*12*10)

	c.Assert(bucket.GetConfig().SecurityToken, Equals, "token")
	c.Assert(bucket.GetConfig().IsCname, Equals, true)
	c.Assert(bucket.GetConfig().IsEnableMD5, Equals, false)
}

func (s *Ks3BucketSuite) TestSTSToken(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	//stsClient := sts.NewClient(stsaccessID, stsaccessKey, stsARN, "ks3_test_sess")
	//
	//resp, err := stsClient.AssumeRole(1800)
	//c.Assert(err, IsNil)
	//
	client, err := New(endpoint, "ak", "sk",
		SecurityToken("SecurityToken"))
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	// Put
	err = bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Get
	body, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// List
	lor, err := bucket.ListObjects()
	c.Assert(err, IsNil)
	testLogger.Println("Objects:", lor.Objects)

	// Put with URL
	signedURL, err := bucket.SignURL(objectName, HTTPPut, 3600)
	c.Assert(err, IsNil)

	err = bucket.PutObjectWithURL(signedURL, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Get with URL
	signedURL, err = bucket.SignURL(objectName, HTTPGet, 3600)
	c.Assert(err, IsNil)

	body, err = bucket.GetObjectWithURL(signedURL)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Delete
	err = bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketSuite) TestSTSTonekNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := objectName + ".jpg"

	client, err := New(endpoint, accessID, accessKey, SecurityToken("Invalid"))
	c.Assert(err, IsNil)

	_, err = client.ListBuckets()
	c.Assert(err, NotNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	err = bucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, NotNil)

	err = bucket.PutObjectFromFile(objectName, "")
	c.Assert(err, NotNil)

	_, err = bucket.GetObject(objectName)
	c.Assert(err, NotNil)

	err = bucket.GetObjectToFile(objectName, "")
	c.Assert(err, NotNil)

	_, err = bucket.ListObjects()
	c.Assert(err, NotNil)

	err = bucket.SetObjectACL(objectName, ACLPublicRead)
	c.Assert(err, NotNil)

	_, err = bucket.GetObjectACL(objectName)
	c.Assert(err, NotNil)

	err = bucket.UploadFile(objectName, localFile, MinPartSize)
	c.Assert(err, NotNil)

	err = bucket.DownloadFile(objectName, localFile, MinPartSize)
	c.Assert(err, NotNil)

	_, err = bucket.IsObjectExist(objectName)
	c.Assert(err, NotNil)

	_, err = bucket.ListMultipartUploads()
	c.Assert(err, NotNil)

	err = bucket.DeleteObject(objectName)
	c.Assert(err, NotNil)

	_, err = bucket.DeleteObjects([]string{objectName})
	c.Assert(err, NotNil)

	err = client.DeleteBucket(bucketName)
	c.Assert(err, NotNil)
}

func (s *Ks3BucketSuite) TestUploadBigFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	bigFile := "D:\\tmp\\bigfile.zip"
	newFile := "D:\\tmp\\newbigfile.zip"

	exist, err := isFileExist(bigFile)
	c.Assert(err, IsNil)
	if !exist {
		return
	}

	// Put
	start := GetNowSec()
	err = s.bucket.PutObjectFromFile(objectName, bigFile)
	c.Assert(err, IsNil)
	end := GetNowSec()
	testLogger.Println("Put big file:", bigFile, "use sec:", end-start)

	// Check
	start = GetNowSec()
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	end = GetNowSec()
	testLogger.Println("Get big file:", bigFile, "use sec:", end-start)

	start = GetNowSec()
	eq, err := compareFiles(bigFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	end = GetNowSec()
	testLogger.Println("Compare big file:", bigFile, "use sec:", end-start)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketSuite) TestSymlink(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	targetObjectName := objectName + "target"

	err := s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(targetObjectName)
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetSymlink(objectName)
	c.Assert(err, NotNil)

	// Put symlink
	err = s.bucket.PutSymlink(objectName, targetObjectName)
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(targetObjectName, strings.NewReader("target"))
	c.Assert(err, IsNil)

	err = s.bucket.PutSymlink(objectName, targetObjectName)
	c.Assert(err, IsNil)

	meta, err = s.bucket.GetSymlink(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get(HTTPHeaderKs3SymlinkTarget), Equals, targetObjectName)

	// List object
	lor, err := s.bucket.ListObjects()
	c.Assert(err, IsNil)
	exist, v := s.getObject(lor.Objects, objectName)
	c.Assert(exist, Equals, true)
	c.Assert(v.Type, Equals, "Symlink")

	body, err := s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, "target")

	meta, err = s.bucket.GetSymlink(targetObjectName)
	c.Assert(err, NotNil)

	err = s.bucket.PutObject(objectName, strings.NewReader("src"))
	c.Assert(err, IsNil)

	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, "src")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(targetObjectName)
	c.Assert(err, IsNil)

	// Put symlink again
	objectName = objectNamePrefix + RandStr(8)
	targetObjectName = objectName + "-target"

	err = s.bucket.PutSymlink(objectName, targetObjectName)
	c.Assert(err, IsNil)

	err = s.bucket.PutObject(targetObjectName, strings.NewReader("target1"))
	c.Assert(err, IsNil)

	meta, err = s.bucket.GetSymlink(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get(HTTPHeaderKs3SymlinkTarget), Equals, targetObjectName)

	body, err = s.bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, "target1")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(targetObjectName)
	c.Assert(err, IsNil)
}

// TestRestoreObject
func (s *Ks3BucketSuite) TestRestoreObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// List objects
	lor, err := s.archiveBucket.ListObjects()
	c.Assert(err, IsNil)
	left := len(lor.Objects)

	// Put object
	err = s.archiveBucket.PutObject(objectName, strings.NewReader(""))
	c.Assert(err, IsNil)

	// List
	lor, err = s.archiveBucket.ListObjects()
	c.Assert(err, IsNil)
	c.Assert(len(lor.Objects), Equals, left+1)
	for _, object := range lor.Objects {
		c.Assert(object.StorageClass, Equals, string(StorageArchive))
		c.Assert(object.Type, Equals, "Normal")
	}

	// Head object
	meta, err := s.archiveBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	_, ok := meta["X-Kss-Restore"]
	c.Assert(ok, Equals, false)
	c.Assert(meta.Get("X-Kss-Storage-Class"), Equals, "Archive")

	// Error restore object
	err = s.archiveBucket.RestoreObject("notexistobject")
	c.Assert(err, NotNil)

	// Restore object
	err = s.archiveBucket.RestoreObject(objectName)
	c.Assert(err, IsNil)

	// Head object
	meta, err = s.archiveBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Restore"), Equals, "ongoing-request=\"true\"")
	c.Assert(meta.Get("X-Kss-Storage-Class"), Equals, "Archive")
}

// TestRestoreObjectWithXml
func (s *Ks3BucketSuite) TestRestoreObjectWithConfig(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName, StorageClass(StorageArchive))
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	objectName := objectNamePrefix + RandStr(8)

	// Put object
	err = bucket.PutObject(objectName, strings.NewReader("123456789"), ObjectStorageClass(StorageArchive))
	c.Assert(err, IsNil)

	var restoreConfig RestoreConfiguration
	restoreConfig.Days = 2

	err = bucket.RestoreObjectDetail(objectName, restoreConfig)
	c.Assert(err, IsNil)

	objectName = objectNamePrefix + RandStr(8)
	err = bucket.PutObject(objectName, strings.NewReader("123456789"), ObjectStorageClass(StorageArchive))
	c.Assert(err, IsNil)
	restoreConfig.JobParameters = &RestoreJobParameters{
		Tier: string(RestoreBulk),
	}
	err = bucket.RestoreObjectDetail(objectName, restoreConfig)
	c.Assert(err, IsNil)

	objectName = objectNamePrefix + RandStr(8)
	err = bucket.PutObject(objectName, strings.NewReader("123456789"), ObjectStorageClass(StorageArchive))
	c.Assert(err, IsNil)
	restoreConfig.Days = 0
	err = bucket.RestoreObjectDetail(objectName, restoreConfig)
	c.Assert(err, IsNil)

	ForceDeleteBucket(client, bucketName, c)
}

// TestRestoreObjectWithXml
func (s *Ks3BucketSuite) TestRestoreObjectWithXml(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName, StorageClass(StorageArchive))
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	objectName := objectNamePrefix + RandStr(8)

	// Put object
	err = bucket.PutObject(objectName, strings.NewReader("123456789"), ObjectStorageClass(StorageArchive))
	c.Assert(err, IsNil)

	xmlConfig := `<RestoreRequest><Days>7</Days></RestoreRequest>`

	err = bucket.RestoreObjectXML(objectName, xmlConfig)
	c.Assert(err, IsNil)
	ForceDeleteBucket(client, bucketName, c)
}

// TestProcessObject
func (s *Ks3BucketSuite) TestProcessObject(c *C) {
	objectName := objectNamePrefix + RandStr(8) + ".jpg"
	err := s.bucket.PutObjectFromFile(objectName, "../sample/BingWallpaper-2015-11-07.jpg")
	c.Assert(err, IsNil)

	// If bucket-name not specified, it is saved to the current bucket by default.
	destObjName := objectNamePrefix + RandStr(8) + "-dest.jpg"
	process := fmt.Sprintf("image/resize,w_100|sys/saveas,o_%v", base64.URLEncoding.EncodeToString([]byte(destObjName)))
	result, err := s.bucket.ProcessObject(objectName, process)
	c.Assert(err, IsNil)
	exist, _ := s.bucket.IsObjectExist(destObjName)
	c.Assert(exist, Equals, true)
	c.Assert(result.Bucket, Equals, "")
	c.Assert(result.Object, Equals, destObjName)

	destObjName = objectNamePrefix + RandStr(8) + "-dest.jpg"
	process = fmt.Sprintf("image/resize,w_100|sys/saveas,o_%v,b_%v", base64.URLEncoding.EncodeToString([]byte(destObjName)), base64.URLEncoding.EncodeToString([]byte(s.bucket.BucketName)))
	result, err = s.bucket.ProcessObject(objectName, process)
	c.Assert(err, IsNil)
	exist, _ = s.bucket.IsObjectExist(destObjName)
	c.Assert(exist, Equals, true)
	c.Assert(result.Bucket, Equals, s.bucket.BucketName)
	c.Assert(result.Object, Equals, destObjName)

	//no support process
	process = fmt.Sprintf("image/resize,w_100|saveas,o_%v,b_%v", base64.URLEncoding.EncodeToString([]byte(destObjName)), base64.URLEncoding.EncodeToString([]byte(s.bucket.BucketName)))
	result, err = s.bucket.ProcessObject(objectName, process)
	c.Assert(err, NotNil)
}

// Private
func CreateFileAndWrite(fileName string, data []byte) error {
	os.Remove(fileName)

	fo, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer fo.Close()

	bytes, err := fo.Write(data)
	if err != nil {
		return err
	}

	if bytes != len(data) {
		return fmt.Errorf(fmt.Sprintf("write %d bytes not equal data length %d", bytes, len(data)))
	}

	return nil
}

// Compare the content between fileL and fileR
func compareFiles(fileL string, fileR string) (bool, error) {
	finL, err := os.Open(fileL)
	if err != nil {
		return false, err
	}
	defer finL.Close()

	finR, err := os.Open(fileR)
	if err != nil {
		return false, err
	}
	defer finR.Close()

	statL, err := finL.Stat()
	if err != nil {
		return false, err
	}

	statR, err := finR.Stat()
	if err != nil {
		return false, err
	}

	if statL.Size() != statR.Size() {
		return false, nil
	}

	size := statL.Size()
	if size > 102400 {
		size = 102400
	}

	bufL := make([]byte, size)
	bufR := make([]byte, size)
	for {
		n, _ := finL.Read(bufL)
		if 0 == n {
			break
		}

		n, _ = finR.Read(bufR)
		if 0 == n {
			break
		}

		if !bytes.Equal(bufL, bufR) {
			return false, nil
		}
	}

	return true, nil
}

// Compare the content of file and data
func compareFileData(file string, data []byte) (bool, error) {
	fin, err := os.Open(file)
	if err != nil {
		return false, err
	}
	defer fin.Close()

	stat, err := fin.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size() != (int64)(len(data)) {
		return false, nil
	}

	buf := make([]byte, stat.Size())
	n, err := fin.Read(buf)
	if err != nil {
		return false, err
	}
	if stat.Size() != (int64)(n) {
		return false, errors.New("read error")
	}

	if !bytes.Equal(buf, data) {
		return false, nil
	}

	return true, nil
}

func walkDir(dirPth, suffix string) ([]string, error) {
	var files = []string{}
	suffix = strings.ToUpper(suffix)
	err := filepath.Walk(dirPth,
		func(filename string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() {
				return nil
			}
			if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) {
				files = append(files, filename)
			}
			return nil
		})
	return files, err
}

func removeTempFiles(path string, prefix string) error {
	files, err := walkDir(path, prefix)
	if err != nil {
		return nil
	}

	for _, file := range files {
		os.Remove(file)
	}

	return nil
}

func isFileExist(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func readBody(body io.ReadCloser) (string, error) {
	data, err := ioutil.ReadAll(body)
	body.Close()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *Ks3BucketSuite) getObject(objects []ObjectProperties, object string) (bool, ObjectProperties) {
	for _, v := range objects {
		if v.Key == object {
			return true, v
		}
	}
	return false, ObjectProperties{}
}

func (s *Ks3BucketSuite) detectUploadSpeed(bucket *Bucket, c *C) (upSpeed int) {
	objectName := objectNamePrefix + RandStr(8)

	// 1M byte
	textBuffer := RandStr(1024 * 1024)

	// Put string
	startT := time.Now()
	err := bucket.PutObject(objectName, strings.NewReader(textBuffer))
	endT := time.Now()

	c.Assert(err, IsNil)
	err = bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// byte/s
	upSpeed = len(textBuffer) * 1000 / int(endT.UnixNano()/1000/1000-startT.UnixNano()/1000/1000)
	return upSpeed
}

func (s *Ks3BucketSuite) TestPutSingleObjectLimitSpeed(c *C) {

	// create client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(1)
	if err != nil {
		// go version is less than go1.7,not support limit upload speed
		// doesn't run this test
		return
	}
	// set unlimited again
	client.LimitUploadSpeed(0)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	//detect speed:byte/s
	detectSpeed := s.detectUploadSpeed(bucket, c)

	var limitSpeed = 0
	if detectSpeed <= perTokenBandwidthSize*2 {
		limitSpeed = perTokenBandwidthSize
	} else {
		//this situation, the test works better
		limitSpeed = detectSpeed / 2
	}

	// KB/s
	err = client.LimitUploadSpeed(limitSpeed / perTokenBandwidthSize)
	c.Assert(err, IsNil)

	objectName := objectNamePrefix + RandStr(8)

	// 1M byte
	textBuffer := RandStr(1024 * 1024)

	// Put body
	startT := time.Now()
	err = bucket.PutObject(objectName, strings.NewReader(textBuffer))
	endT := time.Now()

	realSpeed := int64(len(textBuffer)) * 1000 / (endT.UnixNano()/1000/1000 - startT.UnixNano()/1000/1000)

	fmt.Printf("detect speed:%d,limit speed:%d,real speed:%d.\n", detectSpeed, limitSpeed, realSpeed)

	c.Assert(float64(realSpeed) < float64(limitSpeed)*1.1, Equals, true)

	if detectSpeed > perTokenBandwidthSize {
		// the minimum uploas limit speed is perTokenBandwidthSize(1024 byte/s)
		c.Assert(float64(realSpeed) > float64(limitSpeed)*0.9, Equals, true)
	}

	// Get object and compare content
	body, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, textBuffer)

	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)

	return
}

func putObjectRoutin(bucket *Bucket, object string, textBuffer *string, notifyChan chan int) error {
	err := bucket.PutObject(object, strings.NewReader(*textBuffer))
	if err == nil {
		notifyChan <- 1
	} else {
		notifyChan <- 0
	}
	return err
}

func (s *Ks3BucketSuite) TestPutManyObjectLimitSpeed(c *C) {
	// create client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(1)
	if err != nil {
		// go version is less than go1.7,not support limit upload speed
		// doesn't run this test
		return
	}
	// set unlimited
	client.LimitUploadSpeed(0)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	//detect speed:byte/s
	detectSpeed := s.detectUploadSpeed(bucket, c)
	var limitSpeed = 0
	if detectSpeed <= perTokenBandwidthSize*2 {
		limitSpeed = perTokenBandwidthSize
	} else {
		limitSpeed = detectSpeed / 2
	}

	// KB/s
	err = client.LimitUploadSpeed(limitSpeed / perTokenBandwidthSize)
	c.Assert(err, IsNil)

	// object1
	objectNameFirst := objectNamePrefix + RandStr(8)
	objectNameSecond := objectNamePrefix + RandStr(8)

	// 1M byte
	textBuffer := RandStr(1024 * 1024)

	objectCount := 2
	notifyChan := make(chan int, objectCount)

	//start routin
	startT := time.Now()
	go putObjectRoutin(bucket, objectNameFirst, &textBuffer, notifyChan)
	go putObjectRoutin(bucket, objectNameSecond, &textBuffer, notifyChan)

	// wait routin end
	sum := int(0)
	for j := 0; j < objectCount; j++ {
		result := <-notifyChan
		sum += result
	}
	endT := time.Now()

	realSpeed := len(textBuffer) * 2 * 1000 / int(endT.UnixNano()/1000/1000-startT.UnixNano()/1000/1000)
	c.Assert(float64(realSpeed) < float64(limitSpeed)*1.1, Equals, true)

	if detectSpeed > perTokenBandwidthSize {
		// the minimum uploas limit speed is perTokenBandwidthSize(1024 byte/s)
		c.Assert(float64(realSpeed) > float64(limitSpeed)*0.9, Equals, true)
	}
	c.Assert(sum, Equals, 2)

	// Get object and compare content
	body, err := bucket.GetObject(objectNameFirst)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, textBuffer)

	body, err = bucket.GetObject(objectNameSecond)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, textBuffer)

	// clear bucket and object
	bucket.DeleteObject(objectNameFirst)
	bucket.DeleteObject(objectNameSecond)
	client.DeleteBucket(bucketName)

	fmt.Printf("detect speed:%d,limit speed:%d,real speed:%d.\n", detectSpeed, limitSpeed, realSpeed)

	return
}

func (s *Ks3BucketSuite) TestPutMultipartObjectLimitSpeed(c *C) {

	// create client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(1)
	if err != nil {
		// go version is less than go1.7,not support limit upload speed
		// doesn't run this test
		return
	}
	// set unlimited
	client.LimitUploadSpeed(0)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	//detect speed:byte/s
	detectSpeed := s.detectUploadSpeed(bucket, c)

	var limitSpeed = 0
	if detectSpeed <= perTokenBandwidthSize*2 {
		limitSpeed = perTokenBandwidthSize
	} else {
		//this situation, the test works better
		limitSpeed = detectSpeed / 2
	}

	// KB/s
	err = client.LimitUploadSpeed(limitSpeed / perTokenBandwidthSize)
	c.Assert(err, IsNil)

	objectName := objectNamePrefix + RandStr(8)
	fileName := "." + string(os.PathSeparator) + objectName

	// 1M byte
	fileSize := 0
	textBuffer := RandStr(1024 * 1024)
	if detectSpeed < perTokenBandwidthSize {
		ioutil.WriteFile(fileName, []byte(textBuffer), 0644)
		f, err := os.Stat(fileName)
		c.Assert(err, IsNil)

		fileSize = int(f.Size())
		c.Assert(fileSize, Equals, len(textBuffer))

	} else {
		loopCount := 5
		f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
		c.Assert(err, IsNil)

		for i := 0; i < loopCount; i++ {
			f.Write([]byte(textBuffer))
		}

		fileInfo, err := f.Stat()
		c.Assert(err, IsNil)

		fileSize = int(fileInfo.Size())
		c.Assert(fileSize, Equals, len(textBuffer)*loopCount)

		f.Close()
	}

	// Put body
	startT := time.Now()
	err = bucket.UploadFile(objectName, fileName, 100*1024, Routines(3), Checkpoint(true, ""))
	endT := time.Now()

	c.Assert(err, IsNil)
	realSpeed := fileSize * 1000 / int(endT.UnixNano()/1000/1000-startT.UnixNano()/1000/1000)
	c.Assert(float64(realSpeed) < float64(limitSpeed)*1.1, Equals, true)

	if detectSpeed > perTokenBandwidthSize {
		// the minimum uploas limit speed is perTokenBandwidthSize(1024 byte/s)
		c.Assert(float64(realSpeed) > float64(limitSpeed)*0.9, Equals, true)
	}

	// Get object and compare content
	body, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)

	fileBody, err := ioutil.ReadFile(fileName)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, string(fileBody))

	// delete bucket、object、file
	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
	os.Remove(fileName)

	fmt.Printf("detect speed:%d,limit speed:%d,real speed:%d.\n", detectSpeed, limitSpeed, realSpeed)

	return
}

func (s *Ks3BucketSuite) TestPutObjectFromFileLimitSpeed(c *C) {
	// create client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(1)
	if err != nil {
		// go version is less than go1.7,not support limit upload speed
		// doesn't run this test
		return
	}
	// set unlimited
	client.LimitUploadSpeed(0)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	//detect speed:byte/s
	detectSpeed := s.detectUploadSpeed(bucket, c)

	var limitSpeed = 0
	if detectSpeed <= perTokenBandwidthSize*2 {
		limitSpeed = perTokenBandwidthSize
	} else {
		//this situation, the test works better
		limitSpeed = detectSpeed / 2
	}

	// KB/s
	err = client.LimitUploadSpeed(limitSpeed / perTokenBandwidthSize)
	c.Assert(err, IsNil)

	objectName := objectNamePrefix + RandStr(8)
	fileName := "." + string(os.PathSeparator) + objectName

	// 1M byte
	fileSize := 0
	textBuffer := RandStr(1024 * 1024)
	if detectSpeed < perTokenBandwidthSize {
		ioutil.WriteFile(fileName, []byte(textBuffer), 0644)
		f, err := os.Stat(fileName)
		c.Assert(err, IsNil)

		fileSize = int(f.Size())
		c.Assert(fileSize, Equals, len(textBuffer))

	} else {
		loopCount := 2
		f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
		c.Assert(err, IsNil)

		for i := 0; i < loopCount; i++ {
			f.Write([]byte(textBuffer))
		}

		fileInfo, err := f.Stat()
		c.Assert(err, IsNil)

		fileSize = int(fileInfo.Size())
		c.Assert(fileSize, Equals, len(textBuffer)*loopCount)

		f.Close()
	}

	// Put body
	startT := time.Now()
	err = bucket.PutObjectFromFile(objectName, fileName)
	endT := time.Now()

	c.Assert(err, IsNil)
	realSpeed := fileSize * 1000 / int(endT.UnixNano()/1000/1000-startT.UnixNano()/1000/1000)
	c.Assert(float64(realSpeed) < float64(limitSpeed)*1.1, Equals, true)

	if detectSpeed > perTokenBandwidthSize {
		// the minimum uploas limit speed is perTokenBandwidthSize(1024 byte/s)
		c.Assert(float64(realSpeed) > float64(limitSpeed)*0.9, Equals, true)
	}

	// Get object and compare content
	body, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)

	fileBody, err := ioutil.ReadFile(fileName)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, string(fileBody))

	// delete bucket、file、object
	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
	os.Remove(fileName)

	fmt.Printf("detect speed:%d,limit speed:%d,real speed:%d.\n", detectSpeed, limitSpeed, realSpeed)

	return
}

// upload speed limit parameters will not affect download speed
func (s *Ks3BucketSuite) TestUploadObjectLimitSpeed(c *C) {
	// create limit client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	tokenCount := 1
	err = client.LimitUploadSpeed(tokenCount)
	if err != nil {
		// go version is less than go1.7,not support limit upload speed
		// doesn't run this test
		return
	}
	// set unlimited
	client.LimitUploadSpeed(0)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	//first:upload a object
	textBuffer := RandStr(1024 * 100)
	objectName := objectNamePrefix + RandStr(8)
	err = bucket.PutObject(objectName, strings.NewReader(textBuffer))
	c.Assert(err, IsNil)

	// limit upload speed
	err = client.LimitUploadSpeed(tokenCount)
	c.Assert(err, IsNil)

	// then download the object
	startT := time.Now()
	body, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)

	str, err := readBody(body)
	c.Assert(err, IsNil)
	endT := time.Now()

	c.Assert(str, Equals, textBuffer)

	// byte/s
	downloadSpeed := len(textBuffer) * 1000 / int(endT.UnixNano()/1000/1000-startT.UnixNano()/1000/1000)

	// upload speed limit parameters will not affect download speed
	c.Assert(downloadSpeed > 2*tokenCount*perTokenBandwidthSize, Equals, true)

	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
}

// test LimitUploadSpeed failure
func (s *Ks3BucketSuite) TestLimitUploadSpeedFail(c *C) {
	// create limit client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(-1)
	c.Assert(err, NotNil)

	client.Config = nil
	err = client.LimitUploadSpeed(100)
	c.Assert(err, NotNil)
}

// upload webp object
func (s *Ks3BucketSuite) TestUploadObjectWithWebpFormat(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	// create webp file
	textBuffer := RandStr(1024)
	objectName := objectNamePrefix + RandStr(8)
	fileName := "." + string(os.PathSeparator) + objectName + ".webp"
	ioutil.WriteFile(fileName, []byte(textBuffer), 0644)
	_, err = os.Stat(fileName)
	c.Assert(err, IsNil)

	err = bucket.PutObjectFromFile(objectName, fileName)
	c.Assert(err, IsNil)

	// check object content-type
	props, err := bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(props["Content-Type"][0], Equals, "image/webp")

	os.Remove(fileName)
	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
}

func (s *Ks3BucketSuite) TestPutObjectTagging(c *C) {
	// put object with tagging
	objectName := objectNamePrefix + RandStr(8)
	tag1 := Tag{
		Key:   RandStr(8),
		Value: RandStr(9),
	}
	tag2 := Tag{
		Key:   RandStr(10),
		Value: RandStr(11),
	}
	tagging := Tagging{
		Tags: []Tag{tag1, tag2},
	}
	err := s.bucket.PutObject(objectName, strings.NewReader(RandStr(1024)), SetTagging(tagging))
	c.Assert(err, IsNil)

	headers, err := s.bucket.GetObjectDetailedMeta(objectName)
	taggingCount, err := strconv.Atoi(headers["X-Kss-Tagging-Count"][0])
	c.Assert(err, IsNil)
	c.Assert(taggingCount, Equals, 2)

	// copy object with default option
	destObjectName := objectNamePrefix + RandStr(8)
	_, err = s.bucket.CopyObject(objectName, destObjectName)
	c.Assert(err, IsNil)
	headers, err = s.bucket.GetObjectDetailedMeta(destObjectName)
	taggingCount, err = strconv.Atoi(headers["X-Kss-Tagging-Count"][0])
	c.Assert(err, IsNil)
	c.Assert(taggingCount, Equals, 2)

	// delete object tagging
	err = s.bucket.DeleteObjectTagging(objectName)
	c.Assert(err, IsNil)

	// get object tagging again
	taggingResult, err := s.bucket.GetObjectTagging(objectName)
	c.Assert(err, IsNil)
	c.Assert(len(taggingResult.Tags), Equals, 0)

	// put tagging
	tag := Tag{
		Key:   RandStr(8),
		Value: RandStr(16),
	}
	tagging.Tags = []Tag{tag}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, IsNil)

	taggingResult, err = s.bucket.GetObjectTagging(objectName)
	c.Assert(len(taggingResult.Tags), Equals, 1)
	c.Assert(taggingResult.Tags[0].Key, Equals, tag.Key)
	c.Assert(taggingResult.Tags[0].Value, Equals, tag.Value)

	//put tagging, the length of the key exceeds 128
	tag = Tag{
		Key:   RandStr(129),
		Value: RandStr(16),
	}
	tagging.Tags = []Tag{tag}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	//put tagging, the length of the value exceeds 256
	tag = Tag{
		Key:   RandStr(8),
		Value: RandStr(257),
	}
	tagging.Tags = []Tag{tag}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	//put tagging, the lens of tags exceed 10
	tagging.Tags = []Tag{}
	for i := 0; i < 11; i++ {
		tag = Tag{
			Key:   RandStr(8),
			Value: RandStr(16),
		}
		tagging.Tags = append(tagging.Tags, tag)
	}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	//put tagging, invalid value of tag key
	tag = Tag{
		Key:   RandStr(8) + "&",
		Value: RandStr(16),
	}
	tagging.Tags = []Tag{tag}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	//put tagging, invalid value of tag value
	tag = Tag{
		Key:   RandStr(8),
		Value: RandStr(16) + "&",
	}
	tagging.Tags = []Tag{tag}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	//put tagging, repeated tag keys
	tag1 = Tag{
		Key:   RandStr(8),
		Value: RandStr(16),
	}
	tag2 = Tag{
		Key:   tag1.Key,
		Value: RandStr(16),
	}
	tagging.Tags = []Tag{tag1, tag2}
	err = s.bucket.PutObjectTagging(objectName, tagging)
	c.Assert(err, NotNil)

	s.bucket.DeleteObject(destObjectName)
	s.bucket.DeleteObject(objectName)
}

func (s *Ks3BucketSuite) TestGetObjectTagging(c *C) {
	// get object which has 2 tags
	objectName := objectNamePrefix + RandStr(8)
	tag1 := Tag{
		Key:   RandStr(8),
		Value: RandStr(9),
	}
	tag2 := Tag{
		Key:   RandStr(10),
		Value: RandStr(11),
	}

	taggingInfo := Tagging{
		Tags: []Tag{tag1, tag2},
	}

	err := s.bucket.PutObject(objectName, strings.NewReader(RandStr(1024)), SetTagging(taggingInfo))
	c.Assert(err, IsNil)

	tagging, err := s.bucket.GetObjectTagging(objectName)
	c.Assert(len(tagging.Tags), Equals, 2)
	if tagging.Tags[0].Key == tag1.Key {
		c.Assert(tagging.Tags[0].Value, Equals, tag1.Value)
		c.Assert(tagging.Tags[1].Key, Equals, tag2.Key)
		c.Assert(tagging.Tags[1].Value, Equals, tag2.Value)
	} else {
		c.Assert(tagging.Tags[0].Key, Equals, tag2.Key)
		c.Assert(tagging.Tags[0].Value, Equals, tag2.Value)
		c.Assert(tagging.Tags[1].Key, Equals, tag1.Key)
		c.Assert(tagging.Tags[1].Value, Equals, tag1.Value)
	}

	// get tagging of an object that is not exist
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
	tagging, err = s.bucket.GetObjectTagging(objectName)
	c.Assert(err, NotNil)
	c.Assert(len(tagging.Tags), Equals, 0)

	// get object which has no tag
	objectName = objectNamePrefix + RandStr(8)
	err = s.bucket.PutObject(objectName, strings.NewReader(RandStr(1024)))
	c.Assert(err, IsNil)
	tagging, err = s.bucket.GetObjectTagging(objectName)
	c.Assert(err, IsNil)
	c.Assert(len(tagging.Tags), Equals, 0)

	// copy object, with tagging option
	destObjectName := objectName + "-dest"
	tagging.Tags = []Tag{tag1, tag2}
	_, err = s.bucket.CopyObject(objectName, destObjectName, SetTagging(taggingInfo))
	c.Assert(err, IsNil)
	tagging, err = s.bucket.GetObjectTagging(objectName)
	c.Assert(err, IsNil)
	c.Assert(len(tagging.Tags), Equals, 0)

	// copy object, with tagging option, the value of tagging directive is "REPLACE"
	tagging.Tags = []Tag{tag1, tag2}
	_, err = s.bucket.CopyObject(objectName, destObjectName, SetTagging(taggingInfo), TaggingDirective(TaggingReplace))
	c.Assert(err, IsNil)
	tagging, err = s.bucket.GetObjectTagging(destObjectName)
	c.Assert(err, IsNil)
	c.Assert(len(tagging.Tags), Equals, 2)
	if tagging.Tags[0].Key == tag1.Key {
		c.Assert(tagging.Tags[0].Value, Equals, tag1.Value)
		c.Assert(tagging.Tags[1].Key, Equals, tag2.Key)
		c.Assert(tagging.Tags[1].Value, Equals, tag2.Value)
	} else {
		c.Assert(tagging.Tags[0].Key, Equals, tag2.Key)
		c.Assert(tagging.Tags[0].Value, Equals, tag2.Value)
		c.Assert(tagging.Tags[1].Key, Equals, tag1.Key)
		c.Assert(tagging.Tags[1].Value, Equals, tag1.Value)
	}

	s.bucket.DeleteObject(objectName)
	s.bucket.DeleteObject(destObjectName)
}

func (s *Ks3BucketSuite) TestDeleteObjectTagging(c *C) {
	// delete object tagging, the object is not exist
	objectName := objectNamePrefix + RandStr(8)
	err := s.bucket.DeleteObjectTagging(objectName)
	c.Assert(err, NotNil)

	// delete object tagging
	tag := Tag{
		Key:   RandStr(8),
		Value: RandStr(16),
	}
	tagging := Tagging{
		Tags: []Tag{tag},
	}
	err = s.bucket.PutObject(objectName, strings.NewReader(RandStr(1024)), SetTagging(tagging))
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObjectTagging(objectName)
	c.Assert(err, IsNil)
	taggingResult, err := s.bucket.GetObjectTagging(objectName)
	c.Assert(err, IsNil)
	c.Assert(len(taggingResult.Tags), Equals, 0)

	//delete object tagging again
	err = s.bucket.DeleteObjectTagging(objectName)
	c.Assert(err, IsNil)

	s.bucket.DeleteObject(objectName)
}

func (s *Ks3BucketSuite) TestUploadFileMimeShtml(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	objectName := objectNamePrefix + RandStr(8)
	fileName := "ks3-sdk-test-file-" + RandLowStr(5) + ".shtml"
	CreateFile(fileName, "123", c)

	err = bucket.PutObjectFromFile(objectName, fileName)
	c.Assert(err, IsNil)

	headResult, err := bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	strContentType := headResult.Get("Content-Type")
	c.Assert(strings.Contains(strContentType, "text/html"), Equals, true)
	os.Remove(fileName)
	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestOptionsMethod(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	// put bucket cors
	var rule = CORSRule{
		AllowedOrigin: []string{"www.ksyun.com"},
		AllowedMethod: []string{"PUT", "GET", "POST"},
		AllowedHeader: []string{"x-ks3-meta-author"},
		ExposeHeader:  []string{"x-ks3-meta-name"},
		MaxAgeSeconds: 100,
	}

	// set cors
	err = client.SetBucketCORS(bucketName, []CORSRule{rule})
	c.Assert(err, IsNil)

	// bucket options success
	options := []Option{}
	originOption := Origin("www.ksyun.com")
	acMethodOption := ACReqMethod("PUT")
	acHeadersOption := ACReqHeaders("x-ks3-meta-author")
	options = append(options, originOption)
	options = append(options, acMethodOption)
	options = append(options, acHeadersOption)
	_, err = bucket.OptionsMethod("", options...)
	c.Assert(err, IsNil)

	// options failure
	options = []Option{}
	originOption = Origin("www.ksyun.com")
	acMethodOption = ACReqMethod("PUT")
	acHeadersOption = ACReqHeaders("x-ks3-meta-author-1")
	options = append(options, originOption)
	options = append(options, acMethodOption)
	options = append(options, acHeadersOption)
	_, err = bucket.OptionsMethod("", options...)
	c.Assert(err, NotNil)

	// put object
	objectName := objectNamePrefix + RandStr(8)
	context := RandStr(100)
	err = bucket.PutObject(objectName, strings.NewReader(context))
	c.Assert(err, IsNil)

	// object options success
	options = []Option{}
	originOption = Origin("www.ksyun.com")
	acMethodOption = ACReqMethod("PUT")
	acHeadersOption = ACReqHeaders("x-ks3-meta-author")
	options = append(options, originOption)
	options = append(options, acMethodOption)
	options = append(options, acHeadersOption)
	_, err = bucket.OptionsMethod("", options...)
	c.Assert(err, IsNil)

	// options failure
	options = []Option{}
	originOption = Origin("www.ksyun.com")
	acMethodOption = ACReqMethod("PUT")
	acHeadersOption = ACReqHeaders("x-ks3-meta-author-1")
	options = append(options, originOption)
	options = append(options, acMethodOption)
	options = append(options, acHeadersOption)
	_, err = bucket.OptionsMethod("", options...)
	c.Assert(err, NotNil)

	bucket.DeleteObject(objectName)
	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestBucketTrafficLimitObject1(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	var respHeader http.Header
	var qosDelayTime string
	var traffic int64 = 819220 // 100KB
	maxTraffic := traffic * 120 / 100

	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"
	fd, err := os.Open(localFile)
	c.Assert(err, IsNil)
	defer fd.Close()

	tryGetFileSize := func(f *os.File) int64 {
		fInfo, _ := f.Stat()
		return fInfo.Size()
	}
	contentLength := tryGetFileSize(fd) * 8

	// put object
	start := time.Now().UnixNano() / 1000 / 1000
	err = bucket.PutObject(objectName, fd, TrafficLimitHeader(traffic), GetResponseHeader(&respHeader))
	c.Assert(err, IsNil)
	endingTime := time.Now().UnixNano() / 1000 / 1000
	costT := endingTime - start
	costV := contentLength * 1000 / costT // bit * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)
	qosDelayTime = GetQosDelayTime(respHeader)
	c.Assert(len(qosDelayTime) > 0, Equals, true)

	// putobject without TrafficLimit
	//
	// fd, err = os.Open(localFile)
	// c.Assert(err, IsNil)
	// defer fd.Close()
	// start = time.Now().UnixNano() / 1000 / 1000
	// err = bucket.PutObject(objectName, fd)
	// c.Assert(err, IsNil)
	// endingTime = time.Now().UnixNano() / 1000 / 1000
	// costT = endingTime - start
	// costV = contentLength * 1000 / costT  // bit * 1000 / Millisecond = bit/s
	// testLogger.Println(traffic, maxTraffic, contentLength, costT, costV)
	// c.Assert((costV < maxTraffic), Equals, true)

	// get object to file
	newFile := "test-file-" + RandStr(10)
	start = time.Now().UnixNano() / 1000 / 1000
	err = bucket.GetObjectToFile(objectName, newFile, TrafficLimitHeader(traffic))
	c.Assert(err, IsNil)
	endingTime = time.Now().UnixNano() / 1000 / 1000
	costT = endingTime - start
	costV = contentLength * 1000 / costT // bit * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)
	os.Remove(newFile)

	// append object
	newFile = "test-file-" + RandStr(10)
	objectKey := objectNamePrefix + RandStr(8)
	var nextPos int64
	fd, err = os.Open(localFile)
	c.Assert(err, IsNil)
	defer fd.Close()
	start = time.Now().UnixNano() / 1000 / 1000
	nextPos, err = bucket.AppendObject(objectKey, strings.NewReader(RandStr(18)), nextPos)
	c.Assert(err, IsNil)

	var respAppendHeader http.Header
	nextPos, err = bucket.AppendObject(objectKey, fd, nextPos, TrafficLimitHeader(traffic), GetResponseHeader(&respAppendHeader))
	c.Assert(err, IsNil)
	endingTime = time.Now().UnixNano() / 1000 / 1000
	costT = endingTime - start
	costV = contentLength * 1000 / costT // bit * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)
	qosDelayTime = GetQosDelayTime(respAppendHeader)
	c.Assert(len(qosDelayTime) > 0, Equals, true)

	err = bucket.GetObjectToFile(objectKey, newFile, TrafficLimitHeader(traffic))
	c.Assert(err, IsNil)
	err = bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// put object with url
	fd, err = os.Open(localFile)
	c.Assert(err, IsNil)
	defer fd.Close()
	strURL, err := bucket.SignURL(objectName, HTTPPut, 60, TrafficLimitParam(traffic))
	start = time.Now().UnixNano() / 1000 / 1000
	err = bucket.PutObjectWithURL(strURL, fd)
	c.Assert(err, IsNil)
	endingTime = time.Now().UnixNano() / 1000 / 1000
	costT = endingTime - start
	costV = contentLength * 1000 / costT // bit * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)

	// get object with url
	newFile = "test-file-" + RandStr(10)
	strURL, err = bucket.SignURL(objectName, HTTPGet, 60, TrafficLimitParam(traffic))
	c.Assert(err, IsNil)
	start = time.Now().UnixNano() / 1000 / 1000
	err = bucket.GetObjectToFileWithURL(strURL, newFile)
	c.Assert(err, IsNil)
	endingTime = time.Now().UnixNano() / 1000 / 1000
	costT = endingTime - start
	costV = contentLength * 1000 / costT // bit * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)
	os.Remove(newFile)

	// copy object
	destObjectName := objectNamePrefix + RandStr(8)
	_, err = bucket.CopyObject(objectName, destObjectName, TrafficLimitHeader(traffic))
	c.Assert(err, IsNil)
	err = bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)

	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestBucketTrafficLimitUpload(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)

	var traffic int64 = 819220 // 100KB
	maxTraffic := traffic * 120 / 100
	contentLength := 500 * 1024

	var fileName = "test-file-" + RandStr(8)
	objectName := objectNamePrefix + RandStr(8)
	content := RandStr(contentLength)
	CreateFile(fileName, content, c)

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)

	options := []Option{
		Expires(futureDate), Meta("my", "myprop"),
	}

	fd, err := os.Open(fileName)
	c.Assert(err, IsNil)
	defer fd.Close()

	imur, err := bucket.InitiateMultipartUpload(objectName, options...)
	c.Assert(err, IsNil)
	var parts []UploadPart
	start := time.Now().UnixNano() / 1000 / 1000
	for _, chunk := range chunks {
		fd.Seek(chunk.Offset, os.SEEK_SET)
		part, err := bucket.UploadPart(imur, fd, chunk.Size, chunk.Number, TrafficLimitHeader(traffic))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}
	_, err = bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	endingTime := time.Now().UnixNano() / 1000 / 1000
	costT := endingTime - start
	costV := int64(contentLength) * 8 * 1000 / costT // B * 8 * 1000 / Millisecond = bit/s
	c.Assert((costV < maxTraffic), Equals, true)
	os.Remove(fileName)

	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestPutObjectWithForbidOverWrite(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	bucket, err := client.Bucket(bucketName)

	contentLength := 1024
	objectName := objectNamePrefix + RandStr(8)
	content := RandStr(contentLength)

	// first put success
	err = bucket.PutObject(objectName, strings.NewReader(content), ForbidOverWrite(true))
	c.Assert(err, IsNil)

	// second put failure with ForbidOverWrite true
	var respHeader http.Header
	err = bucket.PutObject(objectName, strings.NewReader(content), ForbidOverWrite(true), GetResponseHeader(&respHeader))
	c.Assert(err, NotNil)

	// third  put success with ForbidOverWrite false
	err = bucket.PutObject(objectName, strings.NewReader(content), ForbidOverWrite(false))
	c.Assert(err, IsNil)

	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestCopyObjectWithForbidOverWrite(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	bucket, err := client.Bucket(bucketName)

	contentLength := 1024
	objectName := objectNamePrefix + RandStr(8)
	content := RandStr(contentLength)

	err = bucket.PutObject(objectName, strings.NewReader(content))
	c.Assert(err, IsNil)

	// first copy success
	copyObjectName := objectName + "-copy"
	_, err = bucket.CopyObject(objectName, copyObjectName, ForbidOverWrite(true))
	c.Assert(err, IsNil)

	// second copy failure with ForbidOverWrite true
	var respHeader http.Header
	_, err = bucket.CopyObject(objectName, copyObjectName, ForbidOverWrite(true), GetResponseHeader(&respHeader))
	c.Assert(err, NotNil)

	// third  copy success with ForbidOverWrite false
	_, err = bucket.CopyObject(objectName, copyObjectName, ForbidOverWrite(false))
	c.Assert(err, IsNil)

	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestDeleteObjectsWithSpecialCharacter(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	bucket, err := client.Bucket(bucketName)

	contentLength := 100
	objectName1 := objectNamePrefix + RandStr(8) + "<-->+&*\r%%"
	objectName2 := objectNamePrefix + RandStr(8) + "\r&*\r%%"
	//objectName2 := objectNamePrefix + RandStr(8) + "%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2F%C0%AE%C0%AE%2Fetc%2Fprofile"
	//objectName2, err = url.QueryUnescape(objectName2)

	c.Assert(err, IsNil)
	content := RandStr(contentLength)

	err = bucket.PutObject(objectName1, strings.NewReader(content))
	c.Assert(err, IsNil)

	err = bucket.PutObject(objectName2, strings.NewReader(content))
	c.Assert(err, IsNil)

	// delete objectName1 objectName2
	objectKeys := []string{objectName1, objectName2}
	_, err = bucket.DeleteObjects(objectKeys)
	c.Assert(err, IsNil)

	// objectName1 is not exist
	exist, err := bucket.IsObjectExist(objectName1)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	// objectName2 is not exist
	exist, err = bucket.IsObjectExist(objectName2)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	ForceDeleteBucket(client, bucketName, c)
}

// TestGetObjectRangeBehavior
func (s *Ks3BucketSuite) TestGetObjectRangeBehavior(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	bucket, err := client.Bucket(bucketName)

	objectName := objectNamePrefix + RandStr(8)
	objectLen := 1000
	objectValue := RandStr(objectLen)

	// Put
	err = bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Range 1
	options := []Option{
		RangeBehavior("standard"),
		Range(1000, 2000),
	}
	resp, err := bucket.GetObject(objectName, options...)
	c.Assert(resp, IsNil)
	c.Assert(err.(ServiceError).StatusCode, Equals, 416)

	// Range 2
	options = []Option{
		RangeBehavior("standard"),
		Range(0, 2000),
	}
	resp, err = bucket.GetObject(objectName, options...)
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(resp)
	resp.Close()
	str := string(data)
	c.Assert(len(str), Equals, 1000)
	c.Assert(resp.(*Response).StatusCode, Equals, 206)

	// Range 3
	options = []Option{
		RangeBehavior("standard"),
		Range(500, 2000),
	}
	resp, err = bucket.GetObject(objectName, options...)
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(resp)
	resp.Close()
	str = string(data)
	c.Assert(len(str), Equals, 500)
	c.Assert(resp.(*Response).StatusCode, Equals, 206)

	ForceDeleteBucket(client, bucketName, c)
}

// RangeBehavior  is an option to set Range value, such as "standard"
func MyRangeBehavior(value string) Option {
	return SetHeader(HTTPHeaderKs3RangeBehavior, value)
}

// TestUserSetHeader
func (s *Ks3BucketSuite) TestSupportUserSetHeader(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	bucket, err := client.Bucket(bucketName)

	objectName := objectNamePrefix + RandStr(8)
	objectLen := 1000
	objectValue := RandStr(objectLen)

	// Put
	err = bucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Range 1
	options := []Option{
		MyRangeBehavior("standard"),
		Range(1000, 2000),
	}
	resp, err := bucket.GetObject(objectName, options...)
	c.Assert(resp, IsNil)
	c.Assert(err.(ServiceError).StatusCode, Equals, 416)

	// Range 2
	options = []Option{
		MyRangeBehavior("standard"),
		Range(0, 2000),
	}
	resp, err = bucket.GetObject(objectName, options...)
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(resp)
	resp.Close()
	str := string(data)
	c.Assert(len(str), Equals, 1000)
	c.Assert(resp.(*Response).StatusCode, Equals, 206)

	// Range 3
	options = []Option{
		MyRangeBehavior("standard"),
		Range(500, 2000),
	}
	resp, err = bucket.GetObject(objectName, options...)
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(resp)
	resp.Close()
	str = string(data)
	c.Assert(len(str), Equals, 500)
	c.Assert(resp.(*Response).StatusCode, Equals, 206)

	ForceDeleteBucket(client, bucketName, c)
}

func (s *Ks3BucketSuite) TestGetSingleObjectLimitSpeed(c *C) {
	// create client and bucket
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitDownloadSpeed(1)
	if err != nil {
		// go version is less than go1.7,not support limit download speed
		// doesn't run this test
		return
	}

	// set limit download speed as 100KB/s
	limitSpeed := 100
	client.LimitDownloadSpeed(limitSpeed)

	bucketName := bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	objectName := objectNamePrefix + RandStr(8)

	// 1M byte
	textBuffer := RandStr(1024 * 1024)

	// Put body
	err = bucket.PutObject(objectName, strings.NewReader(textBuffer))
	c.Assert(err, IsNil)

	// get object to file
	tempFile := "test-go-sdk-" + RandStr(8)
	startT := time.Now()
	err = bucket.GetObjectToFile(objectName, tempFile)
	endT := time.Now()
	c.Assert(err, IsNil)

	realSpeed := int64(len(textBuffer)) / (endT.UnixNano()/1000/1000/1000 - startT.UnixNano()/1000/1000/1000)
	c.Assert(float64(realSpeed/1024) < float64(limitSpeed)*1.15, Equals, true)
	c.Assert(float64(realSpeed/1024) > float64(limitSpeed)*0.85, Equals, true)

	// Get object and compare content
	fileBody, err := ioutil.ReadFile(tempFile)
	c.Assert(err, IsNil)
	c.Assert(textBuffer, Equals, string(fileBody))

	bucket.DeleteObject(objectName)
	client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)
	os.Remove(tempFile)
}
