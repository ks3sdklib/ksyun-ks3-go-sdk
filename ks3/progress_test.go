// bucket test

package ks3

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"

	. "gopkg.in/check.v1"
)

type Ks3ProgressSuite struct {
	client *Client
	bucket *Bucket
}

var _ = Suite(&Ks3ProgressSuite{})

// SetUpSuite runs once when the suite starts running
func (s *Ks3ProgressSuite) SetUpSuite(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	s.client = client

	s.client.CreateBucket(bucketName)

	bucket, err := s.client.Bucket(bucketName)
	c.Assert(err, IsNil)
	s.bucket = bucket

	testLogger.Println("test progress started")
}

// TearDownSuite runs before each test or benchmark starts running
func (s *Ks3ProgressSuite) TearDownSuite(c *C) {
	// Abort multipart uploads
	keyMarker := KeyMarker("")
	uploadIDMarker := UploadIDMarker("")
	for {
		lmu, err := s.bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
		c.Assert(err, IsNil)
		for _, upload := range lmu.Uploads {
			imur := InitiateMultipartUploadResult{Bucket: bucketName, Key: upload.Key, UploadID: upload.UploadID}
			err = s.bucket.AbortMultipartUpload(imur)
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
		lor, err := s.bucket.ListObjects(marker)
		c.Assert(err, IsNil)
		for _, object := range lor.Objects {
			err = s.bucket.DeleteObject(object.Key)
			c.Assert(err, IsNil)
		}
		marker = Marker(lor.NextMarker)
		if !lor.IsTruncated {
			break
		}
	}

	// Delete bucket
	err := s.client.DeleteBucket(s.bucket.BucketName)
	c.Assert(err, IsNil)

	testLogger.Println("test progress completed")
}

// SetUpTest runs after each test or benchmark runs
func (s *Ks3ProgressSuite) SetUpTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".html")
	c.Assert(err, IsNil)
}

// TearDownTest runs once after all tests or benchmarks have finished running
func (s *Ks3ProgressSuite) TearDownTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".html")
	c.Assert(err, IsNil)
}

// Ks3ProgressListener is the progress listener
type Ks3ProgressListener struct {
	TotalRwBytes int64
}

// ProgressChanged handles progress event
func (listener *Ks3ProgressListener) ProgressChanged(event *ProgressEvent) {
	switch event.EventType {
	case TransferStartedEvent:
		testLogger.Printf("Transfer Started, ConsumedBytes: %d, TotalBytes %d.\n",
			event.ConsumedBytes, event.TotalBytes)
	case TransferDataEvent:
		atomic.AddInt64(&listener.TotalRwBytes, event.RwBytes)
		testLogger.Printf("Transfer Data, ConsumedBytes: %d, TotalBytes %d, %d%%.\n",
			event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
	case TransferCompletedEvent:
		testLogger.Printf("Transfer Completed, ConsumedBytes: %d, TotalBytes %d.\n",
			event.ConsumedBytes, event.TotalBytes)
	case TransferFailedEvent:
		testLogger.Printf("Transfer Failed, ConsumedBytes: %d, TotalBytes %d.\n",
			event.ConsumedBytes, event.TotalBytes)
	default:
	}
}

// TestPutObject
func (s *Ks3ProgressSuite) TestPutObject(c *C) {
	objectName := RandStr(8) + ".jpg"
	localFile := "../sample/The Go Programming Language.html"

	fileInfo, err := os.Stat(localFile)
	c.Assert(err, IsNil)

	// PutObject
	fd, err := os.Open(localFile)
	c.Assert(err, IsNil)
	defer fd.Close()

	progressListener := Ks3ProgressListener{}
	err = s.bucket.PutObject(objectName, fd, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// PutObjectFromFile
	progressListener.TotalRwBytes = 0
	err = s.bucket.PutObjectFromFile(objectName, localFile, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// DoPutObject
	fd, err = os.Open(localFile)
	c.Assert(err, IsNil)
	defer fd.Close()

	request := &PutObjectRequest{
		ObjectKey: objectName,
		Reader:    fd,
	}

	progressListener.TotalRwBytes = 0
	options := []Option{Progress(&progressListener)}
	_, err = s.bucket.DoPutObject(request, options)
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// PutObject size is 0
	progressListener.TotalRwBytes = 0
	err = s.bucket.PutObject(objectName, strings.NewReader(""), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(0))

	testLogger.Println("Ks3ProgressSuite.TestPutObject")
}

// TestSignURL
func (s *Ks3ProgressSuite) SignURLTestFunc(c *C, authVersion AuthVersionType, extraHeaders []string) {
	objectName := objectNamePrefix + RandStr(8)
	filePath := RandLowStr(10)
	content := RandStr(20)
	CreateFile(filePath, content, c)

	oldType := s.bucket.Client.Config.AuthVersion
	oldHeaders := s.bucket.Client.Config.AdditionalHeaders
	s.bucket.Client.Config.AuthVersion = authVersion
	s.bucket.Client.Config.AdditionalHeaders = extraHeaders

	// Sign URL for put
	progressListener := Ks3ProgressListener{}
	str, err := s.bucket.SignURL(objectName, HTTPPut, 60, Progress(&progressListener))
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
	fd, err := os.Open(filePath)
	c.Assert(err, IsNil)
	defer fd.Close()

	err = s.bucket.PutObjectWithURL(str, fd, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(content)))

	// Put object from file with URL
	progressListener.TotalRwBytes = 0
	err = s.bucket.PutObjectFromFileWithURL(str, filePath, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(content)))

	// DoPutObject
	fd, err = os.Open(filePath)
	c.Assert(err, IsNil)
	defer fd.Close()

	progressListener.TotalRwBytes = 0
	options := []Option{Progress(&progressListener)}
	_, err = s.bucket.DoPutObjectWithURL(str, fd, options)
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(content)))

	// Sign URL for get
	str, err = s.bucket.SignURL(objectName, HTTPGet, 60, Progress(&progressListener))
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
	progressListener.TotalRwBytes = 0
	body, err := s.bucket.GetObjectWithURL(str, Progress(&progressListener))
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, content)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(content)))

	// Get object to file with URL
	progressListener.TotalRwBytes = 0
	str, err = s.bucket.SignURL(objectName, HTTPGet, 10, Progress(&progressListener))
	c.Assert(err, IsNil)

	newFile := RandStr(10)
	progressListener.TotalRwBytes = 0
	err = s.bucket.GetObjectToFileWithURL(str, newFile, Progress(&progressListener))
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(content)))
	c.Assert(err, IsNil)
	eq, err := compareFiles(filePath, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	os.Remove(filePath)
	os.Remove(newFile)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	testLogger.Println("Ks3ProgressSuite.TestSignURL")

	s.bucket.Client.Config.AuthVersion = oldType
	s.bucket.Client.Config.AdditionalHeaders = oldHeaders
}

func (s *Ks3ProgressSuite) TestSignURL(c *C) {
	s.SignURLTestFunc(c, AuthV1, []string{})
	s.SignURLTestFunc(c, AuthV2, []string{})
	s.SignURLTestFunc(c, AuthV2, []string{"host", "range", "user-agent"})
}

func (s *Ks3ProgressSuite) TestPutObjectNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/The Go Programming Language.html"

	// Invalid endpoint
	client, err := New("http://ks3-cn-taikang.ksyuncs.com", accessID, accessKey)
	c.Assert(err, IsNil)

	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	err = bucket.PutObjectFromFile(objectName, localFile, Progress(&Ks3ProgressListener{}))
	testLogger.Println(err)
	c.Assert(err, NotNil)

	testLogger.Println("Ks3ProgressSuite.TestPutObjectNegative")
}

// TestAppendObject
func (s *Ks3ProgressSuite) TestAppendObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := RandStr(100)
	var val = []byte(objectValue)
	var nextPos int64
	var midPos = 1 + rand.Intn(len(val)-1)

	// AppendObject
	progressListener := Ks3ProgressListener{}
	nextPos, err := s.bucket.AppendObject(objectName, bytes.NewReader(val[0:midPos]), nextPos, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, nextPos)

	// DoAppendObject
	request := &AppendObjectRequest{
		ObjectKey: objectName,
		Reader:    bytes.NewReader(val[midPos:]),
		Position:  nextPos,
	}
	options := []Option{Progress(&Ks3ProgressListener{})}
	_, err = s.bucket.DoAppendObject(request, options)
	c.Assert(err, IsNil)

	testLogger.Println("Ks3ProgressSuite.TestAppendObject")
}

// TestMultipartUpload
func (s *Ks3ProgressSuite) TestMultipartUpload(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	fileInfo, err := os.Stat(fileName)
	c.Assert(err, IsNil)

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	fd, err := os.Open(fileName)
	c.Assert(err, IsNil)
	defer fd.Close()

	// Initiate
	progressListener := Ks3ProgressListener{}
	imur, err := s.bucket.InitiateMultipartUpload(objectName)
	c.Assert(err, IsNil)

	// UploadPart
	var parts []UploadPart
	for _, chunk := range chunks {
		fd.Seek(chunk.Offset, os.SEEK_SET)
		part, err := s.bucket.UploadPart(imur, fd, chunk.Size, chunk.Number, Progress(&progressListener))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	// Complete
	_, err = s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	testLogger.Println("Ks3ProgressSuite.TestMultipartUpload")
}

// TestMultipartUploadFromFile
func (s *Ks3ProgressSuite) TestMultipartUploadFromFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"
	fileInfo, err := os.Stat(fileName)
	c.Assert(err, IsNil)

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)

	// Initiate
	imur, err := s.bucket.InitiateMultipartUpload(objectName)
	c.Assert(err, IsNil)

	// UploadPart
	progressListener := Ks3ProgressListener{}
	var parts []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imur, fileName, chunk.Offset, chunk.Size, chunk.Number, Progress(&progressListener))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	// Complete
	_, err = s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	testLogger.Println("Ks3ProgressSuite.TestMultipartUploadFromFile")
}

// TestGetObject
func (s *Ks3ProgressSuite) TestGetObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := "newpic-progress-1.jpg"

	fileInfo, err := os.Stat(localFile)
	c.Assert(err, IsNil)

	progressListener := Ks3ProgressListener{}
	// PutObject
	err = s.bucket.PutObjectFromFile(objectName, localFile, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// GetObject
	progressListener.TotalRwBytes = 0
	body, err := s.bucket.GetObject(objectName, Progress(&progressListener))
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(body)
	c.Assert(err, IsNil)
	body.Close()
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// GetObjectToFile
	progressListener.TotalRwBytes = 0
	err = s.bucket.GetObjectToFile(objectName, newFile, Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// DoGetObject
	progressListener.TotalRwBytes = 0
	request := &GetObjectRequest{objectName}
	options := []Option{Progress(&progressListener)}
	result, err := s.bucket.DoGetObject(request, options)
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(result.Response.Body)
	c.Assert(err, IsNil)
	result.Response.Body.Close()
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	// GetObject with range
	progressListener.TotalRwBytes = 0
	body, err = s.bucket.GetObject(objectName, Range(1024, 4*1024), Progress(&progressListener))
	c.Assert(err, IsNil)
	text, err := ioutil.ReadAll(body)
	c.Assert(err, IsNil)
	body.Close()
	c.Assert(progressListener.TotalRwBytes, Equals, int64(len(text)))

	// PutObject size is 0
	progressListener.TotalRwBytes = 0
	err = s.bucket.PutObject(objectName, strings.NewReader(""), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, int64(0))

	// GetObject size is 0
	progressListener.TotalRwBytes = 0
	body, err = s.bucket.GetObject(objectName, Progress(&progressListener))
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(body)
	c.Assert(err, IsNil)
	body.Close()
	c.Assert(progressListener.TotalRwBytes, Equals, int64(0))

	testLogger.Println("Ks3ProgressSuite.TestGetObject")
}

// TestGetObjectNegative
func (s *Ks3ProgressSuite) TestGetObjectNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"

	// PutObject
	err := s.bucket.PutObjectFromFile(objectName, localFile)
	c.Assert(err, IsNil)

	// GetObject
	body, err := s.bucket.GetObject(objectName, Progress(&Ks3ProgressListener{}))
	c.Assert(err, IsNil)

	buf := make([]byte, 4*1024)
	n, err := body.Read(buf)
	c.Assert(err, IsNil)

	//time.Sleep(70 * time.Second) TODO

	// Read should fail
	for err == nil {
		n, err = body.Read(buf)
		n += n
	}
	c.Assert(err, NotNil)
	body.Close()

	testLogger.Println("Ks3ProgressSuite.TestGetObjectNegative")
}

// TestUploadFile
func (s *Ks3ProgressSuite) TestUploadFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"

	fileInfo, err := os.Stat(fileName)
	c.Assert(err, IsNil)

	progressListener := Ks3ProgressListener{}
	err = s.bucket.UploadFile(objectName, fileName, 100*1024, Routines(5), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	progressListener.TotalRwBytes = 0
	err = s.bucket.UploadFile(objectName, fileName, 100*1024, Routines(3), Checkpoint(true, objectName+".cp"), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	testLogger.Println("Ks3ProgressSuite.TestUploadFile")
}

// TestDownloadFile
func (s *Ks3ProgressSuite) TestDownloadFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := "down-new-file-progress-2.jpg"

	fileInfo, err := os.Stat(fileName)
	c.Assert(err, IsNil)

	// Upload
	err = s.bucket.UploadFile(objectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)

	progressListener := Ks3ProgressListener{}
	err = s.bucket.DownloadFile(objectName, newFile, 100*1024, Routines(5), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	progressListener.TotalRwBytes = 0
	err = s.bucket.DownloadFile(objectName, newFile, 1024*1024, Routines(3), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	progressListener.TotalRwBytes = 0
	err = s.bucket.DownloadFile(objectName, newFile, 50*1024, Routines(3), Checkpoint(true, ""), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	testLogger.Println("Ks3ProgressSuite.TestDownloadFile")
}

// TestCopyFile
func (s *Ks3ProgressSuite) TestCopyFile(c *C) {
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-copy"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"

	fileInfo, err := os.Stat(fileName)
	c.Assert(err, IsNil)

	// Upload
	progressListener := Ks3ProgressListener{}
	err = s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	progressListener.TotalRwBytes = 0
	err = s.bucket.CopyFile(bucketName, srcObjectName, destObjectName, 100*1024, Routines(5), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	progressListener.TotalRwBytes = 0
	err = s.bucket.CopyFile(bucketName, srcObjectName, destObjectName, 1024*100, Routines(3), Checkpoint(true, ""), Progress(&progressListener))
	c.Assert(err, IsNil)
	c.Assert(progressListener.TotalRwBytes, Equals, fileInfo.Size())

	testLogger.Println("Ks3ProgressSuite.TestCopyFile")
}
