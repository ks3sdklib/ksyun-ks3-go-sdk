// multipart test

package ks3

import (
	"math/rand"
	"net/http"
	"os"
	"strconv"

	. "gopkg.in/check.v1"
)

type Ks3BucketMultipartSuite struct {
	client *Client
	bucket *Bucket
}

var _ = Suite(&Ks3BucketMultipartSuite{})

// SetUpSuite runs once when the suite starts running
func (s *Ks3BucketMultipartSuite) SetUpSuite(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	s.client = client

	bucketName := bucketNamePrefix + RandLowStr(6)
	s.client.CreateBucket(bucketName)

	bucket, err := s.client.Bucket(bucketName)
	c.Assert(err, IsNil)
	s.bucket = bucket

	// Delete part
	keyMarker := KeyMarker("")
	uploadIDMarker := UploadIDMarker("")
	for {
		lmur, err := s.bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
		c.Assert(err, IsNil)
		for _, upload := range lmur.Uploads {
			var imur = InitiateMultipartUploadResult{Bucket: s.bucket.BucketName,
				Key: upload.Key, UploadID: upload.UploadID}
			err = s.bucket.AbortMultipartUpload(imur)
			c.Assert(err, IsNil)
		}
		keyMarker = KeyMarker(lmur.NextKeyMarker)
		uploadIDMarker = UploadIDMarker(lmur.NextUploadIDMarker)
		if !lmur.IsTruncated {
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

	testLogger.Println("test multipart started")
}

// TearDownSuite runs before each test or benchmark starts running
func (s *Ks3BucketMultipartSuite) TearDownSuite(c *C) {
	// Delete part
	keyMarker := KeyMarker("")
	uploadIDMarker := UploadIDMarker("")
	for {
		lmur, err := s.bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
		c.Assert(err, IsNil)
		for _, upload := range lmur.Uploads {
			var imur = InitiateMultipartUploadResult{Bucket: s.bucket.BucketName,
				Key: upload.Key, UploadID: upload.UploadID}
			err = s.bucket.AbortMultipartUpload(imur)
			c.Assert(err, IsNil)
		}
		keyMarker = KeyMarker(lmur.NextKeyMarker)
		uploadIDMarker = UploadIDMarker(lmur.NextUploadIDMarker)
		if !lmur.IsTruncated {
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

	testLogger.Println("test multipart completed")
}

// SetUpTest runs after each test or benchmark runs
func (s *Ks3BucketMultipartSuite) SetUpTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)
}

// TearDownTest runs once after all tests or benchmarks have finished running
func (s *Ks3BucketMultipartSuite) TearDownTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".temp")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt1")
	c.Assert(err, IsNil)

	err = removeTempFiles("../ks3", ".txt2")
	c.Assert(err, IsNil)
}

// TestMultipartUpload
func (s *Ks3BucketMultipartSuite) TestMultipartUpload(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	options := []Option{
		Expires(futureDate), Meta("my", "myprop"),
	}

	fd, err := os.Open(fileName)
	c.Assert(err, IsNil)
	defer fd.Close()

	imur, err := s.bucket.InitiateMultipartUpload(objectName, options...)
	c.Assert(err, IsNil)
	var parts []UploadPart
	for _, chunk := range chunks {
		fd.Seek(chunk.Offset, os.SEEK_SET)
		part, err := s.bucket.UploadPart(imur, fd, chunk.Size, chunk.Number)
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Multipart")

	err = s.bucket.GetObjectToFile(objectName, "newpic1.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestMultipartUploadFromFile
func (s *Ks3BucketMultipartSuite) TestMultipartUploadFromFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	options := []Option{
		Expires(futureDate), Meta("my", "myprop"),
	}
	imur, err := s.bucket.InitiateMultipartUpload(objectName, options...)
	c.Assert(err, IsNil)
	var parts []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imur, fileName, chunk.Offset, chunk.Size, chunk.Number)
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Multipart")

	err = s.bucket.GetObjectToFile(objectName, "newpic1.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestUploadPartCopy
func (s *Ks3BucketMultipartSuite) TestUploadPartCopy(c *C) {
	objectSrc := objectNamePrefix + RandStr(8) + "-src"
	objectDest := objectNamePrefix + RandStr(8) + "-dest"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	err = s.bucket.PutObjectFromFile(objectSrc, fileName)
	c.Assert(err, IsNil)

	options := []Option{
		Expires(futureDate), Meta("my", "myprop"),
	}
	imur, err := s.bucket.InitiateMultipartUpload(objectDest, options...)
	c.Assert(err, IsNil)
	var parts []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	meta, err := s.bucket.GetObjectDetailedMeta(objectDest)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("Expires"), Equals, futureDate.Format(http.TimeFormat))
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Multipart")

	err = s.bucket.GetObjectToFile(objectDest, "newpic2.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectSrc)
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectDest)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketMultipartSuite) TestListUploadedParts(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectSrc := objectName + "-src"
	objectDest := objectName + "-dest"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartSize(fileName, 100*1024)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	err = s.bucket.PutObjectFromFile(objectSrc, fileName)
	c.Assert(err, IsNil)

	// Upload
	imurUpload, err := s.bucket.InitiateMultipartUpload(objectName)
	var partsUpload []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imurUpload, fileName, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		partsUpload = append(partsUpload, part)
	}

	// Copy
	imurCopy, err := s.bucket.InitiateMultipartUpload(objectDest)
	var partsCopy []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartCopy(imurCopy, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		partsCopy = append(partsCopy, part)
	}

	// List
	lupr, err := s.bucket.ListUploadedParts(imurUpload)
	c.Assert(err, IsNil)
	testLogger.Println("lupr:", lupr)
	c.Assert(len(lupr.UploadedParts), Equals, len(chunks))

	lupr, err = s.bucket.ListUploadedParts(imurCopy)
	c.Assert(err, IsNil)
	testLogger.Println("lupr:", lupr)
	c.Assert(len(lupr.UploadedParts), Equals, len(chunks))

	lmur, err := s.bucket.ListMultipartUploads()
	c.Assert(err, IsNil)
	testLogger.Println("lmur:", lmur)

	// Complete
	_, err = s.bucket.CompleteMultipartUpload(imurUpload, partsUpload)
	c.Assert(err, IsNil)
	_, err = s.bucket.CompleteMultipartUpload(imurCopy, partsCopy)
	c.Assert(err, IsNil)

	// Download
	err = s.bucket.GetObjectToFile(objectDest, "newpic3.jpg")
	c.Assert(err, IsNil)
	err = s.bucket.GetObjectToFile(objectName, "newpic4.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectDest)
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectSrc)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketMultipartSuite) TestAbortMultipartUpload(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectSrc := objectName + "-src"
	objectDest := objectName + "-dest"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartSize(fileName, 100*1024)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	err = s.bucket.PutObjectFromFile(objectSrc, fileName)
	c.Assert(err, IsNil)

	// Upload
	imurUpload, err := s.bucket.InitiateMultipartUpload(objectName)
	var partsUpload []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imurUpload, fileName, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		partsUpload = append(partsUpload, part)
	}

	// Copy
	imurCopy, err := s.bucket.InitiateMultipartUpload(objectDest)
	var partsCopy []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartCopy(imurCopy, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		partsCopy = append(partsCopy, part)
	}

	// List
	lupr, err := s.bucket.ListUploadedParts(imurUpload)
	c.Assert(err, IsNil)
	testLogger.Println("lupr:", lupr)
	c.Assert(len(lupr.UploadedParts), Equals, len(chunks))

	lupr, err = s.bucket.ListUploadedParts(imurCopy)
	c.Assert(err, IsNil)
	testLogger.Println("lupr:", lupr)
	c.Assert(len(lupr.UploadedParts), Equals, len(chunks))

	lmur, err := s.bucket.ListMultipartUploads()
	c.Assert(err, IsNil)
	testLogger.Println("lmur:", lmur)
	c.Assert(len(lmur.Uploads), Equals, 2)

	// Abort
	err = s.bucket.AbortMultipartUpload(imurUpload)
	c.Assert(err, IsNil)
	err = s.bucket.AbortMultipartUpload(imurCopy)
	c.Assert(err, IsNil)

	lmur, err = s.bucket.ListMultipartUploads()
	c.Assert(err, IsNil)
	testLogger.Println("lmur:", lmur)
	c.Assert(len(lmur.Uploads), Equals, 0)

	// Download
	err = s.bucket.GetObjectToFile(objectDest, "newpic3.jpg")
	c.Assert(err, NotNil)
	err = s.bucket.GetObjectToFile(objectName, "newpic4.jpg")
	c.Assert(err, NotNil)
}

// TestUploadPartCopyWithConstraints
func (s *Ks3BucketMultipartSuite) TestUploadPartCopyWithConstraints(c *C) {
	objectSrc := objectNamePrefix + RandStr(8) + "-src"
	objectDest := objectNamePrefix + RandStr(8) + "-dest"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartNum(fileName, 3)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	err = s.bucket.PutObjectFromFile(objectSrc, fileName)
	c.Assert(err, IsNil)

	imur, err := s.bucket.InitiateMultipartUpload(objectDest)
	var parts []UploadPart
	for _, chunk := range chunks {
		_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number),
			CopySourceIfModifiedSince(futureDate))
		c.Assert(err, NotNil)
	}

	for _, chunk := range chunks {
		_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number),
			CopySourceIfUnmodifiedSince(futureDate))
		c.Assert(err, IsNil)
	}

	meta, err := s.bucket.GetObjectDetailedMeta(objectSrc)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)

	for _, chunk := range chunks {
		_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number),
			CopySourceIfNoneMatch(meta.Get("Etag")))
		c.Assert(err, NotNil)
	}

	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number),
			CopySourceIfMatch(meta.Get("Etag")))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	err = s.bucket.GetObjectToFile(objectDest, "newpic5.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectSrc)
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectDest)
	c.Assert(err, IsNil)
}

// TestMultipartUploadFromFileOutofOrder
func (s *Ks3BucketMultipartSuite) TestMultipartUploadFromFileOutofOrder(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartSize(fileName, 1024*100)
	shuffleArray(chunks)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	imur, err := s.bucket.InitiateMultipartUpload(objectName)
	var parts []UploadPart
	for _, chunk := range chunks {
		_, err := s.bucket.UploadPartFromFile(imur, fileName, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
	}
	// Double upload
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imur, fileName, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	err = s.bucket.GetObjectToFile(objectName, "newpic6.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestUploadPartCopyOutofOrder
func (s *Ks3BucketMultipartSuite) TestUploadPartCopyOutofOrder(c *C) {
	objectSrc := objectNamePrefix + RandStr(8) + "-src"
	objectDest := objectNamePrefix + RandStr(8) + "-dest"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartSize(fileName, 1024*100)
	shuffleArray(chunks)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	err = s.bucket.PutObjectFromFile(objectSrc, fileName)
	c.Assert(err, IsNil)

	imur, err := s.bucket.InitiateMultipartUpload(objectDest)
	var parts []UploadPart
	for _, chunk := range chunks {
		_, err := s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
	}
	// Double copy
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartCopy(imur, s.bucket.BucketName, objectSrc, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	err = s.bucket.GetObjectToFile(objectDest, "newpic7.jpg")
	c.Assert(err, IsNil)

	err = s.bucket.DeleteObject(objectSrc)
	c.Assert(err, IsNil)
	err = s.bucket.DeleteObject(objectDest)
	c.Assert(err, IsNil)
}

// TestMultipartUploadFromFileType
func (s *Ks3BucketMultipartSuite) TestMultipartUploadFromFileType(c *C) {
	objectName := objectNamePrefix + RandStr(8) + ".jpg"
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	chunks, err := SplitFileByPartNum(fileName, 4)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	imur, err := s.bucket.InitiateMultipartUpload(objectName)
	var parts []UploadPart
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imur, fileName, chunk.Offset, chunk.Size, chunk.Number)
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}

	testLogger.Println("parts:", parts)
	cmur, err := s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)
	testLogger.Println("cmur:", cmur)

	err = s.bucket.GetObjectToFile(objectName, "newpic8.jpg")
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Content-Type"), Equals, "image/jpeg")

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketMultipartSuite) TestListMultipartUploads(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	imurs := []InitiateMultipartUploadResult{}
	for i := 0; i < 20; i++ {
		imur, err := s.bucket.InitiateMultipartUpload(objectName + strconv.Itoa(i))
		c.Assert(err, IsNil)
		imurs = append(imurs, imur)
	}

	lmpu, err := s.bucket.ListMultipartUploads()
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 20)

	lmpu, err = s.bucket.ListMultipartUploads(MaxUploads(3))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 3)

	lmpu, err = s.bucket.ListMultipartUploads(Prefix(objectName))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 20)

	lmpu, err = s.bucket.ListMultipartUploads(Prefix(objectName + "1"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 11)

	lmpu, err = s.bucket.ListMultipartUploads(Prefix(objectName + "22"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 0)

	lmpu, err = s.bucket.ListMultipartUploads(KeyMarker(objectName + "10"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 18)

	lmpu, err = s.bucket.ListMultipartUploads(KeyMarker(objectName+"10"), MaxUploads(3))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 3)

	lmpu, err = s.bucket.ListMultipartUploads(Prefix(objectName), Delimiter("4"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 18)
	c.Assert(len(lmpu.CommonPrefixes), Equals, 2)

	upLoadIDStr := RandStr(3)
	lmpu, err = s.bucket.ListMultipartUploads(KeyMarker(objectName+"12"), UploadIDMarker(upLoadIDStr))
	c.Assert(err, IsNil)
	checkNum := 16
	for _, im := range imurs {
		if im.Key == objectName+"12" && im.UploadID > upLoadIDStr {
			checkNum = 17
			break
		}
	}
	c.Assert(len(lmpu.Uploads), Equals, checkNum)

	for _, imur := range imurs {
		err = s.bucket.AbortMultipartUpload(imur)
		c.Assert(err, IsNil)
	}
}

func (s *Ks3BucketMultipartSuite) TestListMultipartUploadsEncodingKey(c *C) {
	prefix := objectNamePrefix + "让你任性让你狂" + RandStr(8)

	imurs := []InitiateMultipartUploadResult{}
	for i := 0; i < 3; i++ {
		imur, err := s.bucket.InitiateMultipartUpload(prefix + strconv.Itoa(i))
		c.Assert(err, IsNil)
		imurs = append(imurs, imur)
	}

	lmpu, err := s.bucket.ListMultipartUploads()
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 3)

	lmpu, err = s.bucket.ListMultipartUploads(Prefix(prefix + "1"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 1)

	lmpu, err = s.bucket.ListMultipartUploads(KeyMarker(prefix + "1"))
	c.Assert(err, IsNil)
	c.Assert(len(lmpu.Uploads), Equals, 1)

	lmpu, err = s.bucket.ListMultipartUploads(EncodingType("url"))
	c.Assert(err, IsNil)
	for i, upload := range lmpu.Uploads {
		c.Assert(upload.Key, Equals, prefix+strconv.Itoa(i))
	}

	for _, imur := range imurs {
		err = s.bucket.AbortMultipartUpload(imur)
		c.Assert(err, IsNil)
	}
}

func (s *Ks3BucketMultipartSuite) TestMultipartNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)

	// Key tool long
	data := make([]byte, 100*1024)
	imur, err := s.bucket.InitiateMultipartUpload(string(data))
	c.Assert(err, NotNil)

	// Invalid imur
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	fd, err := os.Open(fileName)
	c.Assert(err, IsNil)
	defer fd.Close()

	_, err = s.bucket.UploadPart(imur, fd, 1024, 1)
	c.Assert(err, NotNil)

	_, err = s.bucket.UploadPartFromFile(imur, fileName, 0, 1024, 1)
	c.Assert(err, NotNil)

	_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, fileName, 0, 1024, 1)
	c.Assert(err, NotNil)

	err = s.bucket.AbortMultipartUpload(imur)
	c.Assert(err, NotNil)

	_, err = s.bucket.ListUploadedParts(imur)
	c.Assert(err, NotNil)

	// Invalid exist
	imur, err = s.bucket.InitiateMultipartUpload(objectName)
	c.Assert(err, IsNil)

	_, err = s.bucket.UploadPart(imur, fd, 1024, 1)
	c.Assert(err, IsNil)

	_, err = s.bucket.UploadPart(imur, fd, 102400, 10001)
	c.Assert(err, NotNil)

	//    _, err = s.bucket.UploadPartFromFile(imur, fileName, 0, 1024, 1)
	//    c.Assert(err, IsNil)

	_, err = s.bucket.UploadPartFromFile(imur, fileName, 0, 102400, 10001)
	c.Assert(err, NotNil)

	_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, fileName, 0, 1024, 1)
	c.Assert(err, NotNil)

	_, err = s.bucket.UploadPartCopy(imur, s.bucket.BucketName, fileName, 0, 1024, 1000)
	c.Assert(err, NotNil)

	err = s.bucket.AbortMultipartUpload(imur)
	c.Assert(err, IsNil)

	// Invalid option
	_, err = s.bucket.InitiateMultipartUpload(objectName, IfModifiedSince(futureDate))
	c.Assert(err, IsNil)
}

func (s *Ks3BucketMultipartSuite) TestMultipartUploadFromFileBigFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	bigFile := "D:\\tmp\\bigfile.zip"
	newFile := "D:\\tmp\\newbigfile.zip"

	exist, err := isFileExist(bigFile)
	c.Assert(err, IsNil)
	if !exist {
		return
	}

	chunks, err := SplitFileByPartNum(bigFile, 64)
	c.Assert(err, IsNil)
	testLogger.Println("chunks:", chunks)

	imur, err := s.bucket.InitiateMultipartUpload(objectName)
	var parts []UploadPart
	start := GetNowSec()
	for _, chunk := range chunks {
		part, err := s.bucket.UploadPartFromFile(imur, bigFile, chunk.Offset, chunk.Size, (int)(chunk.Number))
		c.Assert(err, IsNil)
		parts = append(parts, part)
	}
	end := GetNowSec()
	testLogger.Println("Uplaod big file:", bigFile, "use sec:", end-start)

	testLogger.Println("parts:", parts)
	_, err = s.bucket.CompleteMultipartUpload(imur, parts)
	c.Assert(err, IsNil)

	start = GetNowSec()
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	end = GetNowSec()
	testLogger.Println("Download big file:", bigFile, "use sec:", end-start)

	start = GetNowSec()
	eq, err := compareFiles(bigFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)
	end = GetNowSec()
	testLogger.Println("Compare big file:", bigFile, "use sec:", end-start)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestUploadFile
func (s *Ks3BucketMultipartSuite) TestUploadFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	// Upload with 100K part size
	err := s.bucket.UploadFile(objectName, fileName, 100*1024)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err := compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Upload with part size equals to 1/4 of the file size
	err = s.bucket.UploadFile(objectName, fileName, 482048/4)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Upload with part size equals to the file size
	err = s.bucket.UploadFile(objectName, fileName, 482048)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Upload with part size is bigger than the file size
	err = s.bucket.UploadFile(objectName, fileName, 482049)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Option
	options := []Option{
		Expires(futureDate),
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval")}
	err = s.bucket.UploadFile(objectName, fileName, 482049, options...)
	c.Assert(err, IsNil)

	// Check
	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	acl, err := s.bucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectAcl:", acl)
	c.Assert(acl.GetCannedACL(), Equals, ACLPublicRead)

	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")
}

func (s *Ks3BucketMultipartSuite) TestUploadFileNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"

	// Smaller than the required minimal part size (100KB)
	err := s.bucket.UploadFile(objectName, fileName, 100*1024-1)
	c.Assert(err, NotNil)

	// Bigger than the max part size (5G)
	err = s.bucket.UploadFile(objectName, fileName, 1024*1024*1024*5+1)
	c.Assert(err, NotNil)

	// File does not exist
	err = s.bucket.UploadFile(objectName, "/root1/123abc9874", 1024*1024*1024)
	c.Assert(err, NotNil)

	// Invalid key , key is empty.
	err = s.bucket.UploadFile("", fileName, 100*1024)
	c.Assert(err, NotNil)
}

// TestDownloadFile
func (s *Ks3BucketMultipartSuite) TestDownloadFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	var fileName = "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	err := s.bucket.UploadFile(objectName, fileName, 100*1024)
	c.Assert(err, IsNil)

	// Download file with part size of 100K
	err = s.bucket.DownloadFile(objectName, newFile, 100*1024)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err := compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// Download the file with part size equals to 1/4 of the file size
	err = s.bucket.DownloadFile(objectName, newFile, 482048/4)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// Download the file with part size same as the file size
	err = s.bucket.DownloadFile(objectName, newFile, 482048)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// Download the file with part size bigger than the file size
	err = s.bucket.DownloadFile(objectName, newFile, 482049)
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// Option
	meta, err := s.bucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)

	// If-Match
	err = s.bucket.DownloadFile(objectName, newFile, 482048/4, IfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)

	os.Remove(newFile)
	err = s.bucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	// If-None-Match
	err = s.bucket.DownloadFile(objectName, newFile, 482048, IfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	os.Remove(newFile)
	err = s.bucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

func (s *Ks3BucketMultipartSuite) TestDownloadFileNegative(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	newFile := RandStr(8) + ".jpg"

	// Smaller than the required minimal part size (100KB)
	err := s.bucket.DownloadFile(objectName, newFile, 100*1024-1)
	c.Assert(err, NotNil)

	// Bigger than the required max part size (5G)
	err = s.bucket.DownloadFile(objectName, newFile, 1024*1024*1024+1)
	c.Assert(err, NotNil)

	// File does not exist
	err = s.bucket.DownloadFile(objectName, "/KS3/TEMP/ZIBI/QUQU/BALA", 1024*1024*1024+1)
	c.Assert(err, NotNil)

	// Key does not exist
	err = s.bucket.DownloadFile(objectName, newFile, 100*1024)
	c.Assert(err, NotNil)
}

// Private
func shuffleArray(chunks []FileChunk) []FileChunk {
	for i := range chunks {
		j := rand.Intn(i + 1)
		chunks[i], chunks[j] = chunks[j], chunks[i]
	}
	return chunks
}
