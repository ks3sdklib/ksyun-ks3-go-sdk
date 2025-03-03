package ks3

import (
	"fmt"
	"os"
	"time"

	. "gopkg.in/check.v1"
)

type Ks3CopySuite struct {
	client *Client
	bucket *Bucket
}

var _ = Suite(&Ks3CopySuite{})

// SetUpSuite runs once when the suite starts running
func (s *Ks3CopySuite) SetUpSuite(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	s.client = client

	bucketName := bucketNamePrefix + RandLowStr(6)
	s.client.CreateBucket(bucketName)

	bucket, err := s.client.Bucket(bucketName)
	c.Assert(err, IsNil)
	s.bucket = bucket

	testLogger.Println("test copy started")
}

// TearDownSuite runs before each test or benchmark starts running
func (s *Ks3CopySuite) TearDownSuite(c *C) {
	// Delete Part
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

	testLogger.Println("test copy completed")
}

// SetUpTest runs after each test or benchmark runs
func (s *Ks3CopySuite) SetUpTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)
}

// TearDownTest runs once after all tests or benchmarks have finished running
func (s *Ks3CopySuite) TearDownTest(c *C) {
	err := removeTempFiles("../ks3", ".jpg")
	c.Assert(err, IsNil)
}

// TestCopyRoutineWithoutRecovery is multi-routine copy without resumable recovery
func (s *Ks3CopySuite) TestCopyRoutineWithoutRecovery(c *C) {
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := "copy-new-file.jpg"

	// Upload source file
	err := s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Does not specify parameter 'routines', by default it's single routine
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024)
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err := compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Specify one routine.
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(1))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Specify three routines, which is less than parts count 5
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(3))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Specify 5 routines which is the same as parts count
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(5))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Specify routine count 10, which is more than parts count
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(10))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Invalid routine count, will use single routine
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(-1))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Option
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(3), Meta("myprop", "mypropval"))

	meta, err := s.bucket.GetObjectDetailedMeta(destObjectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	err = s.bucket.DeleteObject(srcObjectName)
	c.Assert(err, IsNil)
}

// CopyErrorHooker is a copypart request hook
func CopyErrorHooker(part copyPart) error {
	if part.Number == 5 {
		time.Sleep(time.Second)
		return fmt.Errorf("ErrorHooker")
	}
	return nil
}

// TestCopyRoutineWithoutRecoveryNegative is a multiple routines copy without checkpoint
func (s *Ks3CopySuite) TestCopyRoutineWithoutRecoveryNegative(c *C) {
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"

	// Upload source file
	err := s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)

	copyPartHooker = CopyErrorHooker
	// Worker routine errors
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 100*1024, Routines(2))

	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "ErrorHooker")
	copyPartHooker = defaultCopyPartHook

	// Source bucket does not exist
	err = s.bucket.CopyFile("notexist", srcObjectName, destObjectName, 100*1024, Routines(2))
	c.Assert(err, NotNil)

	// Target object does not exist
	err = s.bucket.CopyFile(s.bucket.BucketName, "notexist", destObjectName, 100*1024, Routines(2))

	// The part size is invalid
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024, Routines(2))
	c.Assert(err, NotNil)

	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*1024*1024*100, Routines(2))
	c.Assert(err, NotNil)

	// Delete the source file
	err = s.bucket.DeleteObject(srcObjectName)
	c.Assert(err, IsNil)
}

// TestCopyRoutineWithRecovery is a multiple routines copy with resumable recovery
func (s *Ks3CopySuite) TestCopyRoutineWithRecovery(c *C) {
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	// Upload source file
	err := s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Routines default value, CP's default path is destObjectName+.cp
	// Copy object with checkpoint enabled, single runtine.
	// Copy 4 parts---the CopyErrorHooker makes sure the copy of part 5 will fail.
	copyPartHooker = CopyErrorHooker
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "ErrorHooker")
	copyPartHooker = defaultCopyPartHook

	// Check CP
	ccp := copyCheckpoint{}
	err = ccp.load(destObjectName + ".cp")
	c.Assert(err, IsNil)
	c.Assert(ccp.Magic, Equals, copyCpMagic)
	c.Assert(len(ccp.MD5), Equals, len("LC34jZU5xK4hlxi3Qn3XGQ=="))
	c.Assert(ccp.SrcBucketName, Equals, s.bucket.BucketName)
	c.Assert(ccp.SrcObjectKey, Equals, srcObjectName)
	c.Assert(ccp.DestBucketName, Equals, s.bucket.BucketName)
	c.Assert(ccp.DestObjectKey, Equals, destObjectName)
	c.Assert(len(ccp.CopyID), Equals, len("3F79722737D1469980DACEDCA325BB52"))
	c.Assert(ccp.ObjStat.Size, Equals, int64(482048))
	c.Assert(len(ccp.ObjStat.LastModified), Equals, len("2015-12-17 18:43:03 +0800 CST"))
	c.Assert(ccp.ObjStat.Etag, Equals, "\"75ad421727e894a9a1599dec3351405e\"")
	c.Assert(len(ccp.Parts), Equals, 5)
	c.Assert(len(ccp.todoParts()), Equals, 1)
	c.Assert(ccp.PartStat[4], Equals, false)

	// Second copy, finish the last part
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err := compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	err = ccp.load(fileName + ".cp")
	c.Assert(err, NotNil)

	//multicopy with empty checkpoint path
	copyPartHooker = CopyErrorHooker
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Checkpoint(true, ""))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "ErrorHooker")
	copyPartHooker = defaultCopyPartHook
	ccp = copyCheckpoint{}
	err = ccp.load(destObjectName + ".cp")
	c.Assert(err, NotNil)

	//multi copy with checkpoint dir
	copyPartHooker = CopyErrorHooker
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(2), CheckpointDir(true, "./"))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "ErrorHooker")
	copyPartHooker = defaultCopyPartHook

	// Check CP
	ccp = copyCheckpoint{}
	cpConf := cpConfig{IsEnable: true, DirPath: "./"}
	cpFilePath := getCopyCpFilePath(&cpConf, s.bucket.BucketName, srcObjectName, s.bucket.BucketName, destObjectName, "")
	err = ccp.load(cpFilePath)
	c.Assert(err, IsNil)
	c.Assert(ccp.Magic, Equals, copyCpMagic)
	c.Assert(len(ccp.MD5), Equals, len("LC34jZU5xK4hlxi3Qn3XGQ=="))
	c.Assert(ccp.SrcBucketName, Equals, s.bucket.BucketName)
	c.Assert(ccp.SrcObjectKey, Equals, srcObjectName)
	c.Assert(ccp.DestBucketName, Equals, s.bucket.BucketName)
	c.Assert(ccp.DestObjectKey, Equals, destObjectName)
	c.Assert(len(ccp.CopyID), Equals, len("3F79722737D1469980DACEDCA325BB52"))
	c.Assert(ccp.ObjStat.Size, Equals, int64(482048))
	c.Assert(len(ccp.ObjStat.LastModified), Equals, len("2015-12-17 18:43:03 +0800 CST"))
	c.Assert(ccp.ObjStat.Etag, Equals, "\"75ad421727e894a9a1599dec3351405e\"")
	c.Assert(len(ccp.Parts), Equals, 5)
	c.Assert(len(ccp.todoParts()), Equals, 1)
	c.Assert(ccp.PartStat[4], Equals, false)

	// Second copy, finish the last part.
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(2), CheckpointDir(true, "./"))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	err = ccp.load(srcObjectName + ".cp")
	c.Assert(err, NotNil)

	// First copy without error.
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(3), Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Copy with multiple coroutines, no errors.
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(10), Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, IsNil)

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Option
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(5), Checkpoint(true, destObjectName+".cp"), Meta("myprop", "mypropval"))
	c.Assert(err, IsNil)

	meta, err := s.bucket.GetObjectDetailedMeta(destObjectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = s.bucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = s.bucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Delete the source file
	err = s.bucket.DeleteObject(srcObjectName)
	c.Assert(err, IsNil)
}

// TestCopyRoutineWithRecoveryNegative is a multiple routineed copy without checkpoint
func (s *Ks3CopySuite) TestCopyRoutineWithRecoveryNegative(c *C) {
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"

	// Source bucket does not exist
	err := s.bucket.CopyFile("notexist", srcObjectName, destObjectName, 100*1024, Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, NotNil)
	c.Assert(err, NotNil)

	// Source object does not exist
	err = s.bucket.CopyFile(s.bucket.BucketName, "notexist", destObjectName, 100*1024, Routines(2), Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, NotNil)

	// Specify part size is invalid.
	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024, Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, NotNil)

	err = s.bucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*1024*1024*100, Routines(2), Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, NotNil)
}

// TestCopyFileCrossBucket is a cross bucket's direct copy.
func (s *Ks3CopySuite) TestCopyFileCrossBucket(c *C) {
	destBucketName := s.bucket.BucketName + "-desc-" + RandLowStr(8)
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	destBucket, err := s.client.Bucket(destBucketName)
	c.Assert(err, IsNil)

	// Create a target bucket
	err = s.client.CreateBucket(destBucketName)

	// Upload source file
	err = s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Copy files
	err = destBucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(5), Checkpoint(true, destObjectName+".cp"))
	c.Assert(err, IsNil)

	err = destBucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err := compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = destBucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Copy file with options
	err = destBucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, Routines(10), Checkpoint(true, "copy.cp"), Meta("myprop", "mypropval"))
	c.Assert(err, IsNil)

	err = destBucket.GetObjectToFile(destObjectName, newFile)
	c.Assert(err, IsNil)

	eq, err = compareFiles(fileName, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	err = destBucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Delete target bucket
	ForceDeleteBucket(s.client, destBucketName, c)
}

// TestCopyFileChoiceOptions
func (s *Ks3CopySuite) TestCopyFileChoiceOptions(c *C) {
	destBucketName := s.bucket.BucketName + "-desc-" + RandLowStr(8)
	srcObjectName := objectNamePrefix + RandStr(8)
	destObjectName := srcObjectName + "-dest"
	fileName := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	destBucket, err := s.client.Bucket(destBucketName)
	c.Assert(err, IsNil)

	// Create a target bucket
	err = s.client.CreateBucket(destBucketName)

	// Upload source file
	err = s.bucket.UploadFile(srcObjectName, fileName, 100*1024, Routines(3))
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// copyfile with properties
	options := []Option{
		ObjectACL(ACLPublicRead),
		RequestPayer(Requester),
		TrafficLimitHeader(1024 * 1024 * 8),
		ServerSideEncryption("AES256"),
		Routines(5), // without checkpoint
	}

	// Copy files
	err = destBucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, options...)
	c.Assert(err, IsNil)

	// check object
	meta, err := destBucket.GetObjectDetailedMeta(destObjectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Server-Side-Encryption"), Equals, "AES256")

	aclResult, err := destBucket.GetObjectACL(destObjectName)
	c.Assert(aclResult.GetCannedACL(), Equals, ACLPublicRead)
	c.Assert(err, IsNil)

	err = destBucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Copy file with options
	options = []Option{
		ObjectACL(ACLPublicRead),
		RequestPayer(Requester),
		TrafficLimitHeader(1024 * 1024 * 8),
		ServerSideEncryption("AES256"),
		Routines(10),
		Checkpoint(true, "copy.cp"), // with checkpoint
	}

	err = destBucket.CopyFile(s.bucket.BucketName, srcObjectName, destObjectName, 1024*100, options...)
	c.Assert(err, IsNil)

	// check object
	meta, err = destBucket.GetObjectDetailedMeta(destObjectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Server-Side-Encryption"), Equals, "AES256")

	aclResult, err = destBucket.GetObjectACL(destObjectName)
	c.Assert(aclResult.GetCannedACL(), Equals, ACLPublicRead)
	c.Assert(err, IsNil)

	err = destBucket.DeleteObject(destObjectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)

	// Delete target bucket
	err = s.client.DeleteBucket(destBucketName)
	c.Assert(err, IsNil)
}
