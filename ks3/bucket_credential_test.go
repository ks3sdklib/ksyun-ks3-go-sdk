// Credentials test
package ks3

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"strings"

	. "gopkg.in/check.v1"
)

type Ks3CredentialBucketSuite struct {
	client    *Client
	creClient *Client
	bucket    *Bucket
	creBucket *Bucket
}

var _ = Suite(&Ks3CredentialBucketSuite{})

func (cs *Ks3CredentialBucketSuite) credentialSubUser(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	err = client.CreateBucket(credentialBucketName)
	c.Assert(err, IsNil)
	cs.client = client
	policyInfo := `
	{
		"Version":"1",
		"Statement":[
			{
				"Action":[
					"ks3:*"
				],
				"Effect":"Allow",
				"Principal":["` + credentialUID + `"],
				"Resource":["acs:ks3:*:*:` + credentialBucketName + `", "acs:ks3:*:*:` + credentialBucketName + `/*"]
			}
		]
	}`

	err = client.SetBucketPolicy(credentialBucketName, policyInfo)
	c.Assert(err, IsNil)

	bucket, err := cs.client.Bucket(credentialBucketName)
	c.Assert(err, IsNil)
	cs.bucket = bucket
}

// SetUpSuite runs once when the suite starts running.
func (cs *Ks3CredentialBucketSuite) SetUpSuite(c *C) {
	if credentialUID == "" {
		testLogger.Println("the cerdential UID is NULL, skip the credential test")
		c.Skip("the credential Uid is null")
	}

	cs.credentialSubUser(c)
	client, err := New(endpoint, credentialAccessID, credentialAccessKey)
	c.Assert(err, IsNil)
	cs.creClient = client

	bucket, err := cs.creClient.Bucket(credentialBucketName)
	c.Assert(err, IsNil)
	cs.creBucket = bucket

	testLogger.Println("test credetial bucket started")
}

func (cs *Ks3CredentialBucketSuite) TearDownSuite(c *C) {
	if credentialUID == "" {
		c.Skip("the credential Uid is null")
	}
	for _, bucket := range []*Bucket{cs.bucket} {
		// Delete multipart
		keyMarker := KeyMarker("")
		uploadIDMarker := UploadIDMarker("")
		for {
			lmu, err := bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
			c.Assert(err, IsNil)
			for _, upload := range lmu.Uploads {
				imur := InitiateMultipartUploadResult{Bucket: credentialBucketName, Key: upload.Key, UploadID: upload.UploadID}
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
	}
	err := cs.client.DeleteBucket(credentialBucketName)
	c.Assert(err, IsNil)
	testLogger.Println("test credential bucket completed")
}

// TestPutObjectFromFile
func (cs *Ks3CredentialBucketSuite) TestPutObjectFromFile(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	localFile := "../sample/BingWallpaper-2015-11-07.jpg"
	newFile := RandStr(8) + ".jpg"

	// Put
	err := cs.creBucket.PutObjectFromFile(objectName, localFile)
	c.Assert(err, IsNil)

	// Check
	err = cs.creBucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err := compareFiles(localFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	meta, err := cs.creBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("Content-Type"), Equals, "image/jpeg")

	acl, err := cs.creBucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("aclRes:", acl)
	c.Assert(acl.ACL, Equals, "default")

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Put with properties
	options := []Option{
		Expires(futureDate),
		ObjectACL(ACLPublicRead),
		Meta("myprop", "mypropval"),
	}
	err = cs.creBucket.PutObjectFromFile(objectName, localFile, options...)
	c.Assert(err, IsNil)

	// Check
	err = cs.creBucket.GetObjectToFile(objectName, newFile)
	c.Assert(err, IsNil)
	eq, err = compareFiles(localFile, newFile)
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, true)

	acl, err = cs.creBucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectACL:", acl)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	meta, err = cs.creBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)
	c.Assert(meta.Get("X-Kss-Meta-Myprop"), Equals, "mypropval")

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
	os.Remove(newFile)
}

// TestCopyObject
func (cs *Ks3CredentialBucketSuite) TestCopyObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := RandStr(18)

	err := cs.creBucket.PutObject(objectName, strings.NewReader(objectValue),
		ACL(ACLPublicRead), Meta("my", "myprop"))
	c.Assert(err, IsNil)

	// Copy
	var objectNameDest = objectName + "dest"
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest)
	c.Assert(err, IsNil)

	// Check
	lor, err := cs.creBucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	testLogger.Println("objects:", lor.Objects)
	c.Assert(len(lor.Objects), Equals, 2)

	body, err := cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-kss-copy-source-if-modified-since
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, CopySourceIfModifiedSince(futureDate))
	c.Assert(err, NotNil)
	testLogger.Println("CopyObject:", err)

	// Copy with constraints x-kss-copy-source-if-unmodified-since
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, CopySourceIfUnmodifiedSince(futureDate))
	c.Assert(err, IsNil)

	// Check
	lor, err = cs.creBucket.ListObjects(Prefix(objectName))
	c.Assert(err, IsNil)
	testLogger.Println("objects:", lor.Objects)
	c.Assert(len(lor.Objects), Equals, 2)

	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-kss-copy-source-if-match
	meta, err := cs.creBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta)

	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, CopySourceIfMatch(meta.Get("Etag")))
	c.Assert(err, IsNil)

	// Check
	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-kss-copy-source-if-none-match
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, CopySourceIfNoneMatch(meta.Get("Etag")))
	c.Assert(err, NotNil)

	// Copy with constraints x-kss-metadata-directive
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, Meta("my", "mydestprop"),
		MetadataDirective(MetaCopy))
	c.Assert(err, IsNil)

	// Check
	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	destMeta, err := cs.creBucket.GetObjectDetailedMeta(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")

	acl, err := cs.creBucket.GetObjectACL(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, "default")

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy with constraints x-kss-metadata-directive and self defined dest object meta
	options := []Option{
		ObjectACL(ACLPublicReadWrite),
		Meta("my", "mydestprop"),
		MetadataDirective(MetaReplace),
	}
	_, err = cs.creBucket.CopyObject(objectName, objectNameDest, options...)
	c.Assert(err, IsNil)

	// Check
	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	destMeta, err = cs.creBucket.GetObjectDetailedMeta(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(destMeta.Get("X-Kss-Meta-My"), Equals, "mydestprop")

	acl, err = cs.creBucket.GetObjectACL(objectNameDest)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicReadWrite))

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}

// TestCopyObjectToOrFrom
func (cs *Ks3CredentialBucketSuite) TestCopyObjectToOrFrom(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue := RandStr(18)
	sorBucketName := credentialBucketName + "-sor"
	objectNameDest := objectName + "-Dest"

	err := cs.client.CreateBucket(sorBucketName)
	c.Assert(err, IsNil)
	// Set ACL_PUBLIC_R
	err = cs.client.SetBucketACL(sorBucketName, ACLPublicRead)
	c.Assert(err, IsNil)

	sorBucket, err := cs.client.Bucket(sorBucketName)
	c.Assert(err, IsNil)

	err = sorBucket.PutObject(objectName, strings.NewReader(objectValue))
	c.Assert(err, IsNil)

	// Copy from
	_, err = cs.creBucket.CopyObjectFrom(sorBucketName, objectName, objectNameDest)
	c.Assert(err, IsNil)

	// Check
	body, err := cs.creBucket.GetObject(objectNameDest)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectNameDest)
	c.Assert(err, IsNil)

	// Copy to
	_, err = sorBucket.CopyObjectTo(credentialBucketName, objectName, objectName)
	c.Assert(err, IsNil)

	// Check
	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	// Clean
	err = sorBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	err = cs.client.DeleteBucket(sorBucketName)
	c.Assert(err, IsNil)
}

// TestAppendObject
func (cs *Ks3CredentialBucketSuite) TestAppendObject(c *C) {
	objectName := objectNamePrefix + RandStr(8)
	objectValue1 := RandStr(18)
	objectValue2 := RandStr(18)
	objectValue := objectValue1 + objectValue2
	var val = []byte(objectValue)
	var localFile = RandStr(8) + ".txt"
	var nextPos int64
	var midPos = 1 + rand.Intn(len(val)-1)

	var err = CreateFileAndWrite(localFile+"1", val[0:midPos])
	c.Assert(err, IsNil)
	err = CreateFileAndWrite(localFile+"2", val[midPos:])
	c.Assert(err, IsNil)

	// String append
	nextPos, err = cs.creBucket.AppendObject(objectName, strings.NewReader(objectValue1), nextPos)
	c.Assert(err, IsNil)
	nextPos, err = cs.creBucket.AppendObject(objectName, strings.NewReader(objectValue2), nextPos)
	c.Assert(err, IsNil)

	body, err := cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)

	// Byte append
	nextPos = 0
	nextPos, err = cs.creBucket.AppendObject(objectName, bytes.NewReader(val[0:midPos]), nextPos)
	c.Assert(err, IsNil)
	nextPos, err = cs.creBucket.AppendObject(objectName, bytes.NewReader(val[midPos:]), nextPos)
	c.Assert(err, IsNil)

	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	err = cs.creBucket.DeleteObject(objectName)
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
	nextPos, err = cs.creBucket.AppendObject(objectName, fd, nextPos, options...)
	c.Assert(err, IsNil)

	meta, err := cs.creBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta:", meta, ",", nextPos)
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Appendable")
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("x-kss-Meta-Mine"), Equals, "")
	c.Assert(meta.Get("X-Kss-Next-Append-Position"), Equals, strconv.FormatInt(nextPos, 10))

	acl, err := cs.creBucket.GetObjectACL(objectName)
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
	nextPos, err = cs.creBucket.AppendObject(objectName, fd, nextPos, options...)
	c.Assert(err, IsNil)

	body, err = cs.creBucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err = readBody(body)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	meta, err = cs.creBucket.GetObjectDetailedMeta(objectName)
	c.Assert(err, IsNil)
	testLogger.Println("GetObjectDetailedMeta xxx:", meta)
	c.Assert(meta.Get("X-Kss-Object-Type"), Equals, "Appendable")
	c.Assert(meta.Get("X-Kss-Meta-My"), Equals, "myprop")
	c.Assert(meta.Get("x-Kss-Meta-Mine"), Equals, "")
	c.Assert(meta.Get("X-Kss-Next-Append-Position"), Equals, strconv.FormatInt(nextPos, 10))

	acl, err = cs.creBucket.GetObjectACL(objectName)
	c.Assert(err, IsNil)
	c.Assert(acl.ACL, Equals, string(ACLPublicRead))

	err = cs.creBucket.DeleteObject(objectName)
	c.Assert(err, IsNil)
}
