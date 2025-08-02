// client test
// use gocheck, install gocheck to execute "go get gopkg.in/check.v1",
// see https://labix.org/gocheck

package ks3

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Test hooks up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type Ks3ClientSuite struct {
	client *Client
	startT time.Time
}

var _ = Suite(&Ks3ClientSuite{})

var (
	// Endpoint/ID/Key
	endpoint  = os.Getenv("KS3_TEST_ENDPOINT")
	accessID  = os.Getenv("KS3_TEST_ACCESS_KEY_ID")
	accessKey = os.Getenv("KS3_TEST_ACCESS_KEY_SECRET")
	accountID = os.Getenv("KS3_TEST_ACCOUNT_ID")

	// Proxy
	proxyHost   = os.Getenv("KS3_TEST_PROXY_HOST")
	proxyUser   = os.Getenv("KS3_TEST_PROXY_USER")
	proxyPasswd = os.Getenv("KS3_TEST_PROXY_PASSWORD")

	// STS
	stsaccessID  = os.Getenv("KS3_TEST_STS_ID")
	stsaccessKey = os.Getenv("KS3_TEST_STS_KEY")
	stsARN       = os.Getenv("KS3_TEST_STS_ARN")

	// Credential
	credentialAccessID  = os.Getenv("KS3_CREDENTIAL_KEY_ID")
	credentialAccessKey = os.Getenv("KS3_CREDENTIAL_KEY_SECRET")
	credentialUID       = os.Getenv("KS3_CREDENTIAL_UID")
)

var (
	// prefix of bucket name for bucket ops test
	bucketNamePrefix = "go-sdk-test-bucket-"
	// bucket name for object ops test
	bucketName = bucketNamePrefix + RandLowStr(6)
	// object name for object ops test
	objectNamePrefix = "go-sdk-test-object-"
	// Credentials
	credentialBucketName = bucketNamePrefix + RandLowStr(6)
)

var (
	logPath            = "go_sdk_test_" + time.Now().Format("20060102_150405") + ".log"
	testLogFile, _     = os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0664)
	testLogger         = log.New(testLogFile, "", log.Ldate|log.Ltime|log.Lshortfile)
	letters            = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	timeoutInOperation = 1 * time.Second
)

// structs for replication get test
type GetResult struct {
	Rules []Rule `xml:"Rule"`
}

type Rule struct {
	Action                      string          `xml:"Action,omitempty"`                      // The replication action (ALL or PUT)
	ID                          string          `xml:"ID,omitempty"`                          // The rule ID
	Destination                 DestinationType `xml:"Destination"`                           // Container for storing target bucket information
	HistoricalObjectReplication string          `xml:"HistoricalObjectReplication,omitempty"` // Whether to copy copy historical data (enabled or not)
	Status                      string          `xml:"Status,omitempty"`                      // The replication status (starting, doing or closing)
}

type DestinationType struct {
	Bucket   string `xml:"Bucket"`
	Location string `xml:"Location"`
}

func RandStr(n int) string {
	b := make([]rune, n)
	randMarker := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range b {
		b[i] = letters[randMarker.Intn(len(letters))]
	}
	return string(b)
}

func CreateFile(fileName, content string, c *C) {
	fout, err := os.Create(fileName)
	defer fout.Close()
	c.Assert(err, IsNil)
	_, err = fout.WriteString(content)
	c.Assert(err, IsNil)
}

func RandLowStr(n int) string {
	return strings.ToLower(RandStr(n))
}

func ForceDeleteBucket(client *Client, bucketName string, c *C) {
	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	// Delete Object
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

	// Delete Part
	keyMarker := KeyMarker("")
	uploadIDMarker := UploadIDMarker("")
	for {
		lmur, err := bucket.ListMultipartUploads(keyMarker, uploadIDMarker)
		c.Assert(err, IsNil)
		for _, upload := range lmur.Uploads {
			var imur = InitiateMultipartUploadResult{Bucket: bucketName,
				Key: upload.Key, UploadID: upload.UploadID}
			err = bucket.AbortMultipartUpload(imur)
			c.Assert(err, IsNil)
		}
		keyMarker = KeyMarker(lmur.NextKeyMarker)
		uploadIDMarker = UploadIDMarker(lmur.NextUploadIDMarker)
		if !lmur.IsTruncated {
			break
		}
	}

	// Delete Bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		testLogger.Println(err)
	}
	c.Assert(err, IsNil)
}

func PutBucket(client *Client, bucketName string, c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)
	testLogger.Println("CreateBucket: ", bucketName)
}

func PutObject(client *Client, bucketName, objectName, content string, c *C) {
	bucket, err := client.Bucket(bucketName)
	c.Assert(err, IsNil)

	err = bucket.PutObject(objectName, strings.NewReader(content), nil)
	c.Assert(err, IsNil)
}

func IsBucketExist(client *Client, bucketName string) (bool, error) {
	_, err := client.HeadBucket(bucketName)
	if err == nil {
		return true, nil
	}

	switch err.(type) {
	case ServiceError:
		if err.(ServiceError).StatusCode == 404 {
			return false, nil
		}
	}

	return false, err
}

// SetUpSuite runs once when the suite starts running
func (s *Ks3ClientSuite) SetUpSuite(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	s.client = client

	lbr, err := s.client.ListBuckets()
	c.Assert(err, IsNil)
	for _, bucket := range lbr.Buckets {
		if strings.Contains(bucket.Name, bucketNamePrefix) {
			testLogger.Println("ForceDeleteBucket: ", bucket.Name)
			ForceDeleteBucket(s.client, bucket.Name, c)
		}
	}

	testLogger.Println("test client started")
}

// TearDownSuite runs before each test or benchmark starts running
func (s *Ks3ClientSuite) TearDownSuite(c *C) {
	lbr, err := s.client.ListBuckets()
	c.Assert(err, IsNil)

	for _, bucket := range lbr.Buckets {
		if strings.Contains(bucket.Name, bucketNamePrefix) {
			testLogger.Println("ForceDeleteBucket: ", bucket.Name)
			ForceDeleteBucket(s.client, bucket.Name, c)
		}
	}

	testLogger.Println("test client completed")
}

var cnt = 1

// SetUpTest runs after each test or benchmark runs
func (s *Ks3ClientSuite) SetUpTest(c *C) {
	testLogger.Printf("set up test:%s\n", c.TestName())
	s.startT = time.Now()
	str1 := "第" + strconv.Itoa(cnt) + "条用例开始执行"
	testLogger.Printf(str1)
}

// TearDownTest runs once after all tests or benchmarks have finished running
func (s *Ks3ClientSuite) TearDownTest(c *C) {
	endT := time.Now()
	cost := endT.UnixNano()/1000/1000 - s.startT.UnixNano()/1000/1000
	testLogger.Printf("tear down test:%s,cost:%d(ms)\n", c.TestName(), cost)
	cnt = cnt + 1
}

// TestCreateBucket
func (s *Ks3ClientSuite) TestCreateBucket(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Create
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	//sleep 3 seconds after create bucket
	time.Sleep(timeoutInOperation)

	// verify bucket is exist
	found, err := client.IsBucketExist(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(found, Equals, true)

	res, err := client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	// CreateBucket creates with ACLPublicRead
	err = client.CreateBucket(bucketNameTest, ACL(ACLPublicRead))
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicRead)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	// ACLPublicReadWrite
	err = client.CreateBucket(bucketNameTest, ACL(ACLPublicReadWrite))
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicReadWrite)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	// ACLPrivate
	err = client.CreateBucket(bucketNameTest, ACL(ACLPrivate))
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	// Delete
	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)

	// Create bucket with configuration and test GetBucketInfo
	for _, storage := range []BucketType{TypeNormal, TypeIA, TypeArchive} {
		bucketNameTest := bucketNamePrefix + RandLowStr(6)
		err = client.CreateBucket(bucketNameTest, BucketTypeClass(storage), ACL(ACLPublicRead))
		c.Assert(err, IsNil)
		time.Sleep(timeoutInOperation)

		// Delete
		err = client.DeleteBucket(bucketNameTest)
		c.Assert(err, IsNil)
	}

	// Error put bucket with configuration
	err = client.CreateBucket("ERRORBUCKETNAME", BucketTypeClass(TypeArchive))
	c.Assert(err, NotNil)
}

// TestCreateBucketNegative
func (s *Ks3ClientSuite) TestCreateBucketNegative(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Bucket name invalid
	err = client.CreateBucket("xx")
	c.Assert(err, NotNil)

	err = client.CreateBucket("XXXX")
	c.Assert(err, NotNil)
	testLogger.Println(err)

	err = client.CreateBucket("_bucket")
	c.Assert(err, NotNil)
	testLogger.Println(err)

	// ACL invalid
	err = client.CreateBucket(bucketNamePrefix+RandLowStr(6), ACL("InvaldAcl"))
	c.Assert(err, IsNil)
}

// TestDeleteBucket
func (s *Ks3ClientSuite) TestDeleteBucket(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Create
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Check
	found, err := client.IsBucketExist(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(found, Equals, true)

	// Delete
	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Check
	found, err = client.IsBucketExist(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(found, Equals, false)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, NotNil)
}

// TestDeleteBucketNegative
func (s *Ks3ClientSuite) TestDeleteBucketNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Bucket name invalid
	err = client.DeleteBucket("xx")
	c.Assert(err, NotNil)

	err = client.DeleteBucket("XXXX")
	c.Assert(err, NotNil)

	err = client.DeleteBucket("_bucket")
	c.Assert(err, NotNil)

	// Delete no exist bucket
	err = client.DeleteBucket("notexist")
	c.Assert(err, NotNil)

	// No permission to delete, this ak/sk for js sdk
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	accessID := "<accessKeyId>"
	accessKey := "<accessKeySecret>"
	clientOtherUser, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = clientOtherUser.DeleteBucket(bucketNameTest)
	c.Assert(err, NotNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestListBucket
func (s *Ks3ClientSuite) TestListBucket(c *C) {
	var prefix = bucketNamePrefix + RandLowStr(6)
	var bucketNameLbOne = prefix + "tlb1"
	var bucketNameLbTwo = prefix + "tlb2"
	var bucketNameLbThree = prefix + "tlb3"

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// CreateBucket
	err = client.CreateBucket(bucketNameLbOne)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameLbTwo)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameLbThree)
	c.Assert(err, IsNil)

	// ListBuckets, specified prefix
	var respHeader http.Header
	_, err = client.ListBuckets(GetResponseHeader(&respHeader))
	c.Assert(GetRequestId(respHeader) != "", Equals, true)
	c.Assert(err, IsNil)

	// DeleteBucket
	err = client.DeleteBucket(bucketNameLbOne)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameLbTwo)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameLbThree)
	c.Assert(err, IsNil)
}

// TestListBucket
func (s *Ks3ClientSuite) TestIsBucketExist(c *C) {
	var prefix = bucketNamePrefix + RandLowStr(6)
	var bucketNameLbOne = prefix + "tibe1"
	var bucketNameLbTwo = prefix + "tibe11"
	var bucketNameLbThree = prefix + "tibe111"

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// CreateBucket
	err = client.CreateBucket(bucketNameLbOne)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameLbTwo)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameLbThree)
	c.Assert(err, IsNil)

	// Exist
	exist, err := client.IsBucketExist(bucketNameLbTwo)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	exist, err = client.IsBucketExist(bucketNameLbThree)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	exist, err = client.IsBucketExist(bucketNameLbOne)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, true)

	// Not exist
	exist, err = client.IsBucketExist(prefix + "tibe")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	exist, err = client.IsBucketExist(prefix + "tibe1111")
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, false)

	// Negative
	exist, err = client.IsBucketExist("BucketNameInvalid")
	c.Assert(err, IsNil)

	// DeleteBucket
	err = client.DeleteBucket(bucketNameLbOne)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameLbTwo)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameLbThree)
	c.Assert(err, IsNil)
}

// TestSetBucketAcl
func (s *Ks3ClientSuite) TestSetBucketAcl(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Private
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	res, err := client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	// Set ACL_PUBLIC_R
	err = client.SetBucketACL(bucketNameTest, ACLPublicRead)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicRead)

	// Set ACL_PUBLIC_RW
	err = client.SetBucketACL(bucketNameTest, ACLPublicReadWrite)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicReadWrite)

	// Set ACL_PUBLIC_RW
	err = client.SetBucketACL(bucketNameTest, ACLPrivate)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketAclNegative
func (s *Ks3ClientSuite) TestBucketAclNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.SetBucketACL(bucketNameTest, "InvalidACL")
	c.Assert(err, IsNil)
	testLogger.Println(err)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestGetBucketAcl
func (s *Ks3ClientSuite) TestGetBucketAcl(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Private
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err := client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	// PublicRead
	err = client.CreateBucket(bucketNameTest, ACL(ACLPublicRead))
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicRead)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	// PublicReadWrite
	err = client.CreateBucket(bucketNameTest, ACL(ACLPublicReadWrite))
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPublicReadWrite)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestGetBucketLocation
func (s *Ks3ClientSuite) TestGetBucketLocation(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Private
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	loc, err := client.GetBucketLocation(bucketNameTest)
	c.Assert(loc, Equals, "BEIJING")

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestGetBucketLocationNegative
func (s *Ks3ClientSuite) TestGetBucketLocationNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Not exist
	_, err = client.GetBucketLocation(bucketNameTest)
	c.Assert(err, NotNil)

	// Not exist
	_, err = client.GetBucketLocation("InvalidBucketName_")
	c.Assert(err, NotNil)
}

// TestSetBucketLifecycle
func (s *Ks3ClientSuite) TestSetBucketLifecycle(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var rule1 = BuildLifecycleRuleByDate("rule1", "one", true, 2015, 11, 11)
	var rule2 = BuildLifecycleRuleByDays("rule2", "two", true, 3)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	// Set single rule
	var rules = []LifecycleRule{rule1}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)
	// Double set rule
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err := client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 1)
	c.Assert(res.Rules[0].ID, Equals, "rule1")

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	// Set two rules
	rules = []LifecycleRule{rule1, rule2}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	// Eliminate effect of cache
	time.Sleep(timeoutInOperation)

	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 2)
	c.Assert(res.Rules[0].ID, Equals, "rule1")
	c.Assert(res.Rules[1].ID, Equals, "rule2")

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketLifecycleNew
func (s *Ks3ClientSuite) TestSetBucketLifecycleNew(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	//invalid status of lifecyclerule
	expiration := LifecycleExpiration{
		Days: 30,
	}
	rule := LifecycleRule{
		ID:         "rule1",
		Prefix:     "one",
		Status:     "Invalid",
		Expiration: &expiration,
	}
	rules := []LifecycleRule{rule}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, NotNil)

	//invalid value of Date
	expiration = LifecycleExpiration{
		Date: RandStr(10),
	}
	rule = LifecycleRule{
		ID:         "rule1",
		Prefix:     "one",
		Status:     "Enabled",
		Expiration: &expiration,
	}
	rules = []LifecycleRule{rule}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, NotNil)

	//invalid value of Days
	abortMPU := LifecycleAbortIncompleteMultipartUpload{
		DaysAfterInitiation: -30,
	}
	rule = LifecycleRule{
		ID:                             "rule1",
		Prefix:                         "one",
		Status:                         "Enabled",
		AbortIncompleteMultipartUpload: &abortMPU,
	}
	rules = []LifecycleRule{rule}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, NotNil)

	expiration = LifecycleExpiration{
		Date: "2018-01-01T00:00:00+08:00",
	}
	rule1 := LifecycleRule{
		ID:         "rule1",
		Prefix:     "one",
		Status:     "Enabled",
		Expiration: &expiration,
	}

	abortMPU = LifecycleAbortIncompleteMultipartUpload{
		DaysAfterInitiation: 30,
	}
	rule2 := LifecycleRule{
		ID:                             "rule2",
		Prefix:                         "two",
		Status:                         "Enabled",
		Expiration:                     &expiration,
		AbortIncompleteMultipartUpload: &abortMPU,
	}

	transition1 := LifecycleTransition{
		Days:         3,
		StorageClass: StorageIA,
	}
	transition2 := LifecycleTransition{
		Days: 40,
		StorageClass: StorageArchive,
	}
	transitions := []LifecycleTransition{transition1, transition2}
	rule3 := LifecycleRule{
		ID:                             "rule3",
		Prefix:                         "three",
		Status:                         "Enabled",
		AbortIncompleteMultipartUpload: &abortMPU,
		Transitions:                    transitions,
	}

	// Set single rule
	rules = []LifecycleRule{rule1}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err := client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 1)
	c.Assert(res.Rules[0].ID, Equals, "rule1")
	c.Assert(res.Rules[0].Expiration, NotNil)
	c.Assert(res.Rules[0].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	// Set two rule: rule1 and rule2
	rules = []LifecycleRule{rule1, rule2}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 2)
	c.Assert(res.Rules[0].ID, Equals, "rule1")
	c.Assert(res.Rules[0].Expiration, NotNil)
	c.Assert(res.Rules[0].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[1].ID, Equals, "rule2")
	c.Assert(res.Rules[1].Expiration, NotNil)
	c.Assert(res.Rules[1].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	// Set two rule: rule2 and rule3
	rules = []LifecycleRule{rule2, rule3}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 2)
	c.Assert(res.Rules[0].ID, Equals, "rule2")
	c.Assert(res.Rules[0].Expiration, NotNil)
	c.Assert(res.Rules[0].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[0].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[0].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)
	c.Assert(res.Rules[1].ID, Equals, "rule3")
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)
	c.Assert(len(res.Rules[1].Transitions), Equals, 2)
	c.Assert(res.Rules[1].Transitions[0].StorageClass, Equals, StorageIA)
	c.Assert(res.Rules[1].Transitions[0].Days, Equals, 3)
	c.Assert(res.Rules[1].Transitions[1].StorageClass, Equals, StorageArchive)
	c.Assert(res.Rules[1].Transitions[1].Days, Equals, 40)

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	// Set two rule: rule1 and rule3
	rules = []LifecycleRule{rule1, rule3}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 2)
	c.Assert(res.Rules[0].ID, Equals, "rule1")
	c.Assert(res.Rules[0].Expiration, NotNil)
	c.Assert(res.Rules[0].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[1].ID, Equals, "rule3")
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)
	c.Assert(len(res.Rules[1].Transitions), Equals, 2)
	c.Assert(res.Rules[1].Transitions[0].StorageClass, Equals, StorageIA)
	c.Assert(res.Rules[1].Transitions[0].Days, Equals, 3)
	c.Assert(res.Rules[1].Transitions[1].StorageClass, Equals, StorageArchive)
	c.Assert(res.Rules[1].Transitions[1].Days, Equals, 40)

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	// Set three rules
	rules = []LifecycleRule{rule1, rule2, rule3}
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)

	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 3)
	c.Assert(res.Rules[0].ID, Equals, "rule1")
	c.Assert(res.Rules[0].Expiration, NotNil)
	c.Assert(res.Rules[0].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[1].ID, Equals, "rule2")
	c.Assert(res.Rules[1].Expiration, NotNil)
	c.Assert(res.Rules[1].Expiration.Date, Equals, "2018-01-01T00:00:00+08:00")
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[1].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)
	c.Assert(res.Rules[2].ID, Equals, "rule3")
	c.Assert(res.Rules[2].AbortIncompleteMultipartUpload, NotNil)
	c.Assert(res.Rules[2].AbortIncompleteMultipartUpload.DaysAfterInitiation, Equals, 30)
	c.Assert(len(res.Rules[2].Transitions), Equals, 2)
	c.Assert(res.Rules[2].Transitions[0].StorageClass, Equals, StorageIA)
	c.Assert(res.Rules[2].Transitions[0].Days, Equals, 3)
	c.Assert(res.Rules[2].Transitions[1].StorageClass, Equals, StorageArchive)
	c.Assert(res.Rules[2].Transitions[1].Days, Equals, 40)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketLifecycleXml
func (s *Ks3ClientSuite) TestSetBucketLifecycleXml(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
    <LifecycleConfiguration>
      <Rule>
        <ID>RuleID1</ID>
        <Filter>
		  <Prefix>Prefix</Prefix>
		</Filter>
        <Status>Enabled</Status>
        <Expiration>
          <Days>65</Days>
        </Expiration>
        <Transition>
          <Days>45</Days>
          <StorageClass>STANDARD_IA</StorageClass>
        </Transition>
        <AbortIncompleteMultipartUpload>
          <DaysAfterInitiation>30</DaysAfterInitiation>
        </AbortIncompleteMultipartUpload>
      </Rule>
      <Rule>
        <ID>RuleID2</ID>
        <Filter>
		  <Prefix>SubPrefix</Prefix>
		</Filter>
        <Status>Enabled</Status>
        <Expiration>
          <Days>60</Days>
        </Expiration>
        <Transition>
          <Days>40</Days>
          <StorageClass>ARCHIVE</StorageClass>
        </Transition>
        <AbortIncompleteMultipartUpload>
          <DaysAfterInitiation>40</DaysAfterInitiation>
        </AbortIncompleteMultipartUpload>
      </Rule>
    </LifecycleConfiguration>`

	err = client.SetBucketLifecycleXml(bucketNameTest, xmlBody)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestDeleteBucketLifecycle
func (s *Ks3ClientSuite) TestDeleteBucketLifecycle(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	var rule1 = BuildLifecycleRuleByDate("rule1", "one", true, 2015, 11, 11)
	var rule2 = BuildLifecycleRuleByDays("rule2", "two", true, 3)
	var rules = []LifecycleRule{rule1, rule2}

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	//time.Sleep(timeoutInOperation)

	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, IsNil)
	//time.Sleep(timeoutInOperation)

	res, err := client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(res.Rules), Equals, 2)

	// Delete
	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	//time.Sleep(timeoutInOperation)
	res, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, NotNil)

	// Eliminate effect of cache
	//time.Sleep(timeoutInOperation)

	// Delete when not set
	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketLifecycleNegative
func (s *Ks3ClientSuite) TestBucketLifecycleNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var rules = []LifecycleRule{}

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	// Set with no rule
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, NotNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)

	// Not exist
	err = client.SetBucketLifecycle(bucketNameTest, rules)
	c.Assert(err, NotNil)

	// Not exist
	_, err = client.GetBucketLifecycle(bucketNameTest)
	c.Assert(err, NotNil)

	// Not exist
	err = client.DeleteBucketLifecycle(bucketNameTest)
	c.Assert(err, NotNil)
}

// TestSetBucketLogging
func (s *Ks3ClientSuite) TestSetBucketLogging(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var bucketNameTarget = bucketNameTest + "-target"

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameTarget)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Set logging
	err = client.SetBucketLogging(bucketNameTest, bucketNameTarget, "prefix", true)
	c.Assert(err, IsNil)
	// Reset
	err = client.SetBucketLogging(bucketNameTest, bucketNameTarget, "prefix", false)
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	res, err := client.GetBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.LoggingEnabled.TargetBucket, Equals, "")
	c.Assert(res.LoggingEnabled.TargetPrefix, Equals, "")

	err = client.DeleteBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)

	// Set to self
	err = client.SetBucketLogging(bucketNameTest, bucketNameTest, "prefix", true)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameTarget)
	c.Assert(err, IsNil)
}

// TestDeleteBucketLogging
func (s *Ks3ClientSuite) TestDeleteBucketLogging(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var bucketNameTarget = bucketNameTest + "-target"

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameTarget)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Get when not set
	res, err := client.GetBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.LoggingEnabled.TargetBucket, Equals, "")
	c.Assert(res.LoggingEnabled.TargetPrefix, Equals, "")

	// Set
	err = client.SetBucketLogging(bucketNameTest, bucketNameTarget, "prefix", true)
	c.Assert(err, IsNil)

	// Get
	time.Sleep(timeoutInOperation)
	res, err = client.GetBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.LoggingEnabled.TargetBucket, Equals, bucketNameTarget)
	c.Assert(res.LoggingEnabled.TargetPrefix, Equals, "prefix")

	// Set
	err = client.SetBucketLogging(bucketNameTest, bucketNameTarget, "prefix", false)
	c.Assert(err, IsNil)

	// Get
	time.Sleep(timeoutInOperation)
	res, err = client.GetBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.LoggingEnabled.TargetBucket, Equals, "")
	c.Assert(res.LoggingEnabled.TargetPrefix, Equals, "")

	// Delete
	err = client.DeleteBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)

	// Get after delete
	time.Sleep(timeoutInOperation)
	res, err = client.GetBucketLogging(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.LoggingEnabled.TargetBucket, Equals, "")
	c.Assert(res.LoggingEnabled.TargetPrefix, Equals, "")

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameTarget)
	c.Assert(err, IsNil)
}

// TestSetBucketLoggingNegative
func (s *Ks3ClientSuite) TestSetBucketLoggingNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var bucketNameTarget = bucketNameTest + "-target"

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Not exist
	_, err = client.GetBucketLogging(bucketNameTest)
	c.Assert(err, NotNil)

	// Not exist
	err = client.SetBucketLogging(bucketNameTest, "targetbucket", "prefix", true)
	c.Assert(err, NotNil)

	// Not exist
	err = client.DeleteBucketLogging(bucketNameTest)
	c.Assert(err, NotNil)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Target bucket not exist
	err = client.SetBucketLogging(bucketNameTest, bucketNameTarget, "prefix", true)
	c.Assert(err, NotNil)

	// Parameter invalid
	err = client.SetBucketLogging(bucketNameTest, "XXXX", "prefix", true)
	c.Assert(err, NotNil)

	err = client.SetBucketLogging(bucketNameTest, "xx", "prefix", true)
	c.Assert(err, NotNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketCORS
func (s *Ks3ClientSuite) TestSetBucketCORS(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var rule1 = CORSRule{
		AllowedOrigin: []string{"*"},
		AllowedMethod: []string{"PUT", "GET", "POST"},
		AllowedHeader: []string{"*"},
		ExposeHeader:  []string{},
		MaxAgeSeconds: 100,
	}

	var rule2 = CORSRule{
		AllowedOrigin: []string{"http://www.a.com", "http://www.b.com"},
		AllowedMethod: []string{"GET"},
		AllowedHeader: []string{"Authorization"},
		ExposeHeader:  []string{"x-ks3-test", "x-ks3-test1"},
		MaxAgeSeconds: 200,
	}

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Set
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule1})
	c.Assert(err, IsNil)

	gbcr, err := client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(gbcr.CORSRules), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedOrigin), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedMethod), Equals, 3)
	c.Assert(len(gbcr.CORSRules[0].AllowedHeader), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].ExposeHeader), Equals, 0)
	c.Assert(gbcr.CORSRules[0].MaxAgeSeconds, Equals, 100)

	// Double set
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule1})
	c.Assert(err, IsNil)

	gbcr, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(gbcr.CORSRules), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedOrigin), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedMethod), Equals, 3)
	c.Assert(len(gbcr.CORSRules[0].AllowedHeader), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].ExposeHeader), Equals, 0)
	c.Assert(gbcr.CORSRules[0].MaxAgeSeconds, Equals, 100)

	// Set rule2
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule2})
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	gbcr, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(gbcr.CORSRules), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedOrigin), Equals, 2)
	c.Assert(len(gbcr.CORSRules[0].AllowedMethod), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].AllowedHeader), Equals, 1)
	c.Assert(len(gbcr.CORSRules[0].ExposeHeader), Equals, 2)
	c.Assert(gbcr.CORSRules[0].MaxAgeSeconds, Equals, 200)

	// Reset
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule1, rule2})
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	gbcr, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(gbcr.CORSRules), Equals, 2)

	// Set after delete
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule1, rule2})
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	gbcr, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(len(gbcr.CORSRules), Equals, 2)

	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestDeleteBucketCORS
func (s *Ks3ClientSuite) TestDeleteBucketCORS(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var rule = CORSRule{
		AllowedOrigin: []string{"*"},
		AllowedMethod: []string{"PUT", "GET", "POST"},
		AllowedHeader: []string{"*"},
		ExposeHeader:  []string{},
		MaxAgeSeconds: 100,
	}

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// Delete not set
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Set
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule})
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Detele
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Detele after deleting
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestSetBucketCORSNegative
func (s *Ks3ClientSuite) TestSetBucketCORSNegative(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var rule = CORSRule{
		AllowedOrigin: []string{"*"},
		AllowedMethod: []string{"PUT", "GET", "POST"},
		AllowedHeader: []string{"*"},
		ExposeHeader:  []string{},
		MaxAgeSeconds: 100,
	}

	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Not exist
	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, NotNil)

	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, NotNil)

	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule})
	c.Assert(err, NotNil)

	bucketNameTest = bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Set
	err = client.SetBucketCORS(bucketNameTest, []CORSRule{rule})
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Delete
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	_, err = client.GetBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	// Delete after deleting
	err = client.DeleteBucketCORS(bucketNameTest)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestEndpointFormat
func (s *Ks3ClientSuite) TestEndpointFormat(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	// http://host
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	res, err := client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
	time.Sleep(timeoutInOperation)

	// http://host:port
	client, err = New(endpoint+":80", accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)

	time.Sleep(timeoutInOperation)
	res, err = client.GetBucketACL(bucketNameTest)
	c.Assert(err, IsNil)
	c.Assert(res.GetCannedACL(), Equals, ACLPrivate)

	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestClientOption
func (s *Ks3ClientSuite) TestClientOption(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	client, err := New(endpoint, accessID, accessKey, UseCname(true),
		Timeout(11, 12), SecurityToken("token"), Proxy(proxyHost))
	c.Assert(err, IsNil)

	// CreateBucket timeout
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, NotNil)

	c.Assert(client.Conn.config.HTTPTimeout.ConnectTimeout, Equals, time.Second*11)
	c.Assert(client.Conn.config.HTTPTimeout.ReadWriteTimeout, Equals, time.Second*12)
	c.Assert(client.Conn.config.HTTPTimeout.HeaderTimeout, Equals, time.Second*12)
	c.Assert(client.Conn.config.HTTPTimeout.IdleConnTimeout, Equals, time.Second*12)
	c.Assert(client.Conn.config.HTTPTimeout.LongTimeout, Equals, time.Second*12*10)

	c.Assert(client.Conn.config.SecurityToken, Equals, "token")
	c.Assert(client.Conn.config.IsCname, Equals, true)

	c.Assert(client.Conn.config.IsUseProxy, Equals, true)
	c.Assert(client.Config.ProxyHost, Equals, proxyHost)

	client, err = New(endpoint, accessID, accessKey, AuthProxy(proxyHost, proxyUser, proxyPasswd))

	c.Assert(client.Conn.config.IsUseProxy, Equals, true)
	c.Assert(client.Config.ProxyHost, Equals, proxyHost)
	c.Assert(client.Conn.config.IsAuthProxy, Equals, true)
	c.Assert(client.Conn.config.ProxyUser, Equals, proxyUser)
	c.Assert(client.Conn.config.ProxyPassword, Equals, proxyPasswd)

	client, err = New(endpoint, accessID, accessKey, UserAgent("go sdk user agent"))
	c.Assert(client.Conn.config.UserAgent, Equals, "go sdk user agent")

	// Check we can overide the http.Client
	httpClient := new(http.Client)
	client, err = New(endpoint, accessID, accessKey, HTTPClient(httpClient))
	c.Assert(client.HTTPClient, Equals, httpClient)
	c.Assert(client.Conn.client, Equals, httpClient)
	client, err = New(endpoint, accessID, accessKey)
	c.Assert(client.HTTPClient, IsNil)
}

// Private
func (s *Ks3ClientSuite) checkBucket(buckets []BucketProperties, bucket string) bool {
	for _, v := range buckets {
		if v.Name == bucket {
			return true
		}
	}
	return false
}

func (s *Ks3ClientSuite) getBucket(buckets []BucketProperties, bucket string) (bool, BucketProperties) {
	for _, v := range buckets {
		if v.Name == bucket {
			return true, v
		}
	}
	return false, BucketProperties{}
}

func (s *Ks3ClientSuite) TestHttpLogNotSignUrl(c *C) {
	logName := "." + string(os.PathSeparator) + "test-go-sdk-httpdebug.log" + RandStr(5)
	f, err := os.OpenFile(logName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	c.Assert(err, IsNil)

	client, err := New(endpoint, accessID, accessKey)
	client.Config.LogLevel = Debug

	client.Config.Logger = log.New(f, "", log.LstdFlags)

	var testBucketName = bucketNamePrefix + RandLowStr(6)

	// CreateBucket
	err = client.CreateBucket(testBucketName)
	f.Close()

	// read log file,get http info
	contents, err := ioutil.ReadFile(logName)
	c.Assert(err, IsNil)

	httpContent := string(contents)

	c.Assert(strings.Contains(httpContent, "signStr"), Equals, true)
	c.Assert(strings.Contains(httpContent, "HTTP/"), Equals, true)
	c.Assert(strings.Contains(httpContent, "Request Headers:"), Equals, true)
	c.Assert(strings.Contains(httpContent, "Response Headers:"), Equals, true)

	// delete test bucket and log
	os.Remove(logName)
	client.DeleteBucket(testBucketName)
}

func (s *Ks3ClientSuite) TestSetLimitUploadSpeed(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	err = client.LimitUploadSpeed(100)

	goVersion := runtime.Version()
	pSlice := strings.Split(strings.ToLower(goVersion), ".")

	// compare with go1.7
	if len(pSlice) >= 2 {
		if pSlice[0] > "go1" {
			c.Assert(err, IsNil)
		} else if pSlice[0] == "go1" {
			subVersion, _ := strconv.Atoi(pSlice[1])
			if subVersion >= 7 {
				c.Assert(err, IsNil)
			} else {
				c.Assert(err, NotNil)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *Ks3ClientSuite) TestBucketPolicy(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	var responseHeader http.Header
	ret, err := client.GetBucketPolicy(bucketName, GetResponseHeader(&responseHeader))
	c.Assert(err, NotNil)
	requestId := GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	policyInfo := fmt.Sprintf(`{
	  "Statement": [
		{
		  "Effect": "Allow",
		  "Action": [
			"ks3:ListBucket",
			"ks3:ListBucketMultipartUploads",
			"ks3:GetObject",
			"ks3:GetObjectAcl",
			"ks3:ListMultipartUploadParts"
		  ],
		  "Principal": {
			"KSC": [
			  "*"
			]
		  },
		  "Resource": [
			"krn:ksc:ks3:::%s",
			"krn:ksc:ks3:::%s/*"
		  ]
		}
	  ]
	}`, bucketName, bucketName)

	err = client.SetBucketPolicy(bucketName, policyInfo, GetResponseHeader(&responseHeader))
	c.Assert(err, IsNil)
	requestId = GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	ret, err = client.GetBucketPolicy(bucketName, GetResponseHeader(&responseHeader))
	c.Assert(err, IsNil)
	testLogger.Println("policy:", ret)
	c.Assert(ret, Equals, policyInfo)
	requestId = GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	err = client.DeleteBucketPolicy(bucketName, GetResponseHeader(&responseHeader))
	c.Assert(err, IsNil)
	requestId = GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)
	client.DeleteBucket(bucketName)
}

func (s *Ks3ClientSuite) TestBucketPolicyNegative(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	var responseHeader http.Header
	_, err = client.GetBucketPolicy(bucketName, GetResponseHeader(&responseHeader))
	c.Assert(err, NotNil)
	requestId := GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	errPolicy := fmt.Sprintf(`{
	  "Statement": [
		{
		  "Effect": "Allow",
		  "Action": [
			"ks3:ListBucket",
			"ks3:ListBucketMultipartUploads",
			"ks3:GetObject",
			"ks3:GetObjectAcl",
			"ks3:ListMultipartUploadParts"
		  ],
		  "Principal": {
			"KSC": [
			  "*"
			]
		  },
		  "Resource": [
			"krn:ksc:ks3:::%s"
		  ]
		}
	  ]
	}`, bucketName)
	err = client.SetBucketPolicy(bucketName, errPolicy, GetResponseHeader(&responseHeader))
	c.Assert(err, NotNil)
	testLogger.Println("err:", err)
	requestId = GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	err = client.DeleteBucketPolicy(bucketName, GetResponseHeader(&responseHeader))
	c.Assert(err, IsNil)

	bucketNameEmpty := bucketNamePrefix + RandLowStr(5)
	client.DeleteBucket(bucketNameEmpty)

	err = client.DeleteBucketPolicy(bucketNameEmpty, GetResponseHeader(&responseHeader))
	c.Assert(err, NotNil)
	requestId = GetRequestId(responseHeader)
	c.Assert(len(requestId) > 0, Equals, true)

	client.DeleteBucket(bucketName)
}

// struct to string
func struct2string(obj interface{}, c *C) string {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		data[t.Field(i).Name] = v.Field(i).Interface()
	}
	str, err := json.Marshal(data)
	c.Assert(err, IsNil)
	return string(str)
}

type TestCredentials struct {
}

func (testCreInf *TestCredentials) GetAccessKeyID() string {
	return os.Getenv("KS3_TEST_ACCESS_KEY_ID")
}

func (testCreInf *TestCredentials) GetAccessKeySecret() string {
	return os.Getenv("KS3_TEST_ACCESS_KEY_SECRET")
}

func (testCreInf *TestCredentials) GetSecurityToken() string {
	return ""
}

type TestCredentialsProvider struct {
}

func (testInfBuild *TestCredentialsProvider) GetCredentials() Credentials {
	return &TestCredentials{}
}

func (s *Ks3ClientSuite) TestClientCredentialInfBuild(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	var defaultBuild TestCredentialsProvider
	client, err := New(endpoint, "", "", SetCredentialsProvider(&defaultBuild))
	c.Assert(err, IsNil)
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestClientSetLocalIpError(c *C) {
	// create client and bucket
	ipAddr, err := net.ResolveIPAddr("ip", "127.0.0.1")
	c.Assert(err, IsNil)
	localTCPAddr := &(net.TCPAddr{IP: ipAddr.IP})
	client, err := New(endpoint, accessID, accessKey, SetLocalAddr(localTCPAddr))
	c.Assert(err, IsNil)

	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, NotNil)
}

func (s *Ks3ClientSuite) TestClientSetLocalIpSuccess(c *C) {
	//get local ip
	conn, err := net.Dial("udp", "8.8.8.8:80")
	c.Assert(err, IsNil)
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	localIp := localAddr.IP.String()
	conn.Close()

	ipAddr, err := net.ResolveIPAddr("ip", localIp)
	c.Assert(err, IsNil)
	localTCPAddr := &(net.TCPAddr{IP: ipAddr.IP})
	client, err := New(endpoint, accessID, accessKey, SetLocalAddr(localTCPAddr))
	c.Assert(err, IsNil)

	var bucketNameTest = bucketNamePrefix + RandLowStr(6)
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, IsNil)
	err = client.DeleteBucket(bucketNameTest)
	c.Assert(err, IsNil)
}

// TestCreateBucketInvalidName
func (s *Ks3ClientSuite) TestCreateBucketInvalidName(c *C) {
	var bucketNameTest = "-" + bucketNamePrefix + RandLowStr(6)
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)
	// Create
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, NotNil)
}

// TestClientProcessEndpointSuccess
func (s *Ks3ClientSuite) TestClientProcessEndpointSuccess(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	testEndpoint := endpoint + "/" + "sina.com" + "?" + "para=abc"

	client, err := New(testEndpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Create
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, NotNil)
}

// TestClientProcessEndpointSuccess
func (s *Ks3ClientSuite) TestClientProcessEndpointError(c *C) {
	var bucketNameTest = bucketNamePrefix + RandLowStr(6)

	testEndpoint := "https://127.0.0.1/" + endpoint

	client, err := New(testEndpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	// Create
	err = client.CreateBucket(bucketNameTest)
	c.Assert(err, NotNil)
}

// TestClientBucketError
func (s *Ks3ClientSuite) TestClientBucketError(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := "-" + RandLowStr(5)
	_, err = client.Bucket(bucketName)
	c.Assert(err, NotNil)
}

func (s *Ks3ClientSuite) TestSetBucketInventory(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	// not any encryption
	invConfig := InventoryConfiguration{
		Id:        "report1",
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "filterPrefix/",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    bucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{
				"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus",
			},
		},
	}

	// case 1: not any encryption
	err = client.PutBucketInventory(bucketName, invConfig)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestBucketInventory(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	invConfig := InventoryConfiguration{
		Id:        "report1",
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "filterPrefix/",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    bucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{
				"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus",
			},
		},
	}

	// case 1: test SetBucketInventory
	err = client.PutBucketInventory(bucketName, invConfig)
	c.Assert(err, IsNil)

	// case 2: test GetBucketInventory
	out, err := client.GetBucketInventory(bucketName, "report1")
	c.Assert(err, IsNil)
	invConfig.XMLName.Local = "InventoryConfiguration"
	invConfig.Filter.XMLName.Local = "Filter"
	invConfig.Schedule.XMLName.Local = "Schedule"
	invConfig.Destination.XMLName.Local = "Destination"
	invConfig.Destination.KS3BucketDestination.XMLName.Local = "KS3BucketDestination"
	invConfig.OptionalFields.XMLName.Local = "OptionalFields"
	c.Assert(struct2string(invConfig, c), Equals, struct2string(out, c))

	// case 3: test ListBucketInventory
	invConfig2 := InventoryConfiguration{
		Id:        "report2",
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "filterPrefix/",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    bucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{
				"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus",
			},
		},
	}
	invConfig2.XMLName.Local = "InventoryConfiguration"
	invConfig2.Filter.XMLName.Local = "Filter"
	invConfig2.Schedule.XMLName.Local = "Schedule"
	invConfig2.Destination.XMLName.Local = "Destination"
	invConfig2.Destination.KS3BucketDestination.XMLName.Local = "KS3BucketDestination"
	invConfig2.OptionalFields.XMLName.Local = "OptionalFields"
	err = client.PutBucketInventory(bucketName, invConfig2)
	c.Assert(err, IsNil)

	listInvConf, err := client.ListBucketInventory(bucketName, "", Marker("report1"), MaxKeys(2))
	c.Assert(err, IsNil)
	var listInvLocal ListInventoryConfigurationsResult
	listInvLocal.InventoryConfiguration = []InventoryConfiguration{
		invConfig,
		invConfig2,
	}
	listInvLocal.XMLName.Local = "ListInventoryConfigurationsResult"
	c.Assert(struct2string(listInvLocal, c), Equals, struct2string(listInvConf, c))

	err = client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestBucketInventoryNegative(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	err = client.CreateBucket(bucketName)
	c.Assert(err, IsNil)

	invConfigErr := InventoryConfiguration{
		Id:        "report1",
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "filterPrefix/",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    bucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{
				"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus",
			},
		},
	}
	// case 1: test SetBucketInventory
	err = client.PutBucketInventory(bucketName, invConfigErr)
	c.Assert(err, IsNil)

	// case 2: test GetBucketInventory
	_, err = client.GetBucketInventory(bucketName, "report1")
	c.Assert(err, IsNil)

	// case 3: test ListBucketInventory
	_, err = client.ListBucketInventory(bucketName, "")
	c.Assert(err, IsNil)

	// case 4: test DeleteBucketInventory
	err = client.DeleteBucketInventory(bucketName, "report1")
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)
}

// compare with go1.7
func compareVersion(goVersion string) bool {
	nowVersion := runtime.Version()
	nowVersion = strings.Replace(nowVersion, "go", "", -1)
	pSlice1 := strings.Split(goVersion, ".")
	pSlice2 := strings.Split(nowVersion, ".")
	for k, v := range pSlice2 {
		n2, _ := strconv.Atoi(string(v))
		n1, _ := strconv.Atoi(string(pSlice1[k]))
		if n2 > n1 {
			return true
		}
		if n2 < n1 {
			return false
		}
	}
	return true
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/redirectTo", http.StatusFound)
}
func targetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "You have been redirected here!")
}

func (s *Ks3ClientSuite) TestClientRedirect(c *C) {
	// must go1.7.0 onward
	if !compareVersion("1.7.0") {
		return
	}

	// get port
	rand.Seed(time.Now().Unix())
	port := 10000 + rand.Intn(10000)

	// start http server
	httpAddr := fmt.Sprintf("127.0.0.1:%d", port)
	mux := http.NewServeMux()
	mux.HandleFunc("/redirectTo", targetHandler)
	mux.HandleFunc("/", homeHandler)
	svr := &http.Server{
		Addr:           httpAddr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        mux,
	}

	go func() {
		svr.ListenAndServe()
	}()

	time.Sleep(3 * time.Second)

	url := "http://" + httpAddr

	// create client 1,redirect disable
	client1, err := New(endpoint, accessID, accessKey, RedirectEnabled(false))
	resp, err := client1.Conn.client.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusFound)
	resp.Body.Close()

	// create client2, redirect enabled
	client2, err := New(endpoint, accessID, accessKey, RedirectEnabled(true))
	resp, err = client2.Conn.client.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, 200)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(string(data), Equals, "You have been redirected here!")
	resp.Body.Close()

	svr.Close()
}

func verifyCertificatehandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("verifyCertificatehandler"))
}

func (s *Ks3ClientSuite) TestClientSkipVerifyCertificateTestServer(c *C) {
	// get port
	rand.Seed(time.Now().Unix())
	port := 10000 + rand.Intn(10000)

	// start https server
	httpAddr := fmt.Sprintf("127.0.0.1:%d", port)
	mux := http.NewServeMux()
	mux.HandleFunc("/", verifyCertificatehandler)
	svr := &http.Server{
		Addr:           httpAddr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        mux,
	}

	go func() {
		svr.ListenAndServeTLS("../sample/test_cert.pem", "../sample/test_key.pem")
	}()

	// wait http server started
	time.Sleep(3 * time.Second)

	url := "https://" + httpAddr

	// create client 1,not verify certificate
	client1, err := New(endpoint, accessID, accessKey, InsecureSkipVerify(true))
	resp, err := client1.Conn.client.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, 200)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(string(data), Equals, "verifyCertificatehandler")
	resp.Body.Close()

	// create client2, verify certificate
	client2, err := New(endpoint, accessID, accessKey, InsecureSkipVerify(false))
	resp, err = client2.Conn.client.Get(url)
	c.Assert(err, NotNil)
}

func (s *Ks3ClientSuite) TestClientSkipVerifyCertificateKs3Server(c *C) {
	// create a bucket with default proprety
	client, err := New(endpoint, accessID, accessKey, InsecureSkipVerify(true))
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

	//
	resp, err := bucket.GetObject(objectName)
	c.Assert(err, IsNil)
	str, err := readBody(resp)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, objectValue)

	ForceDeleteBucket(client, bucketName, c)

}

func (s *Ks3ClientSuite) TestCreateBucketXml(c *C) {
	client, err := New(endpoint, accessID, accessKey)
	c.Assert(err, IsNil)

	bucketName := bucketNamePrefix + RandLowStr(5)
	xmlBody := `
		<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <LocationConstraint>BEIJING</LocationConstraint>
		</CreateBucketConfiguration>
        `
	err = client.CreateBucketXml(bucketName, xmlBody)
	c.Assert(err, IsNil)

	err = client.DeleteBucket(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestPutBucketRetention(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	_, err := s.client.GetBucketRetention(bucketName)
	c.Assert(err, NotNil)

	rule := RetentionRule{
		Status: "Enabled",
		Days:   7,
	}

	err = s.client.PutBucketRetention(bucketName, rule)
	c.Assert(err, IsNil)

	retentionCfg, err := s.client.GetBucketRetention(bucketName)
	c.Assert(err, IsNil)
	c.Assert(retentionCfg.Rule.Status, Equals, "Enabled")
	c.Assert(retentionCfg.Rule.Days, Equals, 7)
}

func (s *Ks3ClientSuite) TestPutBucketRetentionXml(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	_, err := s.client.GetBucketRetentionXml(bucketName)
	c.Assert(err, NotNil)

	retentionXml := `
	<RetentionConfiguration>
		<Rule>
			<Status>Enabled</Status>  
			<Days>7</Days>
		</Rule>
	</RetentionConfiguration>`

	err = s.client.PutBucketRetentionXml(bucketName, retentionXml)
	c.Assert(err, IsNil)

	output, err := s.client.GetBucketRetentionXml(bucketName)
	c.Assert(err, IsNil)

	var retention RetentionConfiguration
	err = xml.Unmarshal([]byte(output), &retention)
	c.Assert(err, IsNil)
	c.Assert(retention.Rule.Status, Equals, "Enabled")
	c.Assert(retention.Rule.Days, Equals, 7)
}

func (s *Ks3ClientSuite) TestListRetention(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	rule := RetentionRule{
		Status: "Enabled",
		Days:   7,
	}

	err := s.client.PutBucketRetention(bucketName, rule)
	c.Assert(err, IsNil)

	bucket, err := s.client.Bucket(bucketName)
	c.Assert(err, IsNil)

	objectKey := RandLowStr(12)

	err = bucket.PutObject(objectKey, strings.NewReader("test content"))
	c.Assert(err, IsNil)

	_, err = bucket.GetObjectMeta(objectKey)
	c.Assert(err, IsNil)

	err = bucket.DeleteObject(objectKey)
	c.Assert(err, IsNil)

	_, err = bucket.ListRetention()
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestPutBucketMirror(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	_, err := s.client.GetBucketMirror(bucketName)
	c.Assert(err, NotNil)

	bucketMirror := BucketMirror{
		Version:          "V3",
		UseDefaultRobots: Boolean(false),
		AsyncMirrorRule: AsyncMirrorRule{
			MirrorUrls: []string{
				"https://abc.com",
				"https://123.com",
			},
			SavingSetting: SavingSetting{
				ACL: "private",
			},
		},
		SyncMirrorRules: []SyncMirrorRules{
			{
				MatchCondition: MatchCondition{
					HTTPCodes: []string{
						"404",
					},
					KeyPrefixes: []string{
						"abc",
					},
				},
				MirrorURL: "https://v-ks-a-i.originalvod.com",
				MirrorRequestSetting: MirrorRequestSetting{
					PassQueryString: Boolean(false),
					Follow3Xx:       Boolean(false),
					HeaderSetting: HeaderSetting{
						SetHeaders: []SetHeaders{
							{
								Key:   "a",
								Value: "b",
							},
						},
						RemoveHeaders: []RemoveHeaders{
							{
								Key: "c",
							},
							{
								Key: "d",
							},
						},
						PassAll: Boolean(false),
						PassHeaders: []PassHeaders{
							{
								Key: "key",
							},
						},
					},
				},
				SavingSetting: SavingSetting{
					ACL: "private",
				},
			},
		},
	}
	err = s.client.PutBucketMirror(bucketName, bucketMirror)
	c.Assert(err, IsNil)

	bucketMirrorResult, err := s.client.GetBucketMirror(bucketName)
	c.Assert(err, IsNil)
	c.Assert(bucketMirrorResult.Version, Equals, "V3")
	c.Assert(bucketMirrorResult.AsyncMirrorRule.SavingSetting.ACL, Equals, "private")
	c.Assert(len(bucketMirrorResult.SyncMirrorRules), Equals, 1)
	c.Assert(*bucketMirrorResult.SyncMirrorRules[0].MirrorRequestSetting.HeaderSetting.PassAll, Equals, false)
	c.Assert(bucketMirrorResult.CreatedTime, Not(Equals), "")
}

func (s *Ks3ClientSuite) TestPutBucketMirrorJson(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	_, err := s.client.GetBucketMirrorJson(bucketName)
	c.Assert(err, NotNil)

	bucketMirrorJson := `{
		"version": "V3",
		"use_default_robots": false,
		"async_mirror_rule": {
			"mirror_urls": [
				"https://abc.com",
				"https://123.com"
			],
			"saving_setting": {
				"acl": "private"
			}
		},
		"sync_mirror_rules": [
			{
				"match_condition": {
					"http_codes": [
						"404"
					],
					"key_prefixes": [
						"abc"
					]
				},
				"mirror_url": "https://v-ks-a-i.originalvod.com",
				"mirror_request_setting": {
					"pass_query_string": false,
					"follow3xx": false,
					"header_setting": {
						"set_headers": [
							{
								"key": "a",
								"value": "b"
							}
						],
						"remove_headers": [
							{
								"key": "c"
							},
							{
								"key": "d"
							}
						],
						"pass_all": false,
						"pass_headers": [
							{
								"key": "key"
							}
						]
					}
				},
				"saving_setting": {
					"acl": "private"
				}
			}
		]
	}`
	err = s.client.PutBucketMirrorJson(bucketName, bucketMirrorJson)
	c.Assert(err, IsNil)

	bucketMirrorResultJson, err := s.client.GetBucketMirrorJson(bucketName)
	c.Assert(err, IsNil)

	bucketMirrorResult := GetBucketMirrorResult{}
	err = json.Unmarshal([]byte(bucketMirrorResultJson), &bucketMirrorResult)
	c.Assert(err, IsNil)
	c.Assert(bucketMirrorResult.Version, Equals, "V3")
	c.Assert(bucketMirrorResult.AsyncMirrorRule.SavingSetting.ACL, Equals, "private")
	c.Assert(len(bucketMirrorResult.SyncMirrorRules), Equals, 1)
	c.Assert(*bucketMirrorResult.SyncMirrorRules[0].MirrorRequestSetting.HeaderSetting.PassAll, Equals, false)
	c.Assert(bucketMirrorResult.CreatedTime, Not(Equals), "")
}

func (s *Ks3ClientSuite) TestDeleteBucketMirror(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	err := s.client.DeleteBucketMirror(bucketName)
	c.Assert(err, NotNil)

	bucketMirror := BucketMirror{
		Version:          "V3",
		UseDefaultRobots: Boolean(false),
		AsyncMirrorRule: AsyncMirrorRule{
			MirrorUrls: []string{
				"https://abc.com",
				"https://123.com",
			},
			SavingSetting: SavingSetting{
				ACL: "private",
			},
		},
		SyncMirrorRules: []SyncMirrorRules{
			{
				MatchCondition: MatchCondition{
					HTTPCodes: []string{
						"404",
					},
					KeyPrefixes: []string{
						"abc",
					},
				},
				MirrorURL: "https://v-ks-a-i.originalvod.com",
				MirrorRequestSetting: MirrorRequestSetting{
					PassQueryString: Boolean(false),
					Follow3Xx:       Boolean(false),
					HeaderSetting: HeaderSetting{
						SetHeaders: []SetHeaders{
							{
								Key:   "a",
								Value: "b",
							},
						},
						RemoveHeaders: []RemoveHeaders{
							{
								Key: "c",
							},
							{
								Key: "d",
							},
						},
						PassAll: Boolean(false),
						PassHeaders: []PassHeaders{
							{
								Key: "key",
							},
						},
					},
				},
				SavingSetting: SavingSetting{
					ACL: "private",
				},
			},
		},
	}
	err = s.client.PutBucketMirror(bucketName, bucketMirror)
	c.Assert(err, IsNil)

	err = s.client.DeleteBucketMirror(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestPutBucketInventory(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	id := "inventory1" + RandLowStr(6)
	dstBucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, dstBucketName, c)
	inventoryConfiguration := InventoryConfiguration{
		Id:        id,
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "abc",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    dstBucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus"},
		},
	}
	err := s.client.PutBucketInventory(bucketName, inventoryConfiguration)
	c.Assert(err, IsNil)

	inventoryResult, err := s.client.GetBucketInventory(bucketName, id)
	c.Assert(err, IsNil)
	c.Assert(inventoryResult.Id, Equals, id)
	c.Assert(*inventoryResult.IsEnabled, Equals, true)
	c.Assert(inventoryResult.Filter.Prefix, Equals, "abc")
	c.Assert(inventoryResult.Destination.KS3BucketDestination.Format, Equals, "CSV")
	c.Assert(inventoryResult.Destination.KS3BucketDestination.AccountId, Equals, accountID)
	c.Assert(inventoryResult.Destination.KS3BucketDestination.Bucket, Equals, dstBucketName)
	c.Assert(inventoryResult.Destination.KS3BucketDestination.Prefix, Equals, "prefix1")
	c.Assert(inventoryResult.Schedule.Frequency, Equals, "Once")
	c.Assert(len(inventoryResult.OptionalFields.Field), Equals, 6)
}

func (s *Ks3ClientSuite) TestListBucketInventory(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	id := "inventory1" + RandLowStr(6)
	dstBucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, dstBucketName, c)
	inventoryConfiguration := InventoryConfiguration{
		Id:        id,
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "abc",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    dstBucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus"},
		},
	}
	err := s.client.PutBucketInventory(bucketName, inventoryConfiguration)
	c.Assert(err, IsNil)

	id2 := "inventory2" + RandLowStr(6)
	inventoryConfiguration2 := InventoryConfiguration{
		Id:        id2,
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "xyz",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "ORC",
				AccountId: accountID,
				Bucket:    dstBucketName,
				Prefix:    "prefix2",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus"},
		},
	}
	err = s.client.PutBucketInventory(bucketName, inventoryConfiguration2)
	c.Assert(err, IsNil)

	inventoryList, err := s.client.ListBucketInventory(bucketName, "")
	c.Assert(err, IsNil)
	c.Assert(len(inventoryList.InventoryConfiguration), Equals, 2)
}

func (s *Ks3ClientSuite) TestDeleteBucketInventory(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	id := "inventory1" + RandLowStr(6)
	inventoryResult, err := s.client.GetBucketInventory(bucketName, id)
	c.Assert(err, NotNil)

	dstBucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, dstBucketName, c)
	inventoryConfiguration := InventoryConfiguration{
		Id:        id,
		IsEnabled: Boolean(true),
		Filter: InventoryFilter{
			Prefix: "abc",
		},
		Destination: Destination{
			KS3BucketDestination: KS3BucketDestination{
				Format:    "CSV",
				AccountId: accountID,
				Bucket:    dstBucketName,
				Prefix:    "prefix1",
			},
		},
		Schedule: Schedule{
			Frequency: "Once",
		},
		OptionalFields: OptionalFields{
			Field: []string{"Size", "LastModifiedDate", "ETag", "StorageClass", "IsMultipartUploaded", "EncryptionStatus"},
		},
	}
	err = s.client.PutBucketInventory(bucketName, inventoryConfiguration)
	c.Assert(err, IsNil)

	inventoryResult, err = s.client.GetBucketInventory(bucketName, id)
	c.Assert(err, IsNil)
	c.Assert(inventoryResult.Id, Equals, id)
	c.Assert(*inventoryResult.IsEnabled, Equals, true)

	err = s.client.DeleteBucketInventory(bucketName, id)
	c.Assert(err, IsNil)

	inventoryResult, err = s.client.GetBucketInventory(bucketName, id)
	c.Assert(err, NotNil)
}

func (s *Ks3ClientSuite) TestBucketReplication(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	targetBucket := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, targetBucket, c)

	replicationResult, err := s.client.GetBucketReplication(bucketName)
	c.Assert(err, NotNil)

	replication := Replication{
		Prefix: []string{"abc/"},
		DeleteMarkerStatus:          "Enabled",
		TargetBucket:                targetBucket,
		HistoricalObjectReplication: "Enabled",
	}

	err = s.client.PutBucketReplication(bucketName, replication)
	c.Assert(err, IsNil)

	replicationResult, err = s.client.GetBucketReplication(bucketName)
	c.Assert(err, IsNil)
	c.Assert(len(replicationResult.Prefix), Equals, 1)
	c.Assert(replicationResult.Prefix[0], Equals, "abc/")
	c.Assert(replicationResult.DeleteMarkerStatus, Equals, "Enabled")
	c.Assert(replicationResult.TargetBucket, Equals, targetBucket)
	c.Assert(replicationResult.Region, Equals, "BEIJING")
	c.Assert(replicationResult.HistoricalObjectReplication, Equals, "Enabled")

	err = s.client.DeleteBucketReplication(bucketName)
	c.Assert(err, IsNil)

	replicationXml := fmt.Sprintf(`<Replication xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<prefix>abc2/</prefix>
		<DeleteMarkerStatus>Enabled</DeleteMarkerStatus>
		<targetBucket>%s</targetBucket>
		<HistoricalObjectReplication>Enabled</HistoricalObjectReplication>
	</Replication>`, targetBucket)

	err = s.client.PutBucketReplicationXml(bucketName, replicationXml)
	c.Assert(err, IsNil)

	replicationResultXml, err := s.client.GetBucketReplicationXml(bucketName)
	c.Assert(err, IsNil)

	var replicationXmlResult Replication
	err = xml.Unmarshal([]byte(replicationResultXml), &replicationXmlResult)
	c.Assert(err, IsNil)
	c.Assert(len(replicationXmlResult.Prefix), Equals, 1)
	c.Assert(replicationXmlResult.Prefix[0], Equals, "abc2/")
	c.Assert(replicationXmlResult.DeleteMarkerStatus, Equals, "Enabled")
	c.Assert(replicationXmlResult.TargetBucket, Equals, targetBucket)
	c.Assert(replicationXmlResult.Region, Equals, "BEIJING")
	c.Assert(replicationXmlResult.HistoricalObjectReplication, Equals, "Enabled")

	err = s.client.DeleteBucketReplication(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestBucketEncryption(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	_, err := s.client.GetBucketEncryption(bucketName)
	c.Assert(err, NotNil)

	encryptionRule := ServerSideEncryptionRule{
		ApplyServerSideEncryptionByDefault: ApplyServerSideEncryptionByDefault{
			SSEAlgorithm: "AES256",
		},
	}

	err = s.client.PutBucketEncryption(bucketName, encryptionRule)
	c.Assert(err, IsNil)

	encryptionResult, err := s.client.GetBucketEncryption(bucketName)
	c.Assert(err, IsNil)
	c.Assert(encryptionResult.ServerSideEncryptionRule.ApplyServerSideEncryptionByDefault.SSEAlgorithm, Equals, "AES256")

	err = s.client.DeleteBucketEncryption(bucketName)
	c.Assert(err, IsNil)

	encryptionRuleXml := `
    <ServerSideEncryptionConfiguration>
        <Rule>
            <ApplyServerSideEncryptionByDefault>
                <SSEAlgorithm>AES256</SSEAlgorithm>
            </ApplyServerSideEncryptionByDefault>
        </Rule>
    </ServerSideEncryptionConfiguration>`

	err = s.client.PutBucketEncryptionXml(bucketName, encryptionRuleXml)
	c.Assert(err, IsNil)

	encryptionResultXml, err := s.client.GetBucketEncryptionXml(bucketName)
	c.Assert(err, IsNil)

	var encryptionCfg ServerSideEncryptionConfiguration
	err = xml.Unmarshal([]byte(encryptionResultXml), &encryptionCfg)
	c.Assert(err, IsNil)
	c.Assert(encryptionCfg.ServerSideEncryptionRule.ApplyServerSideEncryptionByDefault.SSEAlgorithm, Equals, "AES256")

	err = s.client.DeleteBucketEncryption(bucketName)
	c.Assert(err, IsNil)
}

func (s *Ks3ClientSuite) TestBucketTagging(c *C) {
	bucketName := bucketNamePrefix + RandLowStr(6)
	PutBucket(s.client, bucketName, c)

	res, err := s.client.GetBucketTagging(bucketName)
	c.Assert(err, IsNil)
	c.Assert(len(res.Tags), Equals, 0)

	tagging := Tagging{
		Tags: []Tag{
			{
				Key:   "key1",
				Value: "value1",
			},
			{
				Key:   "key2",
				Value: "value2",
			},
		},
	}

	err = s.client.SetBucketTagging(bucketName, tagging)
	c.Assert(err, IsNil)

	res, err = s.client.GetBucketTagging(bucketName)
	c.Assert(err, IsNil)
	c.Assert(len(res.Tags), Equals, 2)

	err = s.client.DeleteBucketTagging(bucketName)
	c.Assert(err, IsNil)
}