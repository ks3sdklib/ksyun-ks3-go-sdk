package sample

import (
	"fmt"

	"github.com/wilac-pv/ksyun-ks3-go-sdk/ks3"
)

// CreateBucketSample shows how to create bucket
func CreateBucketSample() {
	// New client
	client, err := ks3.New(endpoint, accessID, accessKey)
	if err != nil {
		HandleError(err)
	}

	//	DeleteTestBucketAndObject(bucketName)

	// Case 1: Create a bucket with default parameters
	err = client.CreateBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Delete bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Case 2: Create the bucket with ACL
	err = client.CreateBucket(bucketName, ks3.ACL(ks3.ACLPublicRead))
	if err != nil {
		HandleError(err)
	}

	// Case 3: Repeat the same bucket. KS3 will not return error, but just no op. The ACL is not updated.
	err = client.CreateBucket(bucketName, ks3.ACL(ks3.ACLPublicReadWrite))
	if err != nil {
		HandleError(err)
	}

	// Delete bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("CreateBucketSample completed")
}
