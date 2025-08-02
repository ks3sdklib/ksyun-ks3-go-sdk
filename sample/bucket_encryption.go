package sample

import (
	"fmt"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// BucketEncryptionSample shows how to get and set the bucket encryption Algorithm
func BucketEncryptionSample() {
	// New client
	client, err := ks3.New(endpoint, accessID, accessKey)
	if err != nil {
		HandleError(err)
	}

	// Create a bucket with default parameters
	err = client.CreateBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// SetBucketEncryption:AES256 ,"123"
	encryptionRule := ks3.ServerEncryptionRule{}
	encryptionRule.SSEDefault.SSEAlgorithm = string(ks3.AESAlgorithm)
	err = client.SetBucketEncryption(bucketName, encryptionRule)
	if err != nil {
		HandleError(err)
	}

	// Get bucket encryption
	encryptionResult, err := client.GetBucketEncryption(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Bucket Encryption:", encryptionResult)

	// Delete the bucket
	err = client.DeleteBucketEncryption(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Delete the object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("BucketEncryptionSample completed")
}
