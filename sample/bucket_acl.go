package sample

import (
	"fmt"

	"github.com/wilac-pv/ksyun-ks3-go-sdk/ks3"
)

// BucketACLSample shows how to get and set the bucket ACL
func BucketACLSample() {
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

	// Set bucket ACL. The valid ACLs are ACLPrivate、ACLPublicRead、ACLPublicReadWrite
	err = client.SetBucketACL(bucketName, ks3.ACLPublicRead)
	if err != nil {
		HandleError(err)
	}

	// Get bucket ACL
	gbar, err := client.GetBucketACL(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Bucket ACL:", gbar.ACL)

	// Delete the bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("BucketACLSample completed")
}
