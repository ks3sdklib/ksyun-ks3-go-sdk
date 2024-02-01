package sample

import (
	"fmt"
	"strings"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// ObjectMetaSample shows how to get and set the object metadata
func ObjectMetaSample() {
	// Create bucket
	bucket, err := GetTestBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Delete object
	err = bucket.PutObject(objectKey, strings.NewReader("YoursObjectValue"))
	if err != nil {
		HandleError(err)
	}

	// Case 0: Set bucket meta. one or more properties could be set
	// Note: Meta is case insensitive
	options := []ks3.Option{
		ks3.Expires(futureDate),
		ks3.Meta("myprop", "mypropval")}
	err = bucket.SetObjectMeta(objectKey, options...)
	if err != nil {
		HandleError(err)
	}

	// Case 1: Get the object metadata. Only return basic meta information includes ETag, size and last modified.
	props, err := bucket.GetObjectMeta(objectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Object Meta:", props)

	// Case 2: Get all the detailed object meta including custom meta
	props, err = bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Expires:", props.Get("Expires"))

	// Case 3: Get the object's all metadata with contraints. When constraints are met, return the metadata.
	props, err = bucket.GetObjectDetailedMeta(objectKey, ks3.IfUnmodifiedSince(futureDate))
	if err != nil {
		HandleError(err)
	}
	fmt.Println("MyProp:", props.Get("X-Ks3-Meta-Myprop"))

	_, err = bucket.GetObjectDetailedMeta(objectKey, ks3.IfModifiedSince(futureDate))
	if err == nil {
		HandleError(err)
	}

	goar, err := bucket.GetObjectACL(objectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Object ACL:", goar.ACL)

	// Delete object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("ObjectMetaSample completed")
}
