package sample

import (
	"fmt"
	"strings"

	"github.com/wilac-pv/ksyun-ks3-go-sdk/ks3"
)

// ObjectTaggingSample shows how to set and get object Tagging
func ObjectTaggingSample() {
	// Create bucket
	bucket, err := GetTestBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Create object
	err = bucket.PutObject(objectKey, strings.NewReader("ObjectTaggingSample"))
	if err != nil {
		HandleError(err)
	}

	// Case 1: Set Tagging of object
	tag1 := ks3.Tag{
		Key:   "key1",
		Value: "value1",
	}
	tag2 := ks3.Tag{
		Key:   "key2",
		Value: "value2",
	}
	tagging := ks3.Tagging{
		Tags: []ks3.Tag{tag1, tag2},
	}
	err = bucket.PutObjectTagging(objectKey, tagging)
	if err != nil {
		HandleError(err)
	}

	// Case 2: Get Tagging of object
	taggingResult, err := bucket.GetObjectTagging(objectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Printf("Object Tagging: %v\n", taggingResult)

	tag3 := ks3.Tag{
		Key:   "key3",
		Value: "value3",
	}

	// Case 3: Put object with tagging
	tagging = ks3.Tagging{
		Tags: []ks3.Tag{tag1, tag2, tag3},
	}
	err = bucket.PutObject(objectKey, strings.NewReader("ObjectTaggingSample"), ks3.SetTagging(tagging))
	if err != nil {
		HandleError(err)
	}

	// Case 4: Delete Tagging of object
	err = bucket.DeleteObjectTagging(objectKey)
	if err != nil {
		HandleError(err)
	}

	// Delete object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("ObjectACLSample completed")
}
