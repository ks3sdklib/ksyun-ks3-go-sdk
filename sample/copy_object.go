package sample

import (
	"fmt"

	"github.com/chenqichen/ksyun-ks3-go-sdk/ks3"
)

// CopyObjectSample shows the copy files usage
func CopyObjectSample() {
	// Create a bucket
	bucket, err := GetTestBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Create an object
	err = bucket.PutObjectFromFile(objectKey, localFile)
	if err != nil {
		HandleError(err)
	}

	// Case 1: Copy an existing object
	var descObjectKey = "descobject"
	_, err = bucket.CopyObject(objectKey, descObjectKey)
	if err != nil {
		HandleError(err)
	}

	// Case 2: Copy an existing object to another existing object
	_, err = bucket.CopyObject(objectKey, descObjectKey)
	if err != nil {
		HandleError(err)
	}

	err = bucket.DeleteObject(descObjectKey)
	if err != nil {
		HandleError(err)
	}

	// Case 3: Copy file with constraints. When the constraints are met, the copy executes. otherwise the copy does not execute.
	// constraints are not met, copy does not execute
	_, err = bucket.CopyObject(objectKey, descObjectKey, ks3.CopySourceIfModifiedSince(futureDate))
	if err == nil {
		HandleError(err)
	}
	fmt.Println("CopyObjectError:", err)
	// Constraints are met, the copy executes
	_, err = bucket.CopyObject(objectKey, descObjectKey, ks3.CopySourceIfUnmodifiedSince(futureDate))
	if err != nil {
		HandleError(err)
	}

	// Case 4: Specify the properties when copying. The MetadataDirective needs to be MetaReplace
	options := []ks3.Option{
		ks3.Expires(futureDate),
		ks3.Meta("myprop", "mypropval"),
		ks3.MetadataDirective(ks3.MetaReplace)}
	_, err = bucket.CopyObject(objectKey, descObjectKey, options...)
	if err != nil {
		HandleError(err)
	}

	meta, err := bucket.GetObjectDetailedMeta(descObjectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("meta:", meta)

	// Case 5: When the source file is the same as the target file, the copy could be used to update metadata
	options = []ks3.Option{
		ks3.Expires(futureDate),
		ks3.Meta("myprop", "mypropval"),
		ks3.MetadataDirective(ks3.MetaReplace)}

	_, err = bucket.CopyObject(objectKey, objectKey, options...)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("meta:", meta)

	// Case 6: Big file's multipart copy. It supports concurrent copy with resumable upload
	// copy file with multipart. The part size is 100K. By default one routine is used without resumable upload
	err = bucket.CopyFile(bucketName, objectKey, descObjectKey, 100*1024)
	if err != nil {
		HandleError(err)
	}

	// Part size is 100K and three coroutines for the concurrent copy
	err = bucket.CopyFile(bucketName, objectKey, descObjectKey, 100*1024, ks3.Routines(3))
	if err != nil {
		HandleError(err)
	}

	// Part size is 100K and three coroutines for the concurrent copy with resumable upload
	err = bucket.CopyFile(bucketName, objectKey, descObjectKey, 100*1024, ks3.Routines(3), ks3.Checkpoint(true, ""))
	if err != nil {
		HandleError(err)
	}

	// Specify the checkpoint file path. If the checkpoint file path is not specified, the current folder is used.
	err = bucket.CopyFile(bucketName, objectKey, descObjectKey, 100*1024, ks3.Checkpoint(true, localFile+".cp"))
	if err != nil {
		HandleError(err)
	}

	// Case 7: Set the storage classes.KS3 provides three storage classes: Standard, Infrequent Access, and Archive.
	// Copy a object in the same bucket, and set object's storage-class to Archive.
	_, err = bucket.CopyObject(objectKey, objectKey+"DestArchive", ks3.ObjectStorageClass("Archive"))
	if err != nil {
		HandleError(err)
	}

	// Case 8: Copy object with tagging, the value of tagging directive is REPLACE
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
	_, err = bucket.CopyObject(objectKey, objectKey+"WithTagging", ks3.SetTagging(tagging), ks3.TaggingDirective(ks3.TaggingReplace))
	if err != nil {
		HandleError(err)
	}

	// Delete object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("CopyObjectSample completed")
}
