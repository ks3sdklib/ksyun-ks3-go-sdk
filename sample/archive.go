package sample

import (
	"fmt"
	"strings"
	"time"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// ArchiveSample archives sample
func ArchiveSample() {
	// Create archive bucket
	client, err := ks3.New(endpoint, accessID, accessKey)
	if err != nil {
		HandleError(err)
	}

	err = client.CreateBucket(bucketName, ks3.StorageClass(ks3.StorageArchive))
	if err != nil {
		HandleError(err)
	}

	archiveBucket, err := client.Bucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Put archive object
	var val = "北方的风光，千里冰封冻，万里雪花飘。"
	err = archiveBucket.PutObject(objectKey, strings.NewReader(val))
	if err != nil {
		HandleError(err)
	}

	// Check whether the object is archive class
	meta, err := archiveBucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		HandleError(err)
	}

	if meta.Get("X-Ks3-Storage-Class") == string(ks3.StorageArchive) {
		// Restore object
		err = archiveBucket.RestoreObject(objectKey)
		if err != nil {
			HandleError(err)
		}

		// Wait for restore completed
		meta, err = archiveBucket.GetObjectDetailedMeta(objectKey)
		for meta.Get("X-Ks3-Restore") == "ongoing-request=\"true\"" {
			fmt.Println("x-ks3-restore:" + meta.Get("X-Ks3-Restore"))
			time.Sleep(1000 * time.Second)
			meta, err = archiveBucket.GetObjectDetailedMeta(objectKey)
		}
	}

	// Get restored object
	err = archiveBucket.GetObjectToFile(objectKey, localFile)
	if err != nil {
		HandleError(err)
	}

	// Restore repeatedly
	err = archiveBucket.RestoreObject(objectKey)

	// Delete object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("ArchiveSample completed")
}
