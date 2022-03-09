package sample

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/chenqichen/ksyun-ks3-go-sdk/ks3"
)

// CnameSample shows the cname usage
func CnameSample() {
	// New client
	client, err := ks3.New(endpoint4Cname, accessID, accessKey, ks3.UseCname(true))
	if err != nil {
		HandleError(err)
	}

	// Create bucket
	err = client.CreateBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Set bucket ACL
	err = client.SetBucketACL(bucketName, ks3.ACLPrivate)
	if err != nil {
		HandleError(err)
	}

	// Look up bucket ACL
	gbar, err := client.GetBucketACL(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Bucket ACL:", gbar.ACL)

	// List buckets, the list operation could not be done by cname's endpoint
	_, err = client.ListBuckets()
	if err == nil {
		HandleError(err)
	}

	bucket, err := client.Bucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	objectValue := "君不见黄河之水天上来，奔流到海不复回。君不见高堂明镜悲白发，朝如青丝暮成雪。"

	// Put object
	err = bucket.PutObject(objectKey, strings.NewReader(objectValue))
	if err != nil {
		HandleError(err)
	}

	// Get object
	body, err := bucket.GetObject(objectKey)
	if err != nil {
		HandleError(err)
	}
	data, err := ioutil.ReadAll(body)
	body.Close()
	if err != nil {
		HandleError(err)
	}
	fmt.Println(objectKey, ":", string(data))

	// Put object from file
	err = bucket.PutObjectFromFile(objectKey, localFile)
	if err != nil {
		HandleError(err)
	}

	// Get object to file
	err = bucket.GetObjectToFile(objectKey, localFile)
	if err != nil {
		HandleError(err)
	}

	// List objects
	lor, err := bucket.ListObjects()
	if err != nil {
		HandleError(err)
	}
	fmt.Println("objects:", lor.Objects)

	// Delete object
	err = bucket.DeleteObject(objectKey)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("CnameSample completed")
}
