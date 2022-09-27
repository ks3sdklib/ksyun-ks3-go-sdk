package sample

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/wilac-pv/ksyun-ks3-go-sdk/ks3"
)

// SignURLSample signs URL sample
func SignURLSample() {
	// Create bucket
	bucket, err := GetTestBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Put object
	signedURL, err := bucket.SignURL(objectKey, ks3.HTTPPut, 60)
	if err != nil {
		HandleError(err)
	}

	var val = "一代天骄，成吉思汗，只识弯弓射大雕。"
	err = bucket.PutObjectWithURL(signedURL, strings.NewReader(val))
	if err != nil {
		HandleError(err)
	}

	// Put object with option
	options := []ks3.Option{
		ks3.Meta("myprop", "mypropval"),
		ks3.ContentType("image/tiff"),
	}

	signedURL, err = bucket.SignURL(objectKey, ks3.HTTPPut, 60, options...)
	if err != nil {
		HandleError(err)
	}

	err = bucket.PutObjectFromFileWithURL(signedURL, localFile, options...)
	if err != nil {
		HandleError(err)
	}

	// Get object
	signedURL, err = bucket.SignURL(objectKey, ks3.HTTPGet, 60)
	if err != nil {
		HandleError(err)
	}

	body, err := bucket.GetObjectWithURL(signedURL)
	if err != nil {
		HandleError(err)
	}
	defer body.Close()

	// Read content
	data, err := ioutil.ReadAll(body)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}
	fmt.Println("data:", string(data))

	err = bucket.GetObjectToFileWithURL(signedURL, "mynewfile-1.jpg")
	if err != nil {
		HandleError(err)
	}

	// Delete the object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("SignURLSample completed")
}
