package sample

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// PutObjectSample illustrates two methods for uploading a file: simple upload and multipart upload.
func PutObjectSample() {
	// Create bucket
	bucket, err := GetTestBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	var val = "一代天骄，成吉思汗，只识弯弓射大雕。"

	// Case 1: Upload an object from a string
	err = bucket.PutObject(objectKey, strings.NewReader(val))
	if err != nil {
		HandleError(err)
	}

	// Case 2: Upload an object whose value is a byte[]
	err = bucket.PutObject(objectKey, bytes.NewReader([]byte(val)))
	if err != nil {
		HandleError(err)
	}

	// Case 3: Upload the local file with file handle, user should open the file at first.
	fd, err := os.Open(localFile)
	if err != nil {
		HandleError(err)
	}
	defer fd.Close()

	err = bucket.PutObject(objectKey, fd)
	if err != nil {
		HandleError(err)
	}

	// Case 4: Upload an object with local file name, user need not open the file.
	err = bucket.PutObjectFromFile(objectKey, localFile)
	if err != nil {
		HandleError(err)
	}

	// Case 5: Upload an object with specified properties, PutObject/PutObjectFromFile/UploadFile also support this feature.
	options := []ks3.Option{
		ks3.Expires(futureDate),
		ks3.ObjectACL(ks3.ACLPublicRead),
		ks3.Meta("myprop", "mypropval"),
	}
	err = bucket.PutObject(objectKey, strings.NewReader(val), options...)
	if err != nil {
		HandleError(err)
	}

	props, err := bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Object Meta:", props)

	// Case 6: Upload an object with sever side encrpytion kms and kms id specified
	err = bucket.PutObject(objectKey, strings.NewReader(val), ks3.ServerSideEncryption("KMS"), ks3.ServerSideEncryptionKeyID(kmsID))
	if err != nil {
		HandleError(err)
	}

	// Case 7: Upload an object with callback
	callbackMap := map[string]string{}
	callbackMap["callbackUrl"] = "http://ks3-demo.ksyuncs.com:23450"
	callbackMap["callbackHost"] = "ks3-cn-hangzhou.ksyuncs.com"
	callbackMap["callbackBody"] = "filename=${object}&size=${size}&mimeType=${mimeType}"
	callbackMap["callbackBodyType"] = "application/x-www-form-urlencoded"

	callbackBuffer := bytes.NewBuffer([]byte{})
	callbackEncoder := json.NewEncoder(callbackBuffer)
	//do not encode '&' to "\u0026"
	callbackEncoder.SetEscapeHTML(false)
	err = callbackEncoder.Encode(callbackMap)
	if err != nil {
		HandleError(err)
	}

	callbackVal := base64.StdEncoding.EncodeToString(callbackBuffer.Bytes())
	err = bucket.PutObject(objectKey, strings.NewReader(val), ks3.Callback(callbackVal))
	if err != nil {
		HandleError(err)
	}

	// Case 8: Big file's multipart upload. It supports concurrent upload with resumable upload.
	// multipart upload with 100K as part size. By default 1 coroutine is used and no checkpoint is used.
	err = bucket.UploadFile(objectKey, localFile, 100*1024)
	if err != nil {
		HandleError(err)
	}

	// Part size is 100K and 3 coroutines are used
	err = bucket.UploadFile(objectKey, localFile, 100*1024, ks3.Routines(3))
	if err != nil {
		HandleError(err)
	}

	// Part size is 100K and 3 coroutines with checkpoint
	err = bucket.UploadFile(objectKey, localFile, 100*1024, ks3.Routines(3), ks3.Checkpoint(true, ""))
	if err != nil {
		HandleError(err)
	}

	// Specify the local file path for checkpoint files.
	// the 2nd parameter of Checkpoint can specify the file path, when the file path is empty, it will upload the directory.
	err = bucket.UploadFile(objectKey, localFile, 100*1024, ks3.Checkpoint(true, localFile+".cp"))
	if err != nil {
		HandleError(err)
	}

	// Case 9: Set the storage classes.KS3 provides three storage classes: Standard, Infrequent Access, and Archive.
	// Supported APIs: PutObject, CopyObject, UploadFile, AppendObject...
	err = bucket.PutObject(objectKey, strings.NewReader(val), ks3.ObjectStorageClass("IA"))
	if err != nil {
		HandleError(err)
	}

	// Upload a local file, and set the object's storage-class to 'Archive'.
	err = bucket.UploadFile(objectKey, localFile, 100*1024, ks3.ObjectStorageClass("Archive"))
	if err != nil {
		HandleError(err)
	}

	// Delete object and bucket
	err = DeleteTestBucketAndObject(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("PutObjectSample completed")
}
