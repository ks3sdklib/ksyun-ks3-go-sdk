package sample

import (
	"fmt"
	"github.com/wilac-pv/ksyun-ks3-go-sdk/ks3"
	"io/ioutil"
	"strings"
)

// BucketrRequestPaymentSample shows how to set, get the bucket request payment.
func BucketrRequestPaymentSample() {
	// New client
	client, err := ks3.New(endpoint, accessID, accessKey)
	if err != nil {
		HandleError(err)
	}

	// Create the bucket with default parameters
	err = client.CreateBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	reqPayConf := ks3.RequestPaymentConfiguration{
		Payer: string(ks3.Requester),
	}

	// Case 1: Set bucket request payment.
	err = client.SetBucketRequestPayment(bucketName, reqPayConf)
	if err != nil {
		HandleError(err)
	}

	// Get bucket request payment configuration
	ret, err := client.GetBucketRequestPayment(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Bucket request payer:", ret.Payer)

	if credentialUID == "" {
		fmt.Println("Please enter a credential User ID, if you want to test credential user.")
		clearData(client, bucketName)
		return
	}
	// Credential other User
	policyInfo := `
	{
		"Version":"1",
		"Statement":[
			{
				"Action":[
					"ks3:*"
				],
				"Effect":"Allow",
				"Principal":["` + credentialUID + `"],
				"Resource":["acs:ks3:*:*:` + bucketName + `", "acs:ks3:*:*:` + bucketName + `/*"]
			}
		]
	}`

	err = client.SetBucketPolicy(bucketName, policyInfo)
	if err != nil {
		HandleError(err)
	}

	// New a Credential client
	creClient, err := ks3.New(endpoint, credentialAccessID, credentialAccessKey)
	if err != nil {
		HandleError(err)
	}

	// Get credential bucket
	creBucket, err := creClient.Bucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Put object by credential User
	key := "testCredentialObject"
	objectValue := "this is a test string."
	// Put object
	err = creBucket.PutObject(key, strings.NewReader(objectValue), ks3.RequestPayer(ks3.Requester))
	if err != nil {
		HandleError(err)
	}
	// Get object
	body, err := creBucket.GetObject(key, ks3.RequestPayer(ks3.Requester))
	if err != nil {
		HandleError(err)
	}
	defer body.Close()

	data, err := ioutil.ReadAll(body)
	if err != nil {
		HandleError(err)
	}
	fmt.Println(string(data))

	// Delete object
	err = creBucket.DeleteObject(key, ks3.RequestPayer(ks3.Requester))
	if err != nil {
		HandleError(err)
	}

	clearData(client, bucketName)
}

func clearData(client *ks3.Client, bucketName string) {
	// Delete bucket
	err := client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("BucketrRequestPaymentSample completed")
}
