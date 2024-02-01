package sample

import (
	"fmt"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// ListBucketsSample shows the list bucket, including default and specified parameters.
func ListBucketsSample() {
	var myBuckets = []string{
		"my-bucket-1",
		"my-bucket-11",
		"my-bucket-2",
		"my-bucket-21",
		"my-bucket-22",
		"my-bucket-3",
		"my-bucket-31",
		"my-bucket-32"}

	// New client
	client, err := ks3.New(endpoint, accessID, accessKey)
	if err != nil {
		HandleError(err)
	}

	// Remove other bucket
	lbr, err := client.ListBuckets()
	if err != nil {
		HandleError(err)
	}

	for _, bucket := range lbr.Buckets {
		err = client.DeleteBucket(bucket.Name)
		if err != nil {
			//HandleError(err)
		}
	}

	// Create bucket
	for _, bucketName := range myBuckets {
		err = client.CreateBucket(bucketName)
		if err != nil {
			HandleError(err)
		}
	}

	// Case 1: Use default parameter
	lbr, err = client.ListBuckets()
	if err != nil {
		HandleError(err)
	}
	fmt.Println("my buckets:", lbr.Buckets)

	// Case 2: Specify the max keys : 3
	lbr, err = client.ListBuckets(ks3.MaxKeys(3))
	if err != nil {
		HandleError(err)
	}
	fmt.Println("my buckets max num:", lbr.Buckets)

	// Case 3: Specify the prefix of buckets.
	lbr, err = client.ListBuckets(ks3.Prefix("my-bucket-2"))
	if err != nil {
		HandleError(err)
	}
	fmt.Println("my buckets prefix :", lbr.Buckets)

	// Case 4: Specify the marker to return from a certain one
	lbr, err = client.ListBuckets(ks3.Marker("my-bucket-22"))
	if err != nil {
		HandleError(err)
	}
	fmt.Println("my buckets marker :", lbr.Buckets)

	// Case 5: Specify max key and list all buckets with paging, return 3 items each time.
	marker := ks3.Marker("")
	for {
		lbr, err = client.ListBuckets(ks3.MaxKeys(3), marker)
		if err != nil {
			HandleError(err)
		}
		marker = ks3.Marker(lbr.NextMarker)
		fmt.Println("my buckets page :", lbr.Buckets)
		if !lbr.IsTruncated {
			break
		}
	}

	// Case 6: List bucket with marker and max key; return 3 items each time.
	marker = ks3.Marker("my-bucket-22")
	for {
		lbr, err = client.ListBuckets(ks3.MaxKeys(3), marker)
		if err != nil {
			HandleError(err)
		}
		marker = ks3.Marker(lbr.NextMarker)
		fmt.Println("my buckets marker&page :", lbr.Buckets)
		if !lbr.IsTruncated {
			break
		}
	}

	// Case 7: List bucket with prefix and max key, return 3 items each time.
	pre := ks3.Prefix("my-bucket-2")
	marker = ks3.Marker("")
	for {
		lbr, err = client.ListBuckets(ks3.MaxKeys(3), pre, marker)
		if err != nil {
			HandleError(err)
		}
		pre = ks3.Prefix(lbr.Prefix)
		marker = ks3.Marker(lbr.NextMarker)
		fmt.Println("my buckets prefix&page :", lbr.Buckets)
		if !lbr.IsTruncated {
			break
		}
	}

	// Delete bucket
	for _, bucketName := range myBuckets {
		err = client.DeleteBucket(bucketName)
		if err != nil {
			HandleError(err)
		}
	}

	fmt.Println("ListsBucketSample completed")
}
