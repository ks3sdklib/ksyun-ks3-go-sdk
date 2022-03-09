package sample

import (
	"fmt"

	"github.com/chenqichen/ksyun-ks3-go-sdk/ks3"
)

// BucketLifecycleSample shows how to set, get and delete bucket's lifecycle.
func BucketLifecycleSample() {
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

	// Case 1: Set the lifecycle. The rule ID is rule1 and the applied objects' prefix is one and the last modified Date is before 2015/11/11
	expriation := ks3.LifecycleExpiration{
		CreatedBeforeDate: "2015-11-11T00:00:00.000Z",
	}
	rule1 := ks3.LifecycleRule{
		ID:         "rule1",
		Prefix:     "one",
		Status:     "Enabled",
		Expiration: &expriation,
	}
	var rules = []ks3.LifecycleRule{rule1}
	err = client.SetBucketLifecycle(bucketName, rules)
	if err != nil {
		HandleError(err)
	}

	// Case 2: Get the bucket's lifecycle
	lc, err := client.GetBucketLifecycle(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Printf("Bucket Lifecycle:%v, %v\n", lc.Rules, *lc.Rules[0].Expiration)

	// Case 3: Set the lifecycle, The rule ID is rule2 and the applied objects' prefix is two. The object start with the prefix will be transited to IA storage Type 3 days latter, and to archive storage type 30 days latter
	transitionIA := ks3.LifecycleTransition{
		Days:         3,
		StorageClass: ks3.StorageIA,
	}
	transitionArch := ks3.LifecycleTransition{
		Days:         30,
		StorageClass: ks3.StorageArchive,
	}
	rule2 := ks3.LifecycleRule{
		ID:          "rule2",
		Prefix:      "two",
		Status:      "Enabled",
		Transitions: []ks3.LifecycleTransition{transitionIA, transitionArch},
	}
	rules = []ks3.LifecycleRule{rule2}
	err = client.SetBucketLifecycle(bucketName, rules)
	if err != nil {
		HandleError(err)
	}

	// Case 4: Set the lifecycle, The rule ID is rule3 and the applied objects' prefix is three. The object start with the prefix will be transited to IA storage Type 3 days latter, and to archive storage type 30 days latter, the uncompleted multipart upload will be abort 3 days latter.
	abortMPU := ks3.LifecycleAbortMultipartUpload{
		Days: 3,
	}
	rule3 := ks3.LifecycleRule{
		ID:                   "rule3",
		Prefix:               "three",
		Status:               "Enabled",
		AbortMultipartUpload: &abortMPU,
	}
	rules = append(lc.Rules, rule3)
	err = client.SetBucketLifecycle(bucketName, rules)
	if err != nil {
		HandleError(err)
	}

	// Case 5: Set the lifecycle. The rule ID is rule4 and the applied objects' has the tagging which prefix is four and the last modified Date is before 2015/11/11
	expriation = ks3.LifecycleExpiration{
		CreatedBeforeDate: "2015-11-11T00:00:00.000Z",
	}
	tag1 := ks3.Tag{
		Key:   "key1",
		Value: "value1",
	}
	tag2 := ks3.Tag{
		Key:   "key2",
		Value: "value2",
	}
	rule4 := ks3.LifecycleRule{
		ID:         "rule4",
		Prefix:     "four",
		Status:     "Enabled",
		Tags:       []ks3.Tag{tag1, tag2},
		Expiration: &expriation,
	}
	rules = []ks3.LifecycleRule{rule4}
	err = client.SetBucketLifecycle(bucketName, rules)
	if err != nil {
		HandleError(err)
	}

	// Case 6: Delete bucket's Lifecycle
	err = client.DeleteBucketLifecycle(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Delete bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("BucketLifecycleSample completed")
}
