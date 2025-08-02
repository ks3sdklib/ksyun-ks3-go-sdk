package sample

import (
	"fmt"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// BucketWebsiteSample shows how to set, get and delete the bucket website.
func BucketWebsiteSample() {
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

	//Define bucket website indexWebsite or errorWebsite
	var indexWebsite = "myindex.html"
	var errorWebsite = "myerror.html"

	// Set bucket website indexWebsite or errorWebsite
	err = client.SetBucketWebsite(bucketName, indexWebsite, errorWebsite)
	if err != nil {
		HandleError(err)
	}

	bEnable := true
	bDisable := false

	// Define one website detail
	ruleOk := ks3.RoutingRule{
		RuleNumber: 1,
		Condition: ks3.Condition{
			KeyPrefixEquals:             "abc",
			HTTPErrorCodeReturnedEquals: 404,
			IncludeHeader: []ks3.IncludeHeader{
				ks3.IncludeHeader{
					Key:    "host",
					Equals: "test.ks3-cn-beijing-internal.ksyuncs.com",
				},
			},
		},
		Redirect: ks3.Redirect{
			RedirectType:          "Mirror",
			PassQueryString:       &bDisable,
			MirrorURL:             "http://www.test.com/",
			MirrorPassQueryString: &bEnable,
			MirrorFollowRedirect:  &bEnable,
			MirrorCheckMd5:        &bDisable,
			MirrorHeaders: ks3.MirrorHeaders{
				PassAll: &bEnable,
				Pass:    []string{"key1", "key2"},
				Remove:  []string{"remove1", "remove2"},
				Set: []ks3.MirrorHeaderSet{
					ks3.MirrorHeaderSet{
						Key:   "setKey1",
						Value: "setValue1",
					},
				},
			},
		},
	}
	wxmlDetail := ks3.WebsiteXML{}
	wxmlDetail.RoutingRules = append(wxmlDetail.RoutingRules, ruleOk)

	// Get website
	res, err := client.GetBucketWebsite(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Website IndexDocument:", res.IndexDocument.Suffix)

	// Set bucket website detail
	err = client.SetBucketWebsiteDetail(bucketName, wxmlDetail)
	if err != nil {
		HandleError(err)
	}
	// Get website Detail
	res, err = client.GetBucketWebsite(bucketName)
	if err != nil {
		HandleError(err)
	}
	fmt.Println("Website Redirect type:", res.RoutingRules[0].Redirect.RedirectType)

	// Delete Website
	err = client.DeleteBucketWebsite(bucketName)
	if err != nil {
		HandleError(err)
	}

	// Delete bucket
	err = client.DeleteBucket(bucketName)
	if err != nil {
		HandleError(err)
	}

	fmt.Println("BucketWebsiteSample completed")
}
