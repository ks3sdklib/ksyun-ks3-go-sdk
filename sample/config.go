package sample

import "os"

var (
	// Sample code's env configuration. You need to specify them with the actual configuration if you want to run sample code
	endpoint   = os.Getenv("KS3_TEST_ENDPOINT")
	accessID   = os.Getenv("KS3_TEST_ACCESS_KEY_ID")
	accessKey  = os.Getenv("KS3_TEST_ACCESS_KEY_SECRET")
	bucketName = os.Getenv("KS3_TEST_BUCKET")
	kmsID      = os.Getenv("KS3_TEST_KMS_ID")
	accountID  = os.Getenv("KS3_TEST_ACCOUNT_ID")
	stsARN     = os.Getenv("KS3_TEST_STS_ARN")

	// Credential
	credentialAccessID  = os.Getenv("KS3_CREDENTIAL_KEY_ID")
	credentialAccessKey = os.Getenv("KS3_CREDENTIAL_KEY_SECRET")
	credentialUID       = os.Getenv("KS3_CREDENTIAL_UID")

	// The cname endpoint
	endpoint4Cname = os.Getenv("KS3_TEST_CNAME_ENDPOINT")
)

const (

	// The object name in the sample code
	objectKey       string = "my-object"
	appendObjectKey string = "my-object-append"

	// The local files to run sample code.
	localFile          string = "sample/BingWallpaper-2015-11-07.jpg"
	localCsvFile       string = "sample/sample_data.csv"
	localJSONFile      string = "sample/sample_json.json"
	localJSONLinesFile string = "sample/sample_json_lines.json"
	htmlLocalFile      string = "sample/The Go Programming Language.html"
)
