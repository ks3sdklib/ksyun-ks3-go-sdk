package ks3

import (
	"net/http"
)

// signer 抽象签名操作，conn.signHeader/signURL/signPolicyURL 按 AuthVersion
// 在 v2Signer/v4Signer 间分发。三个方法对应请求头签名、预签名 URL、分享外链。
type signer interface {
	signHeader(req *http.Request, ak Credentials, canonicalizedResource string) error
	signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) (string, error)
	signPolicyURL(bucketName string, expiration int64, params map[string]interface{}) (string, error)
}

// subResource 是 V2 普通签名纳入的子资源查询参数，isParamSign 据此判定。
var subResource = []string{
	"acl", "lifecycle", "location", "logging",
	"notification", "partNumber", "batchshadowcopy", "encryption",
	"policy", "requestPayment", "torrent", "uploadId",
	"uploads", "versionId", "versioning", "versions",
	"website", "delete", "thumbnail", "cors",
	"crr", "pfop", "querypfop", "adp", "queryadp",
	"fetch", "restore", "transfer", "tagging",
	"mirror", "append", "position", "recover",
	"clear", "retention", "recycle", "x-kss-process",
	"decompresspolicy", "inventory", "id", "accessmonitor",
	"transferAcceleration", "dataRedundancySwitch", "VpcAccessBlock", "migration",
	"bucketqos", "requesterqos", "notification", "jobs",
	"jobId", "action", "priority", "quota",
	"dataRedundancyTransition", "PublicNetworkBlock", "BucketPublicnetworkBlock", "dataAccelerator",
	"worm", "wormId", "wormExtend", "http2",
	"archiveDirectRead",
}

// queryParam 是 V2 签名纳入的请求覆盖/凭证查询参数，isParamSign 据此判定。
var queryParam = []string{
	"response-content-type", "response-content-language",
	"response-expires", "response-cache-control",
	"response-content-disposition", "response-content-encoding",
	"security-token", "X-Kss-Policy",
}

// isParamSign 判定查询参数是否参与 V2 普通签名：纳入 subResource ∪ queryParam 的并集。
func isParamSign(paramKey string) bool {
	for _, k := range subResource {
		if paramKey == k {
			return true
		}
	}
	for _, k := range queryParam {
		if paramKey == k {
			return true
		}
	}
	return false
}
