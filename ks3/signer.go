package ks3

import (
	"bytes"
	"net/http"
	"sort"
)

// signer 抽象签名操作，conn.signHeader/signURL/signPolicyURL 按 AuthVersion
// 在 v2Signer/v4Signer 间分发。三个方法对应请求头签名、预签名 URL、分享外链。
type signer interface {
	signHeader(req *http.Request, ak Credentials, canonicalizedResource string) error
	signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) (string, error)
	signPolicyURL(bucketName string, expiration int64, params map[string]interface{}) (string, error)
}

// headerSorter 对签名头按键升序排序，供 V2 构造 canonicalizedKS3Headers。
type headerSorter struct {
	Keys []string
	Vals []string
}

func newHeaderSorter(m map[string]string) *headerSorter {
	hs := &headerSorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]string, 0, len(m)),
	}

	for k, v := range m {
		hs.Keys = append(hs.Keys, k)
		hs.Vals = append(hs.Vals, v)
	}
	return hs
}

func (hs *headerSorter) Sort() {
	sort.Sort(hs)
}

func (hs *headerSorter) Len() int {
	return len(hs.Vals)
}

func (hs *headerSorter) Less(i, j int) bool {
	return bytes.Compare([]byte(hs.Keys[i]), []byte(hs.Keys[j])) < 0
}

func (hs *headerSorter) Swap(i, j int) {
	hs.Vals[i], hs.Vals[j] = hs.Vals[j], hs.Vals[i]
	hs.Keys[i], hs.Keys[j] = hs.Keys[j], hs.Keys[i]
}

// subResource 是 V2 普通签名纳入的子资源查询参数，对齐 Java RequestUtils.subResource。
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

// queryParam 是 V2 签名纳入的请求覆盖/凭证查询参数，对齐 Java RequestUtils.QueryParam。
var queryParam = []string{
	"response-content-type", "response-content-language",
	"response-expires", "response-cache-control",
	"response-content-disposition", "response-content-encoding",
	"security-token", "X-Kss-Policy",
}

// isParamSign 判定查询参数是否参与 V2 普通签名：对齐 Java BaseV2Signer.encodeParams
// 纳入 subResource ∪ queryParam 的并集。
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

// isPolicyParamSign 判定查询参数是否参与 V2 分享外链签名（queryParam）。
func isPolicyParamSign(paramKey string) bool {
	for _, k := range queryParam {
		if paramKey == k {
			return true
		}
	}
	return false
}
