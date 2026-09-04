// Package vectors 提供 KS3 向量桶（VectorBucket）服务客户端。
//
// 向量桶接口与标准对象存储不同：请求路径为字面量（如 /CreateVectorBucket，桶名在 JSON body 内），
// 请求体与响应体均为 JSON。VectorsClient 复用 ks3.Client 的签名与 HTTP 链路，
// 仅替换 endpoint、签名 service（ks3vectors）与序列化方式。
package vectors

import (
	"fmt"
	"net/url"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// VectorsClient 是 KS3 向量桶服务客户端。
type VectorsClient struct {
	client *ks3.Client
}

// NewVectorsClient 创建向量桶客户端。
//
// ak/sk 为凭证，region 为 V4 签名 scope（大写，如 BEIJING），endpoint 为向量桶请求 host（如 http://ks3vectors-cn-beijing.ksyuncs.com）。
// opts 复用 ks3.ClientOption 传额外配置（如 ks3.SetLogLevel(ks3.Debug)），签名版本（V4）、签名 service（ks3vectors）
// 由本函数内部追加，调用方无需关心；其余字段由 ks3.New 补默认。
func NewVectorsClient(ak, sk, region, endpoint string, opts ...ks3.ClientOption) (*VectorsClient, error) {
	if ak == "" || sk == "" {
		return nil, fmt.Errorf("vectors: requires AccessKeyID and AccessKeySecret")
	}
	if region == "" {
		return nil, fmt.Errorf("vectors: requires region")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("vectors: requires endpoint")
	}

	// 向量桶专用 option 放在用户 opts 之后，确保最终生效。
	internalOpts := []ks3.ClientOption{
		ks3.Region(region),
		ks3.ServiceName("ks3vectors"),
		ks3.AuthVersion(ks3.AuthV4),
		ks3.EnableCRC(false),
		ks3.EnableMD5(false),
	}
	client, err := ks3.New(endpoint, ak, sk, append(opts, internalOpts...)...)
	if err != nil {
		return nil, err
	}
	return &VectorsClient{client: client}, nil
}

// buildURI 构造字面路径请求的 URL，scheme/host 取自已解析的向量桶 endpoint。
func (vc *VectorsClient) buildURI(path string) *url.URL {
	um := vc.client.Conn.Url
	return &url.URL{
		Scheme: um.Scheme,
		Host:   um.NetLoc,
		Path:   path,
	}
}

// checkVectorBucketName 校验向量桶名：3-63 字符、仅 a-z/0-9/-、字母数字开头结尾。
// 比 ks3.CheckBucketName 严格（不允许下划线，且校验连字符首尾）。
func checkVectorBucketName(name string) error {
	n := len(name)
	if n < 3 || n > 63 {
		return fmt.Errorf("vectors: bucket name %q length must be 3-63, now is %d", name, n)
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return fmt.Errorf("vectors: bucket name %q can only contain lowercase letters, digits and '-'", name)
		}
	}
	if !isLowerAlnum(name[0]) || !isLowerAlnum(name[n-1]) {
		return fmt.Errorf("vectors: bucket name %q must start and end with a lowercase letter or digit", name)
	}
	return nil
}

func isLowerAlnum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}
