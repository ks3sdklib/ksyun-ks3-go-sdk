package vectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// CreateVectorBucketRequest 创建向量桶的请求。
type CreateVectorBucketRequest struct {
	// 向量桶名称。约束：3-63 字符，仅小写字母/数字/连字符，字母数字开头结尾，单账号单 Region 内唯一。
	VectorBucketName *string `json:"vectorBucketName"`
}

// CreateVectorBucketResult 创建向量桶的响应结果。
type CreateVectorBucketResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 创建后返回的向量桶 KRN。
	VectorBucketKrn string `json:"vectorBucketKrn"`
}

// CreateVectorBucket 创建向量存储桶。
func (vc *VectorsClient) CreateVectorBucket(req *CreateVectorBucketRequest, opts ...ks3.Option) (*CreateVectorBucketResult, error) {
	if req == nil {
		req = &CreateVectorBucketRequest{}
	}
	if req.VectorBucketName == nil {
		return nil, fmt.Errorf("vectors: requires vectorBucketName")
	}
	if err := checkVectorBucketName(*req.VectorBucketName); err != nil {
		return nil, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/CreateVectorBucket")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &CreateVectorBucketResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteVectorBucketRequest 删除向量桶的请求。VectorBucketName 与 VectorBucketKrn 二选一。
type DeleteVectorBucketRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
}

// DeleteVectorBucketResult 删除向量桶的响应结果（无响应体，仅状态码与响应头）。
type DeleteVectorBucketResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
}

// DeleteVectorBucket 删除向量存储桶（须先清空桶下所有索引）。
func (vc *VectorsClient) DeleteVectorBucket(req *DeleteVectorBucketRequest, opts ...ks3.Option) (*DeleteVectorBucketResult, error) {
	if req == nil {
		req = &DeleteVectorBucketRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/DeleteVectorBucket")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &DeleteVectorBucketResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}

// GetVectorBucketRequest 获取向量桶详情的请求。VectorBucketName 与 VectorBucketKrn 二选一。
type GetVectorBucketRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
}

// VectorBucket 向量桶信息。
type VectorBucket struct {
	// 创建时间（Unix 毫秒）。
	CreationTime *int64 `json:"creationTime"`
	// 向量桶 KRN。
	VectorBucketKrn *string `json:"vectorBucketKrn"`
	// 向量桶名称。
	VectorBucketName *string `json:"vectorBucketName"`
	// 所属地域。
	Location *string `json:"location"`
}

// GetVectorBucketResult 获取向量桶详情的响应结果。
type GetVectorBucketResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 向量桶详情。
	VectorBucket *VectorBucket `json:"vectorBucket"`
}

// GetVectorBucket 获取向量存储桶的详细信息。
func (vc *VectorsClient) GetVectorBucket(req *GetVectorBucketRequest, opts ...ks3.Option) (*GetVectorBucketResult, error) {
	if req == nil {
		req = &GetVectorBucketRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/GetVectorBucket")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &GetVectorBucketResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// ListVectorBucketsRequest 列举向量桶的请求。
type ListVectorBucketsRequest struct {
	// 返回的最大数量，取值范围 1-500，不设默认 100。
	MaxResults *int `json:"maxResults,omitempty"`
	// 分页令牌，用于获取下一页。
	NextToken *string `json:"nextToken,omitempty"`
	// 只返回名称以该前缀开头的向量桶。
	Prefix *string `json:"prefix,omitempty"`
}

// VectorBucketEntry 列举结果中的向量桶条目。
type VectorBucketEntry struct {
	// 创建时间（Unix 毫秒）。
	CreationTime *int64 `json:"creationTime"`
	// 向量桶 KRN。
	VectorBucketKrn *string `json:"vectorBucketKrn"`
	// 向量桶名称。
	VectorBucketName *string `json:"vectorBucketName"`
	// 所属地域。
	Location *string `json:"location"`
}

// ListVectorBucketsResult 列举向量桶的响应结果。
type ListVectorBucketsResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 分页令牌，用于获取下一页。
	NextToken *string `json:"nextToken"`
	// 向量桶列表。
	VectorBuckets []VectorBucketEntry `json:"vectorBuckets"`
}

// ListVectorBuckets 列举当前账号下全地域的向量桶。
func (vc *VectorsClient) ListVectorBuckets(req *ListVectorBucketsRequest, opts ...ks3.Option) (*ListVectorBucketsResult, error) {
	if req == nil {
		req = &ListVectorBucketsRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/ListVectorBuckets")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &ListVectorBucketsResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}
