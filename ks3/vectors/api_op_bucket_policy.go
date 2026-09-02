package vectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// PutVectorBucketPolicyRequest 设置向量桶策略的请求。
type PutVectorBucketPolicyRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
	// 策略字符串，JSON 格式的策略序列化后的字符串。
	Policy *string `json:"policy"`
}

// PutVectorBucketPolicyResult 设置向量桶策略的响应结果（无响应体，仅状态码与响应头）。
type PutVectorBucketPolicyResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
}

// PutVectorBucketPolicy 设置向量桶的存储桶策略。
func (vc *VectorsClient) PutVectorBucketPolicy(req *PutVectorBucketPolicyRequest, opts ...ks3.Option) (*PutVectorBucketPolicyResult, error) {
	if req == nil {
		req = &PutVectorBucketPolicyRequest{}
	}
	if req.Policy == nil {
		return nil, fmt.Errorf("vectors: requires policy")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/PutVectorBucketPolicy")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &PutVectorBucketPolicyResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}

// GetVectorBucketPolicyRequest 查询向量桶策略的请求。
type GetVectorBucketPolicyRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
}

// GetVectorBucketPolicyResult 查询向量桶策略的响应结果。
type GetVectorBucketPolicyResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 策略字符串，JSON 格式的策略序列化后的字符串。
	Policy string `json:"policy"`
}

// GetVectorBucketPolicy 查询向量桶的存储桶策略。
func (vc *VectorsClient) GetVectorBucketPolicy(req *GetVectorBucketPolicyRequest, opts ...ks3.Option) (*GetVectorBucketPolicyResult, error) {
	if req == nil {
		req = &GetVectorBucketPolicyRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/GetVectorBucketPolicy")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &GetVectorBucketPolicyResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteVectorBucketPolicyRequest 删除向量桶策略的请求。
type DeleteVectorBucketPolicyRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
}

// DeleteVectorBucketPolicyResult 删除向量桶策略的响应结果（无响应体，仅状态码与响应头）。
type DeleteVectorBucketPolicyResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
}

// DeleteVectorBucketPolicy 删除向量桶的存储桶策略。
func (vc *VectorsClient) DeleteVectorBucketPolicy(req *DeleteVectorBucketPolicyRequest, opts ...ks3.Option) (*DeleteVectorBucketPolicyResult, error) {
	if req == nil {
		req = &DeleteVectorBucketPolicyRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/DeleteVectorBucketPolicy")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &DeleteVectorBucketPolicyResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}
