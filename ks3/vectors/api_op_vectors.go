package vectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// VectorData 向量数据，仅支持 float32 类型。
type VectorData struct {
	// 向量值数组。
	Float32 []float32 `json:"float32,omitempty"`
}

// Vector 单条向量数据，用于写入与读取。
type Vector struct {
	// 向量主键，桶内唯一，长度 1-1024，UTF-8。
	Key string `json:"key"`
	// 向量数据，维度与数据类型须与索引一致；未返回数据时为 nil。
	Data *VectorData `json:"data,omitempty"`
	// 向量关联的元数据，可选，支持整数/浮点/字符串/布尔/数组/对象。
	Metadata interface{} `json:"metadata,omitempty"`
}

// PutVectorsRequest 向索引写入向量数据的请求。
type PutVectorsRequest struct {
	// 向量桶名称，与 IndexKrn 二选一时必填。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 IndexName+VectorBucketName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
	// 写入的向量列表，1-500 条。
	Vectors []Vector `json:"vectors"`
}

// PutVectorsResult 写入向量数据的响应结果（无响应体，仅状态码与响应头）。
type PutVectorsResult struct {
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
}

// PutVectors 向索引中写入向量数据。
func (vc *VectorsClient) PutVectors(req *PutVectorsRequest, opts ...ks3.Option) (*PutVectorsResult, error) {
	if req == nil {
		req = &PutVectorsRequest{}
	}
	if len(req.Vectors) == 0 {
		return nil, fmt.Errorf("vectors: requires vectors")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/PutVectors")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &PutVectorsResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}

// GetVectorsRequest 根据 key 获取向量数据的请求。
type GetVectorsRequest struct {
	// 向量桶名称，与 IndexKrn 二选一时必填。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 IndexName+VectorBucketName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
	// 目标向量主键列表，1-100 个。
	Keys []string `json:"keys"`
	// 是否返回向量数据，默认 false。
	ReturnData *bool `json:"returnData,omitempty"`
	// 是否返回向量元数据，默认 false。
	ReturnMetadata *bool `json:"returnMetadata,omitempty"`
}

// GetVectorsResult 根据 key 获取向量数据的响应结果。
type GetVectorsResult struct {
	// 获取到的向量列表。
	Vectors []Vector `json:"vectors"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
}

// GetVectors 根据 key 获取一条或多条向量数据。
func (vc *VectorsClient) GetVectors(req *GetVectorsRequest, opts ...ks3.Option) (*GetVectorsResult, error) {
	if req == nil {
		req = &GetVectorsRequest{}
	}
	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("vectors: requires keys")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/GetVectors")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &GetVectorsResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// ListVectorsRequest 列举向量数据的请求。
type ListVectorsRequest struct {
	// 向量桶名称，与 IndexKrn 二选一时必填。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 IndexName+VectorBucketName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
	// 返回的最大数量，取值范围 1-1000，默认 500。
	MaxResults *int `json:"maxResults,omitempty"`
	// 分页令牌，首次为空。
	NextToken *string `json:"nextToken,omitempty"`
	// 是否返回向量数据，默认 false。
	ReturnData *bool `json:"returnData,omitempty"`
	// 是否返回向量元数据，默认 false。
	ReturnMetadata *bool `json:"returnMetadata,omitempty"`
	// 并行列举的段数，1-16，需与 SegmentIndex 同时指定。
	SegmentCount *int `json:"segmentCount,omitempty"`
	// 列举的段索引，取值范围 1-SegmentCount。
	SegmentIndex *int `json:"segmentIndex,omitempty"`
	// 元数据过滤器。
	Filter interface{} `json:"filter,omitempty"`
}

// ListVectorsResult 列举向量数据的响应结果。
type ListVectorsResult struct {
	// 分页令牌，用于获取下一页，已列举完则为空。
	NextToken *string `json:"nextToken"`
	// 列举出的向量列表。
	Vectors []Vector `json:"vectors"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
}

// ListVectors 列举向量索引中的所有向量数据。
func (vc *VectorsClient) ListVectors(req *ListVectorsRequest, opts ...ks3.Option) (*ListVectorsResult, error) {
	if req == nil {
		req = &ListVectorsRequest{}
	}
	if req.MaxResults != nil && (*req.MaxResults < 1 || *req.MaxResults > 1000) {
		return nil, fmt.Errorf("vectors: maxResults must be in [1, 1000]")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/ListVectors")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &ListVectorsResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteVectorsRequest 删除向量数据的请求。
type DeleteVectorsRequest struct {
	// 向量桶名称，与 IndexKrn 二选一时必填。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 IndexName+VectorBucketName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
	// 要删除的向量主键列表。
	Keys []string `json:"keys"`
}

// DeleteVectorsResult 删除向量数据的响应结果（无响应体，仅状态码与响应头）。
type DeleteVectorsResult struct {
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
}

// DeleteVectors 删除向量索引中的指定向量数据。
func (vc *VectorsClient) DeleteVectors(req *DeleteVectorsRequest, opts ...ks3.Option) (*DeleteVectorsResult, error) {
	if req == nil {
		req = &DeleteVectorsRequest{}
	}
	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("vectors: requires keys")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/DeleteVectors")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &DeleteVectorsResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}

// QueryOutputVector 相似性检索结果中的单条向量。
type QueryOutputVector struct {
	// 向量主键。
	Key string `json:"key"`
	// 向量数据，未返回数据时为 nil。
	Data *VectorData `json:"data,omitempty"`
	// 向量元数据，可选，支持整数/浮点/字符串/布尔/数组/对象。
	Metadata interface{} `json:"metadata,omitempty"`
	// 结果向量与查询向量的相似度距离。
	Distance *float64 `json:"distance,omitempty"`
}

// QueryVectorsRequest 向量相似性检索的请求。
type QueryVectorsRequest struct {
	// 向量桶名称，与 IndexKrn 二选一时必填。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 IndexName+VectorBucketName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
	// 查询向量，维度与数据类型须与索引匹配。
	QueryVector VectorData `json:"queryVector"`
	// 返回的最近邻结果数，取值范围 1-100。
	TopK *int `json:"topK"`
	// 元数据预过滤器，可选。
	Filter interface{} `json:"filter,omitempty"`
	// 是否返回向量数据，默认 false。
	ReturnData *bool `json:"returnData,omitempty"`
	// 是否返回向量元数据，默认 false。
	ReturnMetadata *bool `json:"returnMetadata,omitempty"`
	// 是否返回相似度距离，默认 false。
	ReturnDistance *bool `json:"returnDistance,omitempty"`
}

// QueryVectorsResult 向量相似性检索的响应结果。
type QueryVectorsResult struct {
	// 检索到的向量列表。
	Vectors []QueryOutputVector `json:"vectors"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
}

// QueryVectors 进行向量相似性检索。
func (vc *VectorsClient) QueryVectors(req *QueryVectorsRequest, opts ...ks3.Option) (*QueryVectorsResult, error) {
	if req == nil {
		req = &QueryVectorsRequest{}
	}
	if len(req.QueryVector.Float32) == 0 {
		return nil, fmt.Errorf("vectors: requires queryVector")
	}
	if req.TopK == nil || *req.TopK < 1 || *req.TopK > 100 {
		return nil, fmt.Errorf("vectors: requires topK in [1, 100]")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/QueryVectors")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &QueryVectorsResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}
