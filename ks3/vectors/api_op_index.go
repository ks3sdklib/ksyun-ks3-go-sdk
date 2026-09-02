package vectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// MetadataConfiguration 向量索引的元数据配置。
type MetadataConfiguration struct {
	// 不可用于筛选的元数据键列表，最多 10 个，每个键长度 1-63。
	NonFilterableMetadataKeys []string `json:"nonFilterableMetadataKeys,omitempty"`
}

// CreateIndexRequest 创建向量索引的请求。
type CreateIndexRequest struct {
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
	// 索引名称，桶内唯一，3-63 字符，仅小写字母与数字，首字符须为字母。
	IndexName *string `json:"indexName"`
	// 向量数据类型，当前固定 float32。
	DataType *string `json:"dataType,omitempty"`
	// 向量维度，取值范围 1-4096。
	Dimension *int `json:"dimension,omitempty"`
	// 距离度量函数：euclidean（欧氏距离，默认）/ kssine（余弦距离）。
	DistanceMetric *string `json:"distanceMetric,omitempty"`
	// 元数据配置，可选。
	MetadataConfiguration *MetadataConfiguration `json:"metadataConfiguration,omitempty"`
}

// CreateIndexResult 创建向量索引的响应结果。
type CreateIndexResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 创建后返回的索引 KRN。
	IndexKrn string `json:"indexKrn"`
}

// CreateIndex 在向量存储桶中创建向量索引。
func (vc *VectorsClient) CreateIndex(req *CreateIndexRequest, opts ...ks3.Option) (*CreateIndexResult, error) {
	if req == nil {
		req = &CreateIndexRequest{}
	}
	if req.IndexName == nil {
		return nil, fmt.Errorf("vectors: requires indexName")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/CreateIndex")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &CreateIndexResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetIndexRequest 获取向量索引详情的请求。IndexKrn，或 VectorBucketName+IndexName。
type GetIndexRequest struct {
	// 向量桶名称，与 IndexKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 VectorBucketName+IndexName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
}

// Index 向量索引信息。
type Index struct {
	// 创建时间（Unix 毫秒）。
	CreationTime *int64 `json:"creationTime"`
	// 向量数据类型。
	DataType *string `json:"dataType"`
	// 向量维度。
	Dimension *int `json:"dimension"`
	// 距离度量函数。
	DistanceMetric *string `json:"distanceMetric"`
	// 索引 KRN。
	IndexKrn *string `json:"indexKrn"`
	// 索引名称。
	IndexName *string `json:"indexName"`
	// 元数据配置。
	MetadataConfiguration *MetadataConfiguration `json:"metadataConfiguration"`
	// 所属向量桶名称。
	VectorBucketName *string `json:"vectorBucketName"`
}

// GetIndexResult 获取向量索引详情的响应结果。
type GetIndexResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 索引详情。
	Index *Index `json:"index"`
}

// GetIndex 获取向量索引的详细信息。
func (vc *VectorsClient) GetIndex(req *GetIndexRequest, opts ...ks3.Option) (*GetIndexResult, error) {
	if req == nil {
		req = &GetIndexRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/GetIndex")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &GetIndexResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// ListIndexesRequest 列举向量索引的请求。
type ListIndexesRequest struct {
	// 返回的最大数量，取值范围 1-500，不设默认 100。
	MaxResults *int `json:"maxResults,omitempty"`
	// 分页令牌，用于获取下一页。
	NextToken *string `json:"nextToken,omitempty"`
	// 只返回名称以该前缀开头的索引。
	Prefix *string `json:"prefix,omitempty"`
	// 向量桶名称，与 VectorBucketKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 向量桶 KRN，与 VectorBucketName 二选一。
	VectorBucketKrn *string `json:"vectorBucketKrn,omitempty"`
}

// IndexEntry 列举结果中的索引条目。
type IndexEntry struct {
	// 创建时间（Unix 毫秒）。
	CreationTime *int64 `json:"creationTime"`
	// 向量数据类型。
	DataType *string `json:"dataType"`
	// 向量维度。
	Dimension *int `json:"dimension"`
	// 距离度量函数。
	DistanceMetric *string `json:"distanceMetric"`
	// 索引 KRN。
	IndexKrn *string `json:"indexKrn"`
	// 索引名称。
	IndexName *string `json:"indexName"`
	// 所属向量桶名称。
	VectorBucketName *string `json:"vectorBucketName"`
}

// ListIndexesResult 列举向量索引的响应结果。
type ListIndexesResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
	// 分页令牌，用于获取下一页。
	NextToken *string `json:"nextToken"`
	// 索引列表。
	Indexes []IndexEntry `json:"indexes"`
}

// ListIndexes 列举向量桶中的所有向量索引。
func (vc *VectorsClient) ListIndexes(req *ListIndexesRequest, opts ...ks3.Option) (*ListIndexesResult, error) {
	if req == nil {
		req = &ListIndexesRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/ListIndexes")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	result := &ListIndexesResult{}
	result.StatusCode = resp.StatusCode
	result.Headers = resp.Headers
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteIndexRequest 删除向量索引的请求。IndexKrn，或 VectorBucketName+IndexName。
type DeleteIndexRequest struct {
	// 向量桶名称，与 IndexKrn 二选一。
	VectorBucketName *string `json:"vectorBucketName,omitempty"`
	// 索引名称，与 IndexKrn 二选一时必填。
	IndexName *string `json:"indexName,omitempty"`
	// 索引 KRN，与 VectorBucketName+IndexName 二选一。
	IndexKrn *string `json:"indexKrn,omitempty"`
}

// DeleteIndexResult 删除向量索引的响应结果（无响应体，仅状态码与响应头）。
type DeleteIndexResult struct {
	// HTTP 响应状态码。
	StatusCode int `json:"-"`
	// HTTP 响应头。
	Headers http.Header `json:"-"`
}

// DeleteIndex 删除向量索引。
func (vc *VectorsClient) DeleteIndex(req *DeleteIndexRequest, opts ...ks3.Option) (*DeleteIndexResult, error) {
	if req == nil {
		req = &DeleteIndexRequest{}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{ks3.HTTPHeaderContentType: "application/json"}
	if err := ks3.HandleOptions(headers, opts); err != nil {
		return nil, err
	}
	uri := vc.buildURI("/DeleteIndex")
	resp, err := vc.client.DoRaw(nil, http.MethodPost, uri, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &DeleteIndexResult{StatusCode: resp.StatusCode, Headers: resp.Headers}, nil
}
