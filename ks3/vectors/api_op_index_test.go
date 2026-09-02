package vectors

import (
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

// TestCreateIndex 校验创建索引的请求构造、响应解析与参数校验。
func (s *Ks3VectorsClientSuite) TestCreateIndex(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, Equals, "/CreateIndex")
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"indexName":"my-index"`), Equals, true)
		c.Assert(strings.Contains(body, `"dimension":1024`), Equals, true)
		c.Assert(strings.Contains(body, `"dataType":"float32"`), Equals, true)
		c.Assert(strings.Contains(body, `"distanceMetric":"euclidean"`), Equals, true)
		c.Assert(strings.Contains(body, `"nonFilterableMetadataKeys":["color","size"]`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"indexKrn":"krn:ksc:ks3vectors:beijing:1:bucket/bkt/index/my-index"}`))
	})
	defer srv.Close()

	// 正常：请求构造与响应解析（含所有可选字段）
	res, err := vc.CreateIndex(&CreateIndexRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("my-index"),
		DataType:         strPtr("float32"),
		Dimension:        intPtr(1024),
		DistanceMetric:   strPtr("euclidean"),
		MetadataConfiguration: &MetadataConfiguration{
			NonFilterableMetadataKeys: []string{"color", "size"},
		},
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.IndexKrn, Equals, "krn:ksc:ks3vectors:beijing:1:bucket/bkt/index/my-index")

	// 校验：缺 indexName 报错（indexName 为必填，SDK 校验）
	_, err = vc.CreateIndex(&CreateIndexRequest{VectorBucketName: strPtr("bkt")})
	c.Assert(err, NotNil)
}

// TestGetIndex 校验获取索引详情的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestGetIndex(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"indexName":"my-index"`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"index":{"creationTime":1735449900,"dataType":"float32","dimension":768,"distanceMetric":"kssine","indexKrn":"krn:1","indexName":"my-index","vectorBucketName":"bkt"}}`))
	})
	defer srv.Close()

	res, err := vc.GetIndex(&GetIndexRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("my-index")})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.Index, NotNil)
	// 校验响应所有字段（真实 API 返回 7 个字段，无 metadataConfiguration）
	idx := res.Index
	c.Assert(*idx.IndexName, Equals, "my-index")
	c.Assert(*idx.IndexKrn, Equals, "krn:1")
	c.Assert(*idx.VectorBucketName, Equals, "bkt")
	c.Assert(*idx.DataType, Equals, "float32")
	c.Assert(*idx.Dimension, Equals, 768)
	c.Assert(*idx.DistanceMetric, Equals, "kssine")
	c.Assert(*idx.CreationTime, Equals, int64(1735449900))
	c.Assert(idx.MetadataConfiguration, IsNil)
}

// TestListIndexes 校验列举索引的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestListIndexes(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"maxResults":100`), Equals, true)
		c.Assert(strings.Contains(body, `"nextToken":"tok1"`), Equals, true)
		c.Assert(strings.Contains(body, `"prefix":"idx"`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nextToken":"tok2","indexes":[{"creationTime":1735449900,"dataType":"float32","dimension":768,"distanceMetric":"euclidean","indexKrn":"krn:1","indexName":"idx1","vectorBucketName":"bkt"},{"creationTime":1731657600,"dataType":"float32","dimension":4,"distanceMetric":"kssine","indexKrn":"krn:2","indexName":"idx2","vectorBucketName":"bkt"}]}`))
	})
	defer srv.Close()

	maxResults := 100
	res, err := vc.ListIndexes(&ListIndexesRequest{
		VectorBucketName: strPtr("bkt"),
		MaxResults:       &maxResults,
		NextToken:        strPtr("tok1"),
		Prefix:           strPtr("idx"),
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(*res.NextToken, Equals, "tok2")
	c.Assert(res.Indexes, HasLen, 2)
	// 校验 IndexEntry[0] 所有字段（真实 API 返回 7 个字段）
	e0 := res.Indexes[0]
	c.Assert(*e0.IndexName, Equals, "idx1")
	c.Assert(*e0.IndexKrn, Equals, "krn:1")
	c.Assert(*e0.VectorBucketName, Equals, "bkt")
	c.Assert(*e0.DataType, Equals, "float32")
	c.Assert(*e0.Dimension, Equals, 768)
	c.Assert(*e0.DistanceMetric, Equals, "euclidean")
	c.Assert(*e0.CreationTime, Equals, int64(1735449900))
}

// TestDeleteIndex 校验删除索引的请求构造与参数校验。
func (s *Ks3VectorsClientSuite) TestDeleteIndex(c *C) {
	var gotPath string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"indexName":"my-index"`), Equals, true)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// 正常：请求构造
	res, err := vc.DeleteIndex(&DeleteIndexRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("my-index")})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/DeleteIndex")
}
