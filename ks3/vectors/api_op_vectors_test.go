package vectors

import (
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

// TestPutVectors 校验写入向量的请求构造与参数校验。
func (s *Ks3VectorsClientSuite) TestPutVectors(c *C) {
	var gotPath string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"key":"k1"`), Equals, true)
		c.Assert(strings.Contains(body, `"float32":[1,2,3]`), Equals, true)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// 正常：请求构造
	res, err := vc.PutVectors(&PutVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		Vectors: []InputVector{{
			Key:      "k1",
			Data:     VectorData{Float32: []float32{1, 2, 3}},
			Metadata: map[string]interface{}{"color": "red"},
		}},
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/PutVectors")

	// 校验：缺 vectors 报错（vectors 为必填，SDK 校验）
	_, err = vc.PutVectors(&PutVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx")})
	c.Assert(err, NotNil)
}

// TestGetVectors 校验获取向量的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestGetVectors(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"keys":["k1","k2"]`), Equals, true)
		c.Assert(strings.Contains(body, `"returnData":true`), Equals, true)
		c.Assert(strings.Contains(body, `"returnMetadata":true`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectors":[{"key":"k1","data":{"float32":[1,2]},"metadata":{"color":"red"}},{"key":"k2","data":{"float32":[3,4]}}]}`))
	})
	defer srv.Close()

	res, err := vc.GetVectors(&GetVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		Keys:             []string{"k1", "k2"},
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.Vectors, HasLen, 2)
	// 校验 OutputVector[0] 所有字段
	v0 := res.Vectors[0]
	c.Assert(v0.Key, Equals, "k1")
	c.Assert(v0.Data.Float32, DeepEquals, []float32{1, 2})
	c.Assert(v0.Metadata, DeepEquals, map[string]interface{}{"color": "red"})
	// 校验 OutputVector[1]：无 metadata
	c.Assert(res.Vectors[1].Key, Equals, "k2")
	c.Assert(res.Vectors[1].Data.Float32, DeepEquals, []float32{3, 4})
	c.Assert(res.Vectors[1].Metadata, IsNil)
}

// TestListVectors 校验列举向量的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestListVectors(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"maxResults":10`), Equals, true)
		c.Assert(strings.Contains(body, `"nextToken":"tok1"`), Equals, true)
		c.Assert(strings.Contains(body, `"returnData":true`), Equals, true)
		c.Assert(strings.Contains(body, `"returnMetadata":true`), Equals, true)
		c.Assert(strings.Contains(body, `"segmentCount":4`), Equals, true)
		c.Assert(strings.Contains(body, `"segmentIndex":3`), Equals, true)
		c.Assert(strings.Contains(body, `"filter":{"color":"red"}`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nextToken":"tok2","vectors":[{"key":"k1","data":{"float32":[1,2]},"metadata":{"color":"red"}}]}`))
	})
	defer srv.Close()

	res, err := vc.ListVectors(&ListVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		MaxResults:       intPtr(10),
		NextToken:        strPtr("tok1"),
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
		SegmentCount:     intPtr(4),
		SegmentIndex:     intPtr(3),
		Filter:           map[string]interface{}{"color": "red"},
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(*res.NextToken, Equals, "tok2")
	c.Assert(res.Vectors, HasLen, 1)
	// 校验 OutputVector[0] 所有字段
	v0 := res.Vectors[0]
	c.Assert(v0.Key, Equals, "k1")
	c.Assert(v0.Data.Float32, DeepEquals, []float32{1, 2})
	c.Assert(v0.Metadata, DeepEquals, map[string]interface{}{"color": "red"})
}

// TestDeleteVectors 校验删除向量的请求构造。
func (s *Ks3VectorsClientSuite) TestDeleteVectors(c *C) {
	var gotPath string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"keys":["k1","k2"]`), Equals, true)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	res, err := vc.DeleteVectors(&DeleteVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		Keys:             []string{"k1", "k2"},
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/DeleteVectors")
}

// TestQueryVectors 校验向量检索的请求构造、响应解析与参数校验。
func (s *Ks3VectorsClientSuite) TestQueryVectors(c *C) {
	var gotPath string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"topK":2`), Equals, true)
		c.Assert(strings.Contains(body, `"returnData":true`), Equals, true)
		c.Assert(strings.Contains(body, `"returnMetadata":true`), Equals, true)
		c.Assert(strings.Contains(body, `"returnDistance":true`), Equals, true)
		c.Assert(strings.Contains(body, `"filter":{"color":"red"}`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectors":[{"key":"k1","data":{"float32":[1,2]},"metadata":{"color":"red"},"distance":0},{"key":"k2","data":{"float32":[3,4]},"distance":8}]}`))
	})
	defer srv.Close()

	// 正常：请求构造与响应解析（路径为小写 queryVectors，含所有可选字段）
	res, err := vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		QueryVector:      VectorData{Float32: []float32{1, 2}},
		TopK:             intPtr(2),
		Filter:           map[string]interface{}{"color": "red"},
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
		ReturnDistance:   boolPtr(true),
	})
	c.Assert(err, IsNil)
	c.Assert(gotPath, Equals, "/queryVectors")
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.Vectors, HasLen, 2)
	// 校验 QueryOutputVector[0] 所有字段
	v0 := res.Vectors[0]
	c.Assert(v0.Key, Equals, "k1")
	c.Assert(v0.Data.Float32, DeepEquals, []float32{1, 2})
	c.Assert(v0.Metadata, DeepEquals, map[string]interface{}{"color": "red"})
	c.Assert(*v0.Distance, Equals, float64(0))
	// 校验 QueryOutputVector[1]：无 metadata
	c.Assert(res.Vectors[1].Key, Equals, "k2")
	c.Assert(res.Vectors[1].Data.Float32, DeepEquals, []float32{3, 4})
	c.Assert(*res.Vectors[1].Distance, Equals, float64(8))

	// 校验：topK < 1 报错（topK 为必填，SDK 校验）
	_, err = vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		TopK:             intPtr(0),
	})
	c.Assert(err, NotNil)
}
