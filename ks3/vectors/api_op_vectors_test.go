package vectors

import (
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

// TestPutVectors 校验写入向量的请求构造与参数校验。
func (s *Ks3VectorsClientSuite) TestPutVectors(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// 正常：请求构造
	res, err := vc.PutVectors(&PutVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		Vectors: []Vector{{
			Key:      "k1",
			Data:     &VectorData{Float32: []float32{1, 2, 3}},
			Metadata: map[string]interface{}{"color": "red"},
		}},
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/PutVectors")
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"bkt"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"indexName":"idx"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"key":"k1"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"float32":[1,2,3]`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"metadata":{"color":"red"}`), Equals, true)

	// 按 indexKrn 写入
	_, err = vc.PutVectors(&PutVectorsRequest{
		IndexKrn: strPtr("krn:1"),
		Vectors:  []Vector{{Key: "k1", Data: &VectorData{Float32: []float32{1, 2, 3}}}},
	})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"indexKrn":"krn:1"`), Equals, true)

	// 缺 vectors 报错
	_, err = vc.PutVectors(&PutVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx")})
	c.Assert(err, NotNil)
}

// TestGetVectors 校验获取向量的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestGetVectors(c *C) {
	var gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotBody = readBody(c, r.Body)
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
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"bkt"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"indexName":"idx"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"keys":["k1","k2"]`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnData":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnMetadata":true`), Equals, true)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.Vectors, HasLen, 2)
	// 校验 Vector[0] 所有字段
	v0 := res.Vectors[0]
	c.Assert(v0.Key, Equals, "k1")
	c.Assert(v0.Data.Float32, DeepEquals, []float32{1, 2})
	c.Assert(v0.Metadata, DeepEquals, map[string]interface{}{"color": "red"})
	// 校验 Vector[1]：无 metadata
	c.Assert(res.Vectors[1].Key, Equals, "k2")
	c.Assert(res.Vectors[1].Data.Float32, DeepEquals, []float32{3, 4})
	c.Assert(res.Vectors[1].Metadata, IsNil)

	// 按 indexKrn 获取
	_, err = vc.GetVectors(&GetVectorsRequest{IndexKrn: strPtr("krn:1"), Keys: []string{"k1"}})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"indexKrn":"krn:1"`), Equals, true)

	// 缺 keys 报错
	_, err = vc.GetVectors(&GetVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx")})
	c.Assert(err, NotNil)
}

// TestListVectors 校验列举向量的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestListVectors(c *C) {
	var gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotBody = readBody(c, r.Body)
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
	})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"bkt"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"indexName":"idx"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"maxResults":10`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"nextToken":"tok1"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnData":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnMetadata":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"segmentCount":4`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"segmentIndex":3`), Equals, true)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(*res.NextToken, Equals, "tok2")
	c.Assert(res.Vectors, HasLen, 1)
	// 校验 Vector[0] 所有字段
	v0 := res.Vectors[0]
	c.Assert(v0.Key, Equals, "k1")
	c.Assert(v0.Data.Float32, DeepEquals, []float32{1, 2})
	c.Assert(v0.Metadata, DeepEquals, map[string]interface{}{"color": "red"})

	// 按 indexKrn 列举
	_, err = vc.ListVectors(&ListVectorsRequest{IndexKrn: strPtr("krn:1")})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"indexKrn":"krn:1"`), Equals, true)

	// maxResults 越界报错
	_, err = vc.ListVectors(&ListVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx"), MaxResults: intPtr(0)})
	c.Assert(err, NotNil)
	_, err = vc.ListVectors(&ListVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx"), MaxResults: intPtr(1001)})
	c.Assert(err, NotNil)

	// 容错：req 为 nil 不 panic
	nilVC, nilSrv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		_ = readBody(c, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectors":[]}`))
	})
	defer nilSrv.Close()
	res, err = nilVC.ListVectors(nil)
	c.Assert(err, IsNil)
	c.Assert(res.Vectors, HasLen, 0)
}

// TestDeleteVectors 校验删除向量的请求构造。
func (s *Ks3VectorsClientSuite) TestDeleteVectors(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
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
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"bkt"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"indexName":"idx"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"keys":["k1","k2"]`), Equals, true)

	// 按 indexKrn 删除
	_, err = vc.DeleteVectors(&DeleteVectorsRequest{IndexKrn: strPtr("krn:1"), Keys: []string{"k1"}})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"indexKrn":"krn:1"`), Equals, true)

	// 缺 keys 报错
	_, err = vc.DeleteVectors(&DeleteVectorsRequest{VectorBucketName: strPtr("bkt"), IndexName: strPtr("idx")})
	c.Assert(err, NotNil)
}

// TestQueryVectors 校验向量检索的请求构造、响应解析与参数校验。
func (s *Ks3VectorsClientSuite) TestQueryVectors(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectors":[{"key":"k1","data":{"float32":[1,2]},"metadata":{"color":"red"},"distance":0},{"key":"k2","data":{"float32":[3,4]},"distance":8}]}`))
	})
	defer srv.Close()

	// 正常：请求构造与响应解析（含所有可选字段）
	res, err := vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		QueryVector:      VectorData{Float32: []float32{1, 2}},
		TopK:             intPtr(2),
		Filter:           map[string]interface{}{"color": map[string]interface{}{"$eq": "red"}},
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
		ReturnDistance:   boolPtr(true),
	})
	c.Assert(err, IsNil)
	c.Assert(gotPath, Equals, "/QueryVectors")
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"bkt"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"indexName":"idx"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"queryVector":{"float32":[1,2]}`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"topK":2`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnData":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnMetadata":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"returnDistance":true`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"filter":{"color":{"$eq":"red"}}`), Equals, true)
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

	// 按 indexKrn 检索
	_, err = vc.QueryVectors(&QueryVectorsRequest{
		IndexKrn:    strPtr("krn:1"),
		QueryVector: VectorData{Float32: []float32{1, 2}},
		TopK:        intPtr(2),
	})
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(gotBody, `"indexKrn":"krn:1"`), Equals, true)

	// 缺 queryVector 报错
	_, err = vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		TopK:             intPtr(2),
	})
	c.Assert(err, NotNil)

	// topK < 1 报错
	_, err = vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		QueryVector:      VectorData{Float32: []float32{1, 2}},
		TopK:             intPtr(0),
	})
	c.Assert(err, NotNil)

	// topK > 100 报错
	_, err = vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr("bkt"),
		IndexName:        strPtr("idx"),
		QueryVector:      VectorData{Float32: []float32{1, 2}},
		TopK:             intPtr(101),
	})
	c.Assert(err, NotNil)

	// 校验：req 为 nil 报错（topK 必填）
	_, err = vc.QueryVectors(nil)
	c.Assert(err, NotNil)
}
