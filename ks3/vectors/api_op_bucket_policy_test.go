package vectors

import (
	"net/http"
	"strings"

	. "gopkg.in/check.v1"
)

// TestPutVectorBucketPolicy 校验设置向量桶策略的请求构造与参数校验。
func (s *Ks3VectorsClientSuite) TestPutVectorBucketPolicy(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// 正常：请求构造
	res, err := vc.PutVectorBucketPolicy(&PutVectorBucketPolicyRequest{
		VectorBucketName: strPtr("test-vector-bucket"),
		Policy:           strPtr(`{"Version":"2025-12-01","Statement":[]}`),
	})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/PutVectorBucketPolicy")
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"test-vector-bucket"`), Equals, true)
	c.Assert(strings.Contains(gotBody, `"policy":"`), Equals, true)

	// 校验：缺 policy 报错且不发包
	_, err = vc.PutVectorBucketPolicy(&PutVectorBucketPolicyRequest{VectorBucketName: strPtr("bkt")})
	c.Assert(err, NotNil)
}

// TestGetVectorBucketPolicy 校验查询向量桶策略的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestGetVectorBucketPolicy(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, Equals, "/GetVectorBucketPolicy")
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"vectorBucketName":"test-vector-bucket"`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"policy":"{\"Version\":\"2025-12-01\",\"Statement\":[]}"}`))
	})
	defer srv.Close()

	res, err := vc.GetVectorBucketPolicy(&GetVectorBucketPolicyRequest{VectorBucketName: strPtr("test-vector-bucket")})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	// policy 为 JSON 格式的策略字符串，校验完整解析
	c.Assert(res.Policy, Equals, `{"Version":"2025-12-01","Statement":[]}`)
}

// TestDeleteVectorBucketPolicy 校验删除向量桶策略的请求构造。
func (s *Ks3VectorsClientSuite) TestDeleteVectorBucketPolicy(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	res, err := vc.DeleteVectorBucketPolicy(&DeleteVectorBucketPolicyRequest{VectorBucketName: strPtr("test-vector-bucket")})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(gotPath, Equals, "/DeleteVectorBucketPolicy")
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"test-vector-bucket"`), Equals, true)
}
