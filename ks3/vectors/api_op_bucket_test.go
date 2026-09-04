package vectors

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	. "gopkg.in/check.v1"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// TestCheckVectorBucketName 校验向量桶名校验逻辑。
func (s *Ks3VectorsClientSuite) TestCheckVectorBucketName(c *C) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"abc", true},
		{"a-b", true},
		{"a1b", true},
		{"123", true},
		{"ab", false},                    // 太短
		{strings.Repeat("a", 64), false}, // 太长
		{"Abc", false},                   // 大写
		{"a_b", false},                   // 下划线
		{"-abc", false},                  // 以 - 开头
		{"abc-", false},                  // 以 - 结尾
		{"a.c", false},                   // 点号
		{"a b", false},                   // 空格
	}
	for _, tc := range cases {
		err := checkVectorBucketName(tc.name)
		c.Assert((err == nil) == tc.ok, Equals, true, Commentf("checkVectorBucketName(%q) ok=%v err=%v", tc.name, tc.ok, err))
	}
}

// TestNewVectorsClient 校验客户端构造与参数校验。
func (s *Ks3VectorsClientSuite) TestNewVectorsClient(c *C) {
	_, err := NewVectorsClient("", "", "BEIJING", "http://example.com")
	c.Assert(err, NotNil)
	_, err = NewVectorsClient("ak", "sk", "", "http://example.com")
	c.Assert(err, NotNil)
	_, err = NewVectorsClient("ak", "sk", "BEIJING", "")
	c.Assert(err, NotNil)

	// 正常创建：断言向量桶专用配置已就位
	vc, err := NewVectorsClient("ak", "sk", "BEIJING", "http://example.com")
	c.Assert(err, IsNil)
	c.Assert(vc.client.Config.ServiceName, Equals, "ks3vectors")
	c.Assert(vc.client.Config.AuthVersion, Equals, ks3.AuthV4)
	c.Assert(vc.client.Config.Region, Equals, "BEIJING")
	c.Assert(vc.client.Config.Endpoint, Equals, "http://example.com")
	c.Assert(vc.client.Config.IsEnableCRC, Equals, false)
	c.Assert(vc.client.Config.IsEnableMD5, Equals, false)
}

// TestCreateVectorBucket 校验创建向量桶的请求构造、响应解析与错误处理。
func (s *Ks3VectorsClientSuite) TestCreateVectorBucket(c *C) {
	var gotPath, gotCT, gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("x-kss-request-id", "req-123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"vectorBucketKrn":"krn:ks3:vectors:cn-beijing:test-vector-bucket"}`))
	}))
	defer srv.Close()

	// 用指向 mock 的 ks3 client 直接构造 VectorsClient（绕过 NewVectorsClient 的 endpoint 推导）
	client, err := ks3.New(srv.URL, "ak", "sk",
		ks3.AuthVersion(ks3.AuthV4), ks3.Region("BEIJING"), ks3.ServiceName("ks3vectors"),
		ks3.EnableCRC(false), ks3.EnableMD5(false))
	c.Assert(err, IsNil)
	vc := &VectorsClient{client: client}

	// 正常：请求构造与响应解析
	result, err := vc.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr("test-vector-bucket")})
	c.Assert(err, IsNil)
	c.Assert(result.VectorBucketKrn, Equals, "krn:ks3:vectors:cn-beijing:test-vector-bucket")
	c.Assert(gotPath, Equals, "/CreateVectorBucket")
	c.Assert(gotCT, Equals, "application/json")
	c.Assert(strings.HasPrefix(gotAuth, "KSS4-HMAC-SHA256"), Equals, true)
	c.Assert(strings.Contains(gotBody, "test-vector-bucket"), Equals, true)

	// opts：传 ContentType 覆盖默认请求头，验证 opts 生效
	_, err = vc.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr("test-vector-bucket")}, ks3.ContentType("application/json; charset=utf-8"))
	c.Assert(err, IsNil)
	c.Assert(gotCT, Equals, "application/json; charset=utf-8")

	// 传 nil 不 panic，缺必填字段友好报错
	_, err = vc.CreateVectorBucket(nil)
	c.Assert(err, NotNil)

	// 错误：4xx 响应解析为 VectorServiceError（向量桶错误体为 JSON）
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-kss-request-id", "req-err")
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"bucket already exists"}`))
	}))
	defer srv2.Close()
	errClient, err := ks3.New(srv2.URL, "ak", "sk",
		ks3.AuthVersion(ks3.AuthV4), ks3.Region("BEIJING"), ks3.ServiceName("ks3vectors"),
		ks3.EnableCRC(false), ks3.EnableMD5(false))
	c.Assert(err, IsNil)
	errVC := &VectorsClient{client: errClient}
	_, err = errVC.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr("test-vector-bucket")})
	c.Assert(err, NotNil)
	// 向量桶 JSON 错误解析为 VectorServiceError（非 ServiceError）
	se, ok := err.(ks3.VectorServiceError)
	c.Assert(ok, Equals, true)
	c.Assert(se.Message, Equals, "bucket already exists")
	c.Assert(se.RequestID, Equals, "req-err")
	c.Assert(se.FieldList, HasLen, 0)

	// 校验：响应体反序列化
	var r CreateVectorBucketResult
	err = json.Unmarshal([]byte(`{"vectorBucketKrn":"krn:ks3:vectors:cn-beijing:test-bucket"}`), &r)
	c.Assert(err, IsNil)
	c.Assert(r.VectorBucketKrn, Equals, "krn:ks3:vectors:cn-beijing:test-bucket")
}

// TestVectorServiceErrorFieldList 校验向量桶参数校验错误（带 fieldList）解析为 VectorServiceError。
func (s *Ks3VectorsClientSuite) TestVectorServiceErrorFieldList(c *C) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-kss-request-id", "req-field")
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Invalid index ARN/KRN","fieldList":[{"message":"The indexArn/indexKrn is invalid.","path":"/indexKrn"}]}`))
	}))
	defer srv.Close()

	client, err := ks3.New(srv.URL, "ak", "sk",
		ks3.AuthVersion(ks3.AuthV4), ks3.Region("BEIJING"), ks3.ServiceName("ks3vectors"),
		ks3.EnableCRC(false), ks3.EnableMD5(false))
	c.Assert(err, IsNil)
	vc := &VectorsClient{client: client}

	_, err = vc.GetVectors(&GetVectorsRequest{
		IndexKrn: strPtr("invalid-krn"),
		Keys:     []string{"k1"},
	})
	c.Assert(err, NotNil)
	se, ok := err.(ks3.VectorServiceError)
	c.Assert(ok, Equals, true)
	c.Assert(se.Message, Equals, "Invalid index ARN/KRN")
	c.Assert(se.FieldList, HasLen, 1)
	c.Assert(se.FieldList[0].Message, Equals, "The indexArn/indexKrn is invalid.")
	c.Assert(se.FieldList[0].Path, Equals, "/indexKrn")
	c.Assert(se.RequestID, Equals, "req-field")
}

// TestDeleteVectorBucket 校验删除向量桶的请求构造（按名称与按 KRN）。
func (s *Ks3VectorsClientSuite) TestDeleteVectorBucket(c *C) {
	var gotPath, gotBody string
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = readBody(c, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// 用 vectorBucketName
	res, err := vc.DeleteVectorBucket(&DeleteVectorBucketRequest{VectorBucketName: strPtr("test-bucket")})
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	c.Assert(res.Headers, NotNil)
	c.Assert(gotPath, Equals, "/DeleteVectorBucket")
	c.Assert(strings.Contains(gotBody, `"vectorBucketName":"test-bucket"`), Equals, true)

	// 用 vectorBucketKrn
	_, err = vc.DeleteVectorBucket(&DeleteVectorBucketRequest{VectorBucketKrn: strPtr("krn:ksc:ks3vectors:beijing:1:bucket/test-bucket")})
	c.Assert(err, IsNil)

	// 传 nil 不 panic，发空 body 给服务端裁决
	_, err = vc.DeleteVectorBucket(nil)
	c.Assert(err, IsNil)
}

// TestGetVectorBucket 校验获取向量桶详情的请求构造与响应解析。
func (s *Ks3VectorsClientSuite) TestGetVectorBucket(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, Equals, "/GetVectorBucket")
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"vectorBucketName":"test-bucket"`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectorBucket":{"creationTime":1755127842368,"vectorBucketKrn":"krn:ksc:ks3vectors:beijing:1:bucket/test-bucket","vectorBucketName":"test-bucket","location":"beijing"}}`))
	})
	defer srv.Close()

	result, err := vc.GetVectorBucket(&GetVectorBucketRequest{VectorBucketName: strPtr("test-bucket")})
	c.Assert(err, IsNil)
	c.Assert(result.StatusCode, Equals, http.StatusOK)
	c.Assert(result.VectorBucket, NotNil)
	// 校验响应所有字段（真实 API 返回 4 个字段）
	c.Assert(*result.VectorBucket.VectorBucketName, Equals, "test-bucket")
	c.Assert(*result.VectorBucket.VectorBucketKrn, Equals, "krn:ksc:ks3vectors:beijing:1:bucket/test-bucket")
	c.Assert(*result.VectorBucket.Location, Equals, "beijing")
	c.Assert(*result.VectorBucket.CreationTime, Equals, int64(1755127842368))
}

// TestListVectorBuckets 校验列举向量桶的请求构造、响应解析与 nil req 容错。
func (s *Ks3VectorsClientSuite) TestListVectorBuckets(c *C) {
	vc, srv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, Equals, "/ListVectorBuckets")
		body := readBody(c, r.Body)
		c.Assert(strings.Contains(body, `"maxResults":2`), Equals, true)
		c.Assert(strings.Contains(body, `"nextToken":"tok1"`), Equals, true)
		c.Assert(strings.Contains(body, `"prefix":"go-sdk-"`), Equals, true)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nextToken":"token-2","vectorBuckets":[{"creationTime":1735449000,"vectorBucketName":"b1","vectorBucketKrn":"krn:1","location":"beijing"},{"creationTime":1732924800,"vectorBucketName":"b2","vectorBucketKrn":"krn:2","location":"shanghai"}]}`))
	})
	defer srv.Close()

	// 正常：含所有可选字段 maxResults/nextToken/prefix
	maxResults := 2
	result, err := vc.ListVectorBuckets(&ListVectorBucketsRequest{
		MaxResults: &maxResults,
		NextToken:  strPtr("tok1"),
		Prefix:     strPtr("go-sdk-"),
	})
	c.Assert(err, IsNil)
	c.Assert(result.StatusCode, Equals, http.StatusOK)
	c.Assert(*result.NextToken, Equals, "token-2")
	c.Assert(result.VectorBuckets, HasLen, 2)
	// 校验 VectorBucketEntry[0] 所有字段（真实 API 返回 4 个字段）
	e0 := result.VectorBuckets[0]
	c.Assert(*e0.VectorBucketName, Equals, "b1")
	c.Assert(*e0.VectorBucketKrn, Equals, "krn:1")
	c.Assert(*e0.Location, Equals, "beijing")
	c.Assert(*e0.CreationTime, Equals, int64(1735449000))
	// 校验 VectorBucketEntry[1] 部分字段（验证多元素解析）
	c.Assert(*result.VectorBuckets[1].VectorBucketName, Equals, "b2")
	c.Assert(*result.VectorBuckets[1].Location, Equals, "shanghai")

	// 容错：req 为 nil 不 panic，发送空 body（独立 server，handler 不断言 body 内容）
	nilVC, nilSrv := newMockVectorsClient(c, func(w http.ResponseWriter, r *http.Request) {
		_ = readBody(c, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectorBuckets":[]}`))
	})
	defer nilSrv.Close()
	result, err = nilVC.ListVectorBuckets(nil)
	c.Assert(err, IsNil)
	c.Assert(result.VectorBuckets, HasLen, 0)
}
