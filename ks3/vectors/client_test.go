package vectors

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"
)

// Test hooks up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type Ks3VectorsClientSuite struct {
	vc *VectorsClient
	// 套件级共享向量桶，SetUpSuite 创建、TearDownSuite 删除，各用例复用。
	bucket string
	// vectorsReady 为 false 时（缺凭证/region/endpoint）所有用例 skip，避免无凭证环境整包 FAIL。
	vectorsReady bool
}

var _ = Suite(&Ks3VectorsClientSuite{})

var (
	// 复用 ks3 包的凭证环境变量；region/endpoint 为 vectors 独有。
	accessID  = os.Getenv("KS3_TEST_ACCESS_KEY_ID")
	accessKey = os.Getenv("KS3_TEST_ACCESS_KEY_SECRET")
	region    = os.Getenv("KS3_TEST_REGION")
)

// bucketNamePrefix 是 SDK 测试向量桶的统一标识前缀，SetUpSuite/TearDownSuite 只清理此前缀桶，避免误删普通桶。
const bucketNamePrefix = "go-sdk-vectors-test-"

// newMockVectorsClient 起一个 httptest server 并返回指向它的 VectorsClient，用于 mock 单测。
func newMockVectorsClient(c *C, handler http.HandlerFunc) (*VectorsClient, *httptest.Server) {
	srv := httptest.NewServer(handler)
	client, err := ks3.New(srv.URL, "ak", "sk",
		ks3.AuthVersion(ks3.AuthV4), ks3.Region("BEIJING"), ks3.ServiceName("ks3vectors"),
		ks3.EnableCRC(false), ks3.EnableMD5(false))
	c.Assert(err, IsNil)
	return &VectorsClient{client: client}, srv
}

// readBody 读取请求体为字符串。
func readBody(c *C, r io.Reader) string {
	b, err := io.ReadAll(r)
	c.Assert(err, IsNil)
	return string(b)
}

// strPtr 返回字符串指针，用于构造可选字段。
func strPtr(s string) *string { return &s }

// intPtr 返回 int 指针，用于构造可选字段。
func intPtr(i int) *int { return &i }

// boolPtr 返回 bool 指针，用于构造可选字段。
func boolPtr(b bool) *bool { return &b }

// setUpEndpoint 解析 KS3_TEST_VECTORS_ENDPOINT，默认 http://ks3vectors-cn-qingdao.ksyuncs.com。
func setUpEndpoint() string {
	if ep := os.Getenv("KS3_TEST_VECTORS_ENDPOINT"); ep != "" {
		return ep
	}
	return "http://ks3vectors-cn-qingdao.ksyuncs.com"
}

// randLowStr 生成小写字母数字随机串（用于唯一向量桶名），独立于 ks3 包的 RandLowStr。
func randLowStr(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	t := time.Now().UnixNano()
	for i := range b {
		b[i] = letters[t%int64(len(letters))]
		t /= int64(len(letters))
		if t == 0 {
			t = time.Now().UnixNano() + int64(i)
		}
	}
	return string(b)
}

// cleanIndexes 清空指定向量桶下的所有索引（删索引前先清该索引下的向量），用于删桶前清理。
func (s *Ks3VectorsClientSuite) cleanIndexes(c *C, bucketName *string) {
	var nextToken *string
	for {
		listRes, err := s.vc.ListIndexes(&ListIndexesRequest{VectorBucketName: bucketName, NextToken: nextToken})
		if err != nil {
			c.Logf("cleanIndexes list %s failed: %v", *bucketName, err)
			return
		}
		for _, idx := range listRes.Indexes {
			if idx.IndexKrn == nil {
				continue
			}
			// 先清空该索引下的向量（删索引前）
			if idx.IndexName != nil {
				s.cleanVectors(c, bucketName, idx.IndexName)
			}
			c.Logf("cleanIndexes delete: %s", *idx.IndexKrn)
			if _, err := s.vc.DeleteIndex(&DeleteIndexRequest{IndexKrn: idx.IndexKrn}); err != nil {
				c.Logf("cleanIndexes delete %s failed: %v", *idx.IndexKrn, err)
			}
		}
		if listRes.NextToken == nil || *listRes.NextToken == "" {
			return
		}
		nextToken = listRes.NextToken
	}
}

// cleanVectors 清空指定索引下的所有向量（ListVectors 后逐批 DeleteVectors），用于删索引前清理。
func (s *Ks3VectorsClientSuite) cleanVectors(c *C, bucketName, indexName *string) {
	var nextToken *string
	for {
		listRes, err := s.vc.ListVectors(&ListVectorsRequest{
			VectorBucketName: bucketName,
			IndexName:        indexName,
			MaxResults:       intPtr(500),
			NextToken:        nextToken,
		})
		if err != nil {
			// 服务端若未部署 ListVectors 或索引已空，直接返回
			c.Logf("cleanVectors list %s/%s failed: %v", *bucketName, *indexName, err)
			return
		}
		if len(listRes.Vectors) == 0 {
			return
		}
		keys := make([]string, 0, len(listRes.Vectors))
		for _, v := range listRes.Vectors {
			keys = append(keys, v.Key)
		}
		c.Logf("cleanVectors delete %d vectors in %s/%s", len(keys), *bucketName, *indexName)
		if _, err := s.vc.DeleteVectors(&DeleteVectorsRequest{
			VectorBucketName: bucketName,
			IndexName:        indexName,
			Keys:             keys,
		}); err != nil {
			c.Logf("cleanVectors delete failed: %v", err)
		}
		if listRes.NextToken == nil || *listRes.NextToken == "" {
			return
		}
		nextToken = listRes.NextToken
	}
}

// cleanPolicy 删除指定向量桶的策略，用于删桶前清理。
func (s *Ks3VectorsClientSuite) cleanPolicy(c *C, bucketName *string) {
	// 策略可能不存在，Get 失败直接返回；存在则删除
	if _, err := s.vc.GetVectorBucketPolicy(&GetVectorBucketPolicyRequest{VectorBucketName: bucketName}); err != nil {
		return
	}
	if _, err := s.vc.DeleteVectorBucketPolicy(&DeleteVectorBucketPolicyRequest{VectorBucketName: bucketName}); err != nil {
		c.Logf("cleanPolicy delete %s failed: %v", *bucketName, err)
	}
}

// cleanTestBuckets 列举并删除所有 bucketNamePrefix 前缀的测试向量桶（处理分页）。
// 用于 SetUpSuite 清理上次残留、TearDownSuite 清理本次创建。只删前缀桶，不误删普通桶。
func (s *Ks3VectorsClientSuite) cleanTestBuckets(c *C) {
	var nextToken *string
	for {
		listRes, err := s.vc.ListVectorBuckets(&ListVectorBucketsRequest{Prefix: strPtr(bucketNamePrefix), NextToken: nextToken})
		if err != nil {
			c.Logf("cleanTestBuckets list failed: %v", err)
			return
		}
		for _, b := range listRes.VectorBuckets {
			if b.VectorBucketName == nil || !strings.HasPrefix(*b.VectorBucketName, bucketNamePrefix) {
				continue
			}
			name := *b.VectorBucketName
			// 删桶前先清空桶的策略和索引（索引清理含向量），否则删桶失败。
			s.cleanPolicy(c, &name)
			s.cleanIndexes(c, &name)
			c.Logf("cleanTestBuckets delete: %s", name)
			if _, err := s.vc.DeleteVectorBucket(&DeleteVectorBucketRequest{VectorBucketName: &name}); err != nil {
				c.Logf("cleanTestBuckets delete %s failed: %v", name, err)
			}
		}
		if listRes.NextToken == nil || *listRes.NextToken == "" {
			return
		}
		nextToken = listRes.NextToken
	}
}

// SetUpSuite 建向量桶客户端，清理上次残留测试桶，并创建一个套件级共享向量桶供各用例复用。
// 缺凭证/region/endpoint 时置 vectorsReady=false，用例整体 skip。
func (s *Ks3VectorsClientSuite) SetUpSuite(c *C) {
	if accessID == "" || accessKey == "" || region == "" {
		s.vectorsReady = false
		c.Logf("skip vectors suite: KS3_TEST_ACCESS_KEY_ID/SECRET or KS3_TEST_REGION not set")
		return
	}

	vc, err := NewVectorsClient(accessID, accessKey, region, setUpEndpoint(),
		ks3.SetLogLevel(ks3.Debug)) // 打印 v4 canonicalRequest/stringToSign，便于联调签名
	c.Assert(err, IsNil)
	s.vc = vc
	s.vectorsReady = true

	// 清理上次残留的测试桶
	s.cleanTestBuckets(c)

	// 创建套件级共享桶
	s.bucket = bucketNamePrefix + randLowStr(6)
	_, err = s.vc.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr(s.bucket)})
	c.Assert(err, IsNil)
	c.Logf("vectors suite started, shared bucket=%s", s.bucket)
}

// TearDownSuite 清空共享桶下索引后删除共享桶，并清理本 SDK 标识前缀的残留桶。
func (s *Ks3VectorsClientSuite) TearDownSuite(c *C) {
	if !s.vectorsReady {
		return
	}
	// 删共享桶前先清空其策略和索引（索引清理含向量）
	if s.bucket != "" {
		s.cleanPolicy(c, &s.bucket)
		s.cleanIndexes(c, &s.bucket)
		_, err := s.vc.DeleteVectorBucket(&DeleteVectorBucketRequest{VectorBucketName: strPtr(s.bucket)})
		c.Logf("TearDownSuite delete shared bucket %s: %v", s.bucket, err)
	}
	// 兜底清理所有前缀桶（含用例残留）
	s.cleanTestBuckets(c)
	c.Logf("vectors suite completed")
}

// skipIfNotReady 在每个用例开头调用：缺凭证等环境时跳过该用例。
func (s *Ks3VectorsClientSuite) skipIfNotReady(c *C) {
	if !s.vectorsReady {
		c.Skip("vectors credentials/region/endpoint not set")
	}
}

// TestCreateVectorBucketNameInvalid 校验桶名不合法时客户端直接报错（不发包，纯本地）。
func (s *Ks3VectorsClientSuite) TestCreateVectorBucketNameInvalid(c *C) {
	for _, name := range []string{"ab", strings.Repeat("a", 64), "Abc", "a_b", "-abc", "abc-"} {
		_, err := s.vc.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr(name)})
		c.Assert(err, NotNil)
	}
}

// TestVectorBucketIntegration 独立用例：用独立桶测试向量桶的创建、列举、获取、删除（完整生命周期，不影响共享桶）。
func (s *Ks3VectorsClientSuite) TestVectorBucketIntegration(c *C) {
	s.skipIfNotReady(c)

	bucket := bucketNamePrefix + randLowStr(6)
	c.Logf("region=%s bucket=%s", region, bucket)

	// 创建
	createRes, err := s.vc.CreateVectorBucket(&CreateVectorBucketRequest{VectorBucketName: strPtr(bucket)})
	c.Assert(err, IsNil)
	c.Assert(createRes.StatusCode, Equals, http.StatusOK)
	c.Assert(createRes.VectorBucketKrn, Not(Equals), "")
	c.Assert(strings.Contains(createRes.VectorBucketKrn, bucket), Equals, true)

	// 获取：校验所有字段
	getRes, err := s.vc.GetVectorBucket(&GetVectorBucketRequest{VectorBucketName: strPtr(bucket)})
	c.Assert(err, IsNil)
	c.Assert(getRes.StatusCode, Equals, http.StatusOK)
	c.Assert(getRes.VectorBucket, NotNil)
	c.Assert(*getRes.VectorBucket.VectorBucketName, Equals, bucket)
	c.Assert(*getRes.VectorBucket.Location, Equals, "qingdao")
	c.Assert(*getRes.VectorBucket.VectorBucketKrn, Equals, createRes.VectorBucketKrn)
	c.Assert(*getRes.VectorBucket.CreationTime > 0, Equals, true)

	// 列举：含所有可选字段 maxResults/prefix，应能查到并校验条目字段
	maxResults := 100
	listRes, err := s.vc.ListVectorBuckets(&ListVectorBucketsRequest{MaxResults: &maxResults, Prefix: strPtr(bucket)})
	c.Assert(err, IsNil)
	c.Assert(listRes.StatusCode, Equals, http.StatusOK)
	var found *VectorBucketEntry
	for i := range listRes.VectorBuckets {
		if listRes.VectorBuckets[i].VectorBucketName != nil && *listRes.VectorBuckets[i].VectorBucketName == bucket {
			found = &listRes.VectorBuckets[i]
			break
		}
	}
	c.Assert(found, NotNil, Commentf("created bucket %q not found in list", bucket))
	c.Assert(*found.Location, Equals, "qingdao")
	c.Assert(*found.VectorBucketKrn, Equals, createRes.VectorBucketKrn)
	c.Assert(*found.CreationTime > 0, Equals, true)

	// 删除
	delRes, err := s.vc.DeleteVectorBucket(&DeleteVectorBucketRequest{VectorBucketName: strPtr(bucket)})
	c.Assert(err, IsNil)
	c.Assert(delRes.StatusCode, Equals, http.StatusOK)
	c.Logf("DeleteVectorBucket success: %s", bucket)
}

// TestIndexIntegration 用共享桶测试索引的创建、列举、获取、删除。
func (s *Ks3VectorsClientSuite) TestIndexIntegration(c *C) {
	s.skipIfNotReady(c)

	index := "idx" + randLowStr(4)
	c.Logf("bucket=%s index=%s", s.bucket, index)

	// 创建索引（含所有可选字段，MetadataConfiguration 用上 NonFilterableMetadataKeys）
	createRes, err := s.vc.CreateIndex(&CreateIndexRequest{
		VectorBucketName: strPtr(s.bucket),
		DataType:         strPtr("float32"),
		Dimension:        intPtr(1024),
		DistanceMetric:   strPtr("euclidean"),
		IndexName:        strPtr(index),
		MetadataConfiguration: &MetadataConfiguration{
			NonFilterableMetadataKeys: []string{"color"},
		},
	})
	c.Assert(err, IsNil)
	c.Assert(createRes.StatusCode, Equals, http.StatusOK)
	c.Assert(createRes.IndexKrn, Not(Equals), "")
	c.Assert(strings.Contains(createRes.IndexKrn, index), Equals, true)
	c.Logf("CreateIndex success: IndexKrn=%s", createRes.IndexKrn)

	// 获取索引：校验所有字段（dataType/dimension/distanceMetric 用创建值）
	getRes, err := s.vc.GetIndex(&GetIndexRequest{VectorBucketName: strPtr(s.bucket), IndexName: strPtr(index)})
	c.Assert(err, IsNil)
	c.Assert(getRes.StatusCode, Equals, http.StatusOK)
	c.Assert(getRes.Index, NotNil)
	idx := getRes.Index
	c.Assert(*idx.IndexName, Equals, index)
	c.Assert(*idx.IndexKrn, Equals, createRes.IndexKrn)
	c.Assert(*idx.VectorBucketName, Equals, s.bucket)
	c.Assert(*idx.DataType, Equals, "float32")
	c.Assert(*idx.Dimension, Equals, 1024)
	c.Assert(*idx.DistanceMetric, Equals, "euclidean")
	c.Assert(*idx.CreationTime > 0, Equals, true)

	// 列举索引：含所有可选字段，应能查到并校验条目字段
	maxResults := 100
	listRes, err := s.vc.ListIndexes(&ListIndexesRequest{
		VectorBucketName: strPtr(s.bucket),
		MaxResults:       &maxResults,
		Prefix:           strPtr(index),
	})
	c.Assert(err, IsNil)
	c.Assert(listRes.StatusCode, Equals, http.StatusOK)
	var found *IndexEntry
	for i := range listRes.Indexes {
		if listRes.Indexes[i].IndexName != nil && *listRes.Indexes[i].IndexName == index {
			found = &listRes.Indexes[i]
			break
		}
	}
	c.Assert(found, NotNil, Commentf("created index %q not found in list", index))
	c.Assert(*found.IndexKrn, Equals, createRes.IndexKrn)
	c.Assert(*found.VectorBucketName, Equals, s.bucket)
	c.Assert(*found.DataType, Equals, "float32")
	c.Assert(*found.Dimension, Equals, 1024)
	c.Assert(*found.DistanceMetric, Equals, "euclidean")
	c.Assert(*found.CreationTime > 0, Equals, true)

	// 删除索引
	delRes, err := s.vc.DeleteIndex(&DeleteIndexRequest{VectorBucketName: strPtr(s.bucket), IndexName: strPtr(index)})
	c.Assert(err, IsNil)
	c.Assert(delRes.StatusCode, Equals, http.StatusOK)
	c.Logf("DeleteIndex success: %s", index)
}

// TestVectorsIntegration 用共享桶测试向量：先建索引，再写入、列举、获取、查询、删除向量，最后删索引。
func (s *Ks3VectorsClientSuite) TestVectorsIntegration(c *C) {
	s.skipIfNotReady(c)

	index := "idx" + randLowStr(4)
	c.Logf("bucket=%s index=%s", s.bucket, index)

	// 先创建索引（维度 4，便于测试）
	_, err := s.vc.CreateIndex(&CreateIndexRequest{
		VectorBucketName: strPtr(s.bucket),
		DataType:         strPtr("float32"),
		Dimension:        intPtr(4),
		DistanceMetric:   strPtr("euclidean"),
		IndexName:        strPtr(index),
	})
	c.Assert(err, IsNil)

	// 写入向量
	vec := []InputVector{
		{Key: "k1", Data: VectorData{Float32: []float32{1, 2, 3, 4}}, Metadata: map[string]interface{}{"color": "red"}},
		{Key: "k2", Data: VectorData{Float32: []float32{5, 6, 7, 8}}, Metadata: map[string]interface{}{"color": "blue"}},
	}
	_, err = s.vc.PutVectors(&PutVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		Vectors:          vec,
	})
	c.Assert(err, IsNil)
	c.Logf("PutVectors success: %d vectors", len(vec))

	// 写入后等待服务端可见（向量桶写入存在一致性延迟，无延时约 40% 概率读不到；服务端问题，后续优化）
	time.Sleep(100 * time.Millisecond)

	// 获取向量：强校验数量与内容
	getRes, err := s.vc.GetVectors(&GetVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		Keys:             []string{"k1", "k2"},
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
	})
	c.Assert(err, IsNil)
	c.Assert(getRes.Vectors, HasLen, 2, Commentf("GetVectors should return 2 vectors"))
	// 校验内容：k1 的数据与元数据
	var k1 *OutputVector
	for i := range getRes.Vectors {
		if getRes.Vectors[i].Key == "k1" {
			k1 = &getRes.Vectors[i]
			break
		}
	}
	c.Assert(k1, NotNil, Commentf("k1 not found in GetVectors result"))
	c.Assert(k1.Data.Float32, DeepEquals, []float32{1, 2, 3, 4})
	c.Logf("GetVectors: key=%s data=%v", k1.Key, k1.Data.Float32)

	// 列举向量：含所有可选字段，强校验数量与内容（服务端若未部署 ListVectors 返回错误则 skip）
	listRes, err := s.vc.ListVectors(&ListVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		MaxResults:       intPtr(100),
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
		Filter:           map[string]interface{}{"color": "red"},
	})
	if err != nil {
		c.Logf("ListVectors not supported by server, skip: %v", err)
	} else {
		c.Assert(listRes.StatusCode, Equals, http.StatusOK)
		c.Assert(listRes.Vectors, HasLen, 2, Commentf("ListVectors should return 2 vectors"))
		// 校验 OutputVector[0] 所有字段
		var lv1 *OutputVector
		for i := range listRes.Vectors {
			if listRes.Vectors[i].Key == "k1" {
				lv1 = &listRes.Vectors[i]
				break
			}
		}
		c.Assert(lv1, NotNil)
		c.Assert(lv1.Data.Float32, DeepEquals, []float32{1, 2, 3, 4})
		c.Assert(lv1.Metadata, DeepEquals, map[string]interface{}{"color": "red"})
		c.Logf("ListVectors: %d vectors", len(listRes.Vectors))
	}

	// 相似性检索：含 filter（$eq 过滤 color=red），强校验数量与距离
	queryRes, err := s.vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		QueryVector:      VectorData{Float32: []float32{1, 2, 3, 4}},
		TopK:             intPtr(2),
		Filter:           map[string]interface{}{"color": map[string]interface{}{"$eq": "red"}},
		ReturnData:       boolPtr(true),
		ReturnMetadata:   boolPtr(true),
		ReturnDistance:   boolPtr(true),
	})
	c.Assert(err, IsNil)
	c.Assert(queryRes.StatusCode, Equals, http.StatusOK)
	// filter color=red 只匹配 k1，应返回 1 个
	c.Assert(queryRes.Vectors, HasLen, 1, Commentf("QueryVectors with filter color=red should return 1 vector"))
	c.Assert(*queryRes.Vectors[0].Distance, Equals, float64(0))
	c.Assert(queryRes.Vectors[0].Key, Equals, "k1")
	c.Logf("QueryVectors: filter color=red, top1 key=%s distance=%v", queryRes.Vectors[0].Key, *queryRes.Vectors[0].Distance)

	// 删除向量
	_, err = s.vc.DeleteVectors(&DeleteVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		Keys:             []string{"k1", "k2"},
	})
	c.Assert(err, IsNil)
	c.Logf("DeleteVectors success: k1, k2")

	// 最后删除索引
	_, err = s.vc.DeleteIndex(&DeleteIndexRequest{VectorBucketName: strPtr(s.bucket), IndexName: strPtr(index)})
	c.Assert(err, IsNil)
	c.Logf("DeleteIndex success: %s", index)
}

// TestVectorBucketPolicyIntegration 用共享桶测试策略的新增、获取、删除。
func (s *Ks3VectorsClientSuite) TestVectorBucketPolicyIntegration(c *C) {
	s.skipIfNotReady(c)

	c.Logf("bucket=%s", s.bucket)

	// 新增策略（policy 格式需据服务端联调确认）
	policy := `{"Version":"2025-12-01","Statement":[{"Effect":"Allow","Principal":"*","Action":["ks3vectors:GetVectorBucket"],"Resource":["*"]}]}`
	_, err := s.vc.PutVectorBucketPolicy(&PutVectorBucketPolicyRequest{
		VectorBucketName: strPtr(s.bucket),
		Policy:           strPtr(policy),
	})
	if err != nil {
		c.Logf("PutVectorBucketPolicy failed (policy 格式需联调确认): %v", err)
		c.Skip("PutVectorBucketPolicy failed, policy format needs server confirmation")
	}
	c.Logf("PutVectorBucketPolicy success")

	// 获取策略
	getRes, err := s.vc.GetVectorBucketPolicy(&GetVectorBucketPolicyRequest{VectorBucketName: strPtr(s.bucket)})
	c.Assert(err, IsNil)
	c.Assert(getRes.Policy, Not(Equals), "")
	c.Logf("GetVectorBucketPolicy: %s", getRes.Policy)

	// 删除策略
	_, err = s.vc.DeleteVectorBucketPolicy(&DeleteVectorBucketPolicyRequest{VectorBucketName: strPtr(s.bucket)})
	c.Assert(err, IsNil)
	c.Logf("DeleteVectorBucketPolicy success")
}

// TestVectorServiceErrorIntegration 用参数错误（TopK 超范围）触发服务端 400，
// 验证 VectorServiceError 的 Message/FieldList/RequestID/StatusCode 真实解析正确。
func (s *Ks3VectorsClientSuite) TestVectorServiceErrorIntegration(c *C) {
	s.skipIfNotReady(c)

	// 用共享桶建临时索引（TopK 超范围是参数校验，不需写入数据）
	index := "idx" + randLowStr(4)
	_, err := s.vc.CreateIndex(&CreateIndexRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		DataType:         strPtr("float32"),
		Dimension:        intPtr(4),
		DistanceMetric:   strPtr("euclidean"),
	})
	c.Assert(err, IsNil)
	defer s.vc.DeleteIndex(&DeleteIndexRequest{VectorBucketName: strPtr(s.bucket), IndexName: strPtr(index)})

	// TopK=100 超范围（上限 30），触发服务端参数校验错误
	_, err = s.vc.QueryVectors(&QueryVectorsRequest{
		VectorBucketName: strPtr(s.bucket),
		IndexName:        strPtr(index),
		QueryVector:      VectorData{Float32: []float32{1, 2, 3, 4}},
		TopK:             intPtr(100),
	})
	c.Assert(err, NotNil)

	// 验证解析为 VectorServiceError
	se, ok := err.(ks3.VectorServiceError)
	c.Assert(ok, Equals, true, Commentf("error should be VectorServiceError, got %T", err))
	// 校验所有字段
	c.Assert(se.StatusCode, Equals, 400)
	c.Assert(se.Message, Not(Equals), "", Commentf("Message should not be empty"))
	c.Assert(se.RequestID, Not(Equals), "", Commentf("RequestID should not be empty"))
	c.Assert(se.FieldList, HasLen, 1, Commentf("FieldList should have 1 item"))
	c.Assert(se.FieldList[0].Message, Not(Equals), "", Commentf("FieldList[0].Message should not be empty"))
	c.Assert(se.FieldList[0].Path, Equals, "/topK")
	c.Logf("VectorServiceError: StatusCode=%d Message=%q FieldList[0]={%q,%q} RequestID=%q",
		se.StatusCode, se.Message, se.FieldList[0].Message, se.FieldList[0].Path, se.RequestID)
}
