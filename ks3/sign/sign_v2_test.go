package sign

import (
	"fmt"
	. "gopkg.in/check.v1"
	"net/http"
	"strings"
	"testing"
	"time"
)

func Test(t *testing.T) {
	TestingT(t)
}

type Ks3SignV2Suite struct{}

var _ = Suite(&Ks3SignV2Suite{})

func (s *Ks3SignV2Suite)TestSignV2(c *C) {
	// 填写AK
	ak := "AKLTj8oTzPjmTISH4apZQ66KwA"
	// 填写SK
	sk := "OKILanSMVOvEZkFEOdqtLKoZwZBI6sVSnL0R/LQgv4ISgFntHuC4XieHEXXDSC71nQ=="
	// 填写访问域名
	endpoint := "ks3-cn-beijing.ksyuncs.com"
	// 填写bucket的名称
	bucket := "likui-test3"

	fmt.Println("---------------- 上传对象 ----------------")
	putObject(ak, sk, endpoint, bucket)

	fmt.Println("---------------- 获取对象 ----------------")
	getObject(ak, sk, endpoint, bucket)

	fmt.Println("---------------- 获取对象的ACL ----------------")
	getObjectAcl(ak, sk, endpoint, bucket)

	fmt.Println("---------------- 删除对象 ----------------")
	deleteObject(ak, sk, endpoint, bucket)

	fmt.Println("---------------- 列举bucket ----------------")
	listBucket(ak, sk, endpoint)

	fmt.Println("---------------- 列举object ----------------")
	listObject(ak, sk, endpoint, bucket)
}

func putObject(ak string, sk string, endpoint string, bucket string) {
	objectKey := "demo.txt"
	// 构造请求URL
	url := GetUrl(endpoint, bucket, objectKey, "", "")
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("PUT", url, strings.NewReader("test content"))
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("x-kss-acl", "public-read")
	// 计算签名
	Authorization := SignV2(ak, sk, bucket, objectKey, "", req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}

func getObject(ak string, sk string, endpoint string, bucket string) {
	objectKey := "demo.txt"
	// 构造请求URL
	url := GetUrl(endpoint, bucket, objectKey, "", "")
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	// 计算签名
	Authorization := SignV2(ak, sk, bucket, objectKey, "", req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}

func getObjectAcl(ak string, sk string, endpoint string, bucket string) {
	objectKey := "demo.txt"
	subResource := "acl"
	// 构造请求URL
	url := GetUrl(endpoint, bucket, objectKey, subResource, "")
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("GET", url, strings.NewReader("test content"))
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	// 计算签名
	Authorization := SignV2(ak, sk, bucket, objectKey, subResource, req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}

func deleteObject(ak string, sk string, endpoint string, bucket string) {
	objectKey := "demo.txt"
	// 构造请求URL
	url := GetUrl(endpoint, bucket, objectKey, "", "")
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	// 计算签名
	Authorization := SignV2(ak, sk, bucket, objectKey, "", req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}

func listBucket(ak string, sk string, endpoint string) {
	// 构造请求URL
	url := GetUrl(endpoint, "", "", "", "")
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	// 计算签名
	Authorization := SignV2(ak, sk, "", "", "", req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}

func listObject(ak string, sk string, endpoint string, bucket string) {
	query := "prefix=test&max-keys=100"
	// 构造请求URL
	url := GetUrl(endpoint, bucket, "", "", query)
	fmt.Println("requestURL: ", url)
	// 构造HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	// Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。
	req.Header.Add("Date", time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	// 计算签名
	Authorization := SignV2(ak, sk, bucket, "", "", req)
	fmt.Println("Authorization: ", Authorization)
	// 添加Authorization请求头
	req.Header.Set("Authorization", Authorization)
	// 发送请求
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println("StatusCode: ", resp.StatusCode)
	fmt.Println("Status: ", resp.Status)
}