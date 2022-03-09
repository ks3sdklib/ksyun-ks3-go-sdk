# ksyun KS3 SDK for Go


## [README in English](https://github.com/chenqichen/ksyun-ks3-go-sdk/blob/master/README.md)

## 关于
> - 此Go SDK基于[金山云对象存储服务](https://docs.ksyun.com/documents/39060)官方API构建。
> - 金山云对象存储（Object Storage Service，简称KS3），是金山云对外提供的海量，安全，低成本，高可靠的云存储服务。
> - KS3适合存放任意文件类型，适合各种网站、开发企业及开发者使用。
> - 使用此SDK，用户可以方便地在任何应用、任何时间、任何地点上传，下载和管理数据。

## 版本
> - Current version: v0.0.1

## 运行环境
> - Go 1.5及以上。

## 安装方法
### GitHub安装
> - 执行命令`go get github.com/chenqichen/ksyun-ks3-go-sdk/ks3`获取远程代码包。
> - 在您的代码中使用`import "github.com/chenqichen/ksyun-ks3-go-sdk/ks3"`引入KS3 Go SDK的包。

## 快速使用
#### 获取存储空间列表（List Bucket）
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    lsRes, err := client.ListBuckets()
    if err != nil {
        // HandleError(err)
    }
    
    for _, bucket := range lsRes.Buckets {
        fmt.Println("Buckets:", bucket.Name)
    }
```

#### 创建存储空间（Create Bucket）
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    err = client.CreateBucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
```
    
#### 删除存储空间（Delete Bucket）
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    err = client.DeleteBucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
```

#### 上传文件（Put Object）
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    bucket, err := client.Bucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
    
    err = bucket.PutObjectFromFile("my-object", "LocalFile")
    if err != nil {
        // HandleError(err)
    }
```

#### 下载文件 (Get Object)
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    bucket, err := client.Bucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
    
    err = bucket.GetObjectToFile("my-object", "LocalFile")
    if err != nil {
        // HandleError(err)
    }
```

#### 获取文件列表（List Objects）
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    bucket, err := client.Bucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
    
    lsRes, err := bucket.ListObjects()
    if err != nil {
        // HandleError(err)
    }
    
    for _, object := range lsRes.Objects {
        fmt.Println("Objects:", object.Key)
    }
```
    
#### 删除文件(Delete Object)
```go
    client, err := ks3.New("Endpoint", "AccessKeyId", "AccessKeySecret")
    if err != nil {
        // HandleError(err)
    }
    
    bucket, err := client.Bucket("my-bucket")
    if err != nil {
        // HandleError(err)
    }
    
    err = bucket.DeleteObject("my-object")
    if err != nil {
        // HandleError(err)
    }
```

#### 其它
更多的示例程序，请参看KS3 Go SDK安装路径（即GOPATH变量中的第一个路径）下的`src\github.com\ksyun\ksyun-ks3-go-sdk\sample`，该目录下为示例程序，
或者参看`https://github.com/chenqichen/ksyun-ks3-go-sdk`下sample目录中的示例文件。

## 注意事项
### 运行sample
> - 拷贝示例文件。到KS3 Go SDK的安装路径（即GOPATH变量中的第一个路径），进入KS3 Go SDK的代码目录`src\github.com\ksyun\ksyun-ks3-go-sdk`，
把其下的sample目录和sample.go复制到您的测试工程src目录下。
> - 修改sample/config.go里的endpoint、AccessKeyId、AccessKeySecret、BucketName等配置。
> - 请在您的工程目录下执行`go run src/sample.go`。

## 联系我们
> - [金山云KS3官方网站](http://ks3.console.ksyun.com)
> - [金山云KS3官方文档中心](https://docs.ksyun.com/documents/2326)


## License
> - MIT License, see [license file](LICENSE)

