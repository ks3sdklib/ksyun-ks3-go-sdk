# Ksyun Cloud KS3 SDK for Go

## [README in Chinese](https://github.com/ks3sdklib/ksyun-ks3-go-sdk/blob/master/README-CN.md)

## About
> - This Go SDK is based on the official APIs of [Ksyun Cloud KS3](https://docs.ksyun.com/documents/39060/).
> - Ksyun Cloud Object Storage Service (KS3) is a cloud storage service provided by Ksyun Cloud, featuring massive capacity, security, a low cost, and high reliability. 
> - The KS3 can store any type of files and therefore applies to various websites, development enterprises and developers.
> - With this SDK, you can upload, download and manage data on any app anytime and anywhere conveniently. 

## Version
> - Current version: v0.0.1

## Running Environment
> - Go 1.5 or above. 

## Installing
### Install the SDK through GitHub
> - Run the 'go get github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3' command to get the remote code package.
> - Use 'import "github.com/ks3sdklib/ksyun-ks3-go-sdk/ks3"' in your code to introduce KS3 Go SDK package.

## Getting Started
### List Bucket
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

### Create Bucket
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
    
### Delete Bucket
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

### Put Object
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

### Get Object
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

### List Objects
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
    
### Delete Object
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

##  Complete Example
More example projects can be found at 'src\github.com\ksyun\ksyun-ks3-go-sdk\sample' under the installation path of the KS3 Go SDK (the first path of the GOPATH variable). The directory contains example projects. 
Or you can refer to the example objects in the sample directory under 'https://github.com/ks3sdklib/ksyun-ks3-go-sdk'.

### Running Example
> - Copy the example file. Go to the installation path of KS3 Go SDK (the first path of the GOPATH variable), enter the code directory of the KS3 Go SDK, namely 'src\github.com\ksyun\ksyun-ks3-go-sdk',
and copy the sample directory and sample.go to the src directory of your test project.
> - Modify the  endpoint, AccessKeyId, AccessKeySecret and BucketName configuration settings in sample/config.go.
> - Run 'go run src/sample.go' under your project directory.

## Contacting us
> - [Ksyun Cloud KS3 official website](http://ks3.console.ksyun.com)
> - [Ksyun Cloud KS3 official documentation center](https://docs.ksyun.com/documents/2326).

## License
> - MIT License, see [license file](LICENSE)
