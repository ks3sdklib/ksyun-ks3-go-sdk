package ks3

import "fmt"

// ListObjectsIterator 列举对象迭代器
type ListObjectsIterator struct {
	bucket *Bucket
	options     []Option
	marker      string
	firstPage   bool
	isTruncated bool
}

// NewListObjectsIterator 创建列举对象迭代器
func NewListObjectsIterator(bucket *Bucket, options ...Option) *ListObjectsIterator {
	marker := getMarker(options)
	return &ListObjectsIterator{
		bucket:      bucket,
		options:     options,
		marker: marker,
		firstPage:   true,
		isTruncated: false,
	}
}

// HasNext 是否有下一页
func (it *ListObjectsIterator) HasNext() bool {
	return it.firstPage || it.isTruncated
}

// NextPage 获取下一页
func (it *ListObjectsIterator) NextPage() (ListObjectsResult, error) {
	var result ListObjectsResult
	if !it.HasNext() {
		return result, fmt.Errorf("no more next page")
	}

	options := it.options
	options = append(options, Marker(it.marker))
	result, err := it.bucket.ListObjects(options...)
	if err != nil {
		return result, err
	}

	// 如果结果被截断，且NextMarker为空，则使用最后一个对象的Key作为下次请求的Marker
	// 否则使用NextMarker作为下次请求的Marker
	if result.IsTruncated && result.NextMarker == "" && result.Objects != nil && len(result.Objects) > 0 {
		result.NextMarker = result.Objects[len(result.Objects)-1].Key
	}
	it.firstPage = false
	it.isTruncated = result.IsTruncated
	it.marker = result.NextMarker

	return result, nil
}
