package ks3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
)

type ServerSideEncryptionConfiguration struct {
	XMLName xml.Name `xml:"ServerSideEncryptionConfiguration"`
	ServerSideEncryptionRule ServerSideEncryptionRule `xml:"Rule"`
}

type ServerSideEncryptionRule struct {
	XMLName xml.Name `xml:"Rule"`
	ApplyServerSideEncryptionByDefault ApplyServerSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault"`
}

type ApplyServerSideEncryptionByDefault struct {
	XMLName      xml.Name `xml:"ApplyServerSideEncryptionByDefault"`
	SSEAlgorithm string   `xml:"SSEAlgorithm,omitempty"`
}

func (client Client) PutBucketEncryption(bucketName string, encryptionRule ServerSideEncryptionRule, options ...Option) error {
	encryptionCfg := ServerSideEncryptionConfiguration{ServerSideEncryptionRule: encryptionRule}
	bs, err := xml.Marshal(encryptionCfg)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["encryption"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

func (client Client) PutBucketEncryptionXml(bucketName string, xmlBody string, options ...Option) error {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(xmlBody))

	md5 := encodeAsString(computeMD5Hash(buffer.Bytes()))
	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType
	headers[HTTPHeaderContentMD5] = md5

	params := map[string]interface{}{}
	params["encryption"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusOK})
}

func (client Client) GetBucketEncryption(bucketName string, options ...Option) (ServerSideEncryptionConfiguration, error) {
	var out ServerSideEncryptionConfiguration
	params := map[string]interface{}{}
	params["encryption"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

func (client Client) GetBucketEncryptionXml(bucketName string, options ...Option) (string, error) {
	params := map[string]interface{}{}
	params["encryption"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil, options...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	out := string(body)
	return out, err
}

func (client Client) DeleteBucketEncryption(bucketName string, options ...Option) error {
	params := map[string]interface{}{}
	params["encryption"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil, options...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return CheckRespCode(resp.StatusCode, []int{http.StatusNoContent})
}