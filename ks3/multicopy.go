package ks3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

// CopyFile is multipart copy object
//
// srcBucketName    source bucket name
// srcObjectKey    source object name
// destObjectKey    target object name in the form of bucketname.objectkey
// partSize    the part size in byte.
// options    object's contraints. Check out function InitiateMultipartUpload.
//
// error    it's nil if the operation succeeds, otherwise it's an error object.
//
func (bucket Bucket) CopyFile(srcBucket *Bucket, srcObjectKey, destObjectKey string, partSize int64, options ...Option) error {
	destBucketName := bucket.BucketName
	if partSize < MinPartSize || partSize > MaxPartSize {
		return errors.New("ks3: part size invalid range (1024KB, 5GB]")
	}

	cpConf := getCpConfig(options)
	routines := getRoutines(options)

	var strVersionId string
	versionId, _ := FindOption(options, "versionId", nil)
	if versionId != nil {
		strVersionId = versionId.(string)
	}

	if cpConf != nil && cpConf.IsEnable {
		cpFilePath := getCopyCpFilePath(cpConf, srcBucket.BucketName, srcObjectKey, destBucketName, destObjectKey, strVersionId)
		if cpFilePath != "" {
			return bucket.copyFileWithCp(srcBucket, srcObjectKey, destObjectKey, partSize, options, cpFilePath, routines)
		}
	}

	return bucket.copyFile(srcBucket, srcObjectKey, destObjectKey, partSize, options, routines)
}

func getCopyCpFilePath(cpConf *cpConfig, srcBucket, srcObject, destBucket, destObject, versionId string) string {
	if cpConf.FilePath == "" && cpConf.DirPath != "" {
		dest := fmt.Sprintf("ks3://%v/%v", destBucket, destObject)
		src := fmt.Sprintf("ks3://%v/%v", srcBucket, srcObject)
		cpFileName := getCpFileName(src, dest, versionId)
		cpConf.FilePath = cpConf.DirPath + string(os.PathSeparator) + cpFileName
	}
	return cpConf.FilePath
}

// ----- Concurrently copy without checkpoint ---------

// copyWorkerArg defines the copy worker arguments
type copyWorkerArg struct {
	destbucket     *Bucket
	srcBucket      *Bucket
	imur          InitiateMultipartUploadResult
	srcObjectKey  string
	options       []Option
	hook          copyPartHook
	isAcrossRegion bool
}

// copyPartHook is the hook for testing purpose
type copyPartHook func(part copyPart) error

var copyPartHooker copyPartHook = defaultCopyPartHook

func defaultCopyPartHook(part copyPart) error {
	return nil
}

// copyWorker copies worker
func copyWorker(id int, arg copyWorkerArg, jobs <-chan copyPart, results chan<- UploadPart, failed chan<- error, die <-chan bool) {
	for chunk := range jobs {
		if err := arg.hook(chunk); err != nil {
			failed <- err
			break
		}
		chunkSize := chunk.End - chunk.Start + 1
		var part UploadPart
		var err error
		var respHeader http.Header
		startT := time.Now()
		if !arg.isAcrossRegion {
			// 同区域复制，使用UploadPartCopy
			options := append(arg.options, GetResponseHeader(&respHeader))
			part, err = arg.destbucket.UploadPartCopy(arg.imur, arg.srcBucket.BucketName, arg.srcObjectKey, chunk.Start, chunkSize, chunk.Number, options...)
		} else {
			// 跨区域复制，先GetObject再UploadPart
			reader, err := arg.srcBucket.GetObject(arg.srcObjectKey, Range(chunk.Start, chunk.End))
			if err == nil {
				options := append(arg.options, GetResponseHeader(&respHeader))
				part, err = arg.destbucket.UploadPart(arg.imur, reader, chunkSize, chunk.Number, options...)
			}
		}
		cost := time.Now().UnixNano()/1000/1000 - startT.UnixNano()/1000/1000
		if err != nil {
			arg.destbucket.Client.Config.WriteLog(Error, "copy part error, bucketName:%s, objectKey:%s, partNumber:%d, cost:%d(ms), error:%s\n", arg.imur.Bucket, arg.imur.Key, chunk.Number, cost, err.Error())
			failed <- err
			break
		}
		arg.destbucket.Client.Config.WriteLog(Info, "copy part success, bucketName:%s, objectKey:%s, partNumber:%d, cost:%d(ms), requestId:%s\n", arg.imur.Bucket, arg.imur.Key, chunk.Number, cost, GetRequestId(respHeader))
		select {
		case <-die:
			return
		default:
		}
		results <- part
	}
}

// copyScheduler
func copyScheduler(jobs chan copyPart, parts []copyPart) {
	for _, part := range parts {
		jobs <- part
	}
	close(jobs)
}

// copyPart structure
type copyPart struct {
	Number int   // Part number (from 1 to 10,000)
	Start  int64 // The start index in the source file.
	End    int64 // The end index in the source file
}

// getCopyParts calculates copy parts
func getCopyParts(objectSize, partSize int64) []copyPart {
	parts := []copyPart{}
	part := copyPart{}
	i := 0
	for offset := int64(0); offset < objectSize; offset += partSize {
		part.Number = i + 1
		part.Start = offset
		part.End = GetPartEnd(offset, objectSize, partSize)
		parts = append(parts, part)
		i++
	}
	return parts
}

// getSrcObjectBytes gets the source file size
func getSrcObjectBytes(parts []copyPart) int64 {
	var ob int64
	for _, part := range parts {
		ob += (part.End - part.Start + 1)
	}
	return ob
}

// copyFile is a concurrently copy without checkpoint
func (bucket Bucket) copyFile(srcBucket *Bucket, srcObjectKey, destObjectKey string, partSize int64, options []Option, routines int) error {
	listener := GetProgressListener(options)

	// choice valid options
	headerOptions := ChoiceHeadObjectOption(options)
	partOptions := ChoiceTransferPartOption(options)
	completeOptions := ChoiceCompletePartOption(options)
	abortOptions := ChoiceAbortPartOption(options)

	meta, err := srcBucket.GetObjectDetailedMeta(srcObjectKey, headerOptions...)
	if err != nil {
		return err
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 0)
	if err != nil {
		return err
	}

	// Get copy parts
	parts := getCopyParts(objectSize, partSize)
	// Initialize the multipart upload
	imur, err := bucket.InitiateMultipartUpload(destObjectKey, options...)
	if err != nil {
		return err
	}

	jobs := make(chan copyPart, len(parts))
	results := make(chan UploadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	var completedBytes int64
	totalBytes := getSrcObjectBytes(parts)
	event := newProgressEvent(TransferStartedEvent, 0, totalBytes, 0)
	publishProgress(listener, event)
	isAcrossRegion := getAcrossRegion(options)

	// Start to copy workers
	arg := copyWorkerArg{&bucket, srcBucket, imur, srcObjectKey, partOptions, copyPartHooker, isAcrossRegion}
	for w := 1; w <= routines; w++ {
		go copyWorker(w, arg, jobs, results, failed, die)
	}

	// Start the scheduler
	go copyScheduler(jobs, parts)

	// Wait for the parts finished.
	completed := 0
	ups := make([]UploadPart, len(parts))
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			ups[part.PartNumber-1] = part
			copyBytes := parts[part.PartNumber-1].End - parts[part.PartNumber-1].Start + 1
			completedBytes += copyBytes
			event = newProgressEvent(TransferPartEvent, completedBytes, totalBytes, copyBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			bucket.AbortMultipartUpload(imur, abortOptions...)
			event = newProgressEvent(TransferFailedEvent, completedBytes, totalBytes, 0)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, totalBytes, 0)
	publishProgress(listener, event)

	// Complete the multipart upload
	_, err = bucket.CompleteMultipartUpload(imur, ups, completeOptions...)
	if err != nil {
		bucket.AbortMultipartUpload(imur, abortOptions...)
		return err
	}
	return nil
}

// ----- Concurrently copy with checkpoint  -----

const copyCpMagic = "84F1F18C-FF1D-403B-A1D8-9DEB5F65910A"

type copyCheckpoint struct {
	Magic          string       // Magic
	MD5            string       // CP content MD5
	SrcBucketName  string       // Source bucket
	SrcObjectKey   string       // Source object
	DestBucketName string       // Target bucket
	DestObjectKey  string       // Target object
	CopyID         string       // Copy ID
	ObjStat        objectStat   // Object stat
	Parts          []copyPart   // Copy parts
	CopyParts      []UploadPart // The uploaded parts
	PartStat       []bool       // The part status
}

// isValid checks if the data is valid which means CP is valid and object is not updated.
func (cp copyCheckpoint) isValid(meta http.Header) (bool, error) {
	// Compare CP's magic number and the MD5.
	cpb := cp
	cpb.MD5 = ""
	js, _ := json.Marshal(cpb)
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	if cp.Magic != copyCpMagic || b64 != cp.MD5 {
		return false, nil
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return false, err
	}

	// Compare the object size and last modified time and etag.
	if cp.ObjStat.Size != objectSize ||
		cp.ObjStat.LastModified != meta.Get(HTTPHeaderLastModified) ||
		cp.ObjStat.Etag != meta.Get(HTTPHeaderEtag) {
		return false, nil
	}

	return true, nil
}

// load loads from the checkpoint file
func (cp *copyCheckpoint) load(filePath string) error {
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(contents, cp)
	return err
}

// update updates the parts status
func (cp *copyCheckpoint) update(part UploadPart) {
	cp.CopyParts[part.PartNumber-1] = part
	cp.PartStat[part.PartNumber-1] = true
}

// dump dumps the CP to the file
func (cp *copyCheckpoint) dump(filePath string) error {
	bcp := *cp

	// Calculate MD5
	bcp.MD5 = ""
	js, err := json.Marshal(bcp)
	if err != nil {
		return err
	}
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	bcp.MD5 = b64

	// Serialization
	js, err = json.Marshal(bcp)
	if err != nil {
		return err
	}

	// Dump
	return ioutil.WriteFile(filePath, js, FilePermMode)
}

// todoParts returns unfinished parts
func (cp copyCheckpoint) todoParts() []copyPart {
	dps := []copyPart{}
	for i, ps := range cp.PartStat {
		if !ps {
			dps = append(dps, cp.Parts[i])
		}
	}
	return dps
}

// getCompletedBytes returns finished bytes count
func (cp copyCheckpoint) getCompletedBytes() int64 {
	var completedBytes int64
	for i, part := range cp.Parts {
		if cp.PartStat[i] {
			completedBytes += (part.End - part.Start + 1)
		}
	}
	return completedBytes
}

// prepare initializes the multipart upload
func (cp *copyCheckpoint) prepare(meta http.Header, srcBucket *Bucket, srcObjectKey string, destBucket *Bucket, destObjectKey string,
	partSize int64, options []Option) error {
	// CP
	cp.Magic = copyCpMagic
	cp.SrcBucketName = srcBucket.BucketName
	cp.SrcObjectKey = srcObjectKey
	cp.DestBucketName = destBucket.BucketName
	cp.DestObjectKey = destObjectKey

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return err
	}

	cp.ObjStat.Size = objectSize
	cp.ObjStat.LastModified = meta.Get(HTTPHeaderLastModified)
	cp.ObjStat.Etag = meta.Get(HTTPHeaderEtag)

	// Parts
	cp.Parts = getCopyParts(objectSize, partSize)
	cp.PartStat = make([]bool, len(cp.Parts))
	for i := range cp.PartStat {
		cp.PartStat[i] = false
	}
	cp.CopyParts = make([]UploadPart, len(cp.Parts))

	// Init copy
	imur, err := destBucket.InitiateMultipartUpload(destObjectKey, options...)
	if err != nil {
		return err
	}
	cp.CopyID = imur.UploadID

	return nil
}

func (cp *copyCheckpoint) complete(bucket *Bucket, parts []UploadPart, cpFilePath string, options []Option) error {
	imur := InitiateMultipartUploadResult{Bucket: cp.DestBucketName,
		Key: cp.DestObjectKey, UploadID: cp.CopyID}
	_, err := bucket.CompleteMultipartUpload(imur, parts, options...)
	if err != nil {
		return err
	}
	os.Remove(cpFilePath)
	return err
}

// copyFileWithCp is concurrently copy with checkpoint
func (bucket Bucket) copyFileWithCp(srcBucket *Bucket, srcObjectKey, destObjectKey string, partSize int64, options []Option, cpFilePath string, routines int) error {
	listener := GetProgressListener(options)

	// Load CP data
	ccp := copyCheckpoint{}
	err := ccp.load(cpFilePath)
	if err != nil {
		os.Remove(cpFilePath)
	}

	// choice valid options
	headerOptions := ChoiceHeadObjectOption(options)
	partOptions := ChoiceTransferPartOption(options)
	completeOptions := ChoiceCompletePartOption(options)

	meta, err := srcBucket.GetObjectDetailedMeta(srcObjectKey, headerOptions...)
	if err != nil {
		return err
	}

	// Load error or the CP data is invalid---reinitialize
	valid, err := ccp.isValid(meta)
	if err != nil || !valid {
		if err = ccp.prepare(meta, srcBucket, srcObjectKey, &bucket, destObjectKey, partSize, options); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	// Unfinished parts
	parts := ccp.todoParts()
	imur := InitiateMultipartUploadResult{
		Bucket: bucket.BucketName,
		Key:      destObjectKey,
		UploadID: ccp.CopyID}

	jobs := make(chan copyPart, len(parts))
	results := make(chan UploadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := ccp.getCompletedBytes()
	event := newProgressEvent(TransferStartedEvent, completedBytes, ccp.ObjStat.Size, 0)
	publishProgress(listener, event)
	isAcrossRegion := getAcrossRegion(options)

	// Start the worker coroutines
	arg := copyWorkerArg{&bucket, srcBucket, imur, srcObjectKey, partOptions, copyPartHooker, isAcrossRegion}
	for w := 1; w <= routines; w++ {
		go copyWorker(w, arg, jobs, results, failed, die)
	}

	// Start the scheduler
	go copyScheduler(jobs, parts)

	// Wait for the parts completed.
	completed := 0
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			ccp.update(part)
			ccp.dump(cpFilePath)
			copyBytes := ccp.Parts[part.PartNumber-1].End - ccp.Parts[part.PartNumber-1].Start + 1
			completedBytes += copyBytes
			event = newProgressEvent(TransferPartEvent, completedBytes, ccp.ObjStat.Size, copyBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, ccp.ObjStat.Size, 0)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, ccp.ObjStat.Size, 0)
	publishProgress(listener, event)

	return ccp.complete(&bucket, ccp.CopyParts, cpFilePath, completeOptions)
}
