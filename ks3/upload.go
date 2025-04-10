package ks3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// UploadFile is multipart file upload.
//
// objectKey    the object name.
// filePath    the local file path to upload.
// partSize    the part size in byte.
// options    the options for uploading object.
//
// error    it's nil if the operation succeeds, otherwise it's an error object.
func (bucket Bucket) UploadFile(objectKey, filePath string, partSize int64, options ...Option) error {
	if partSize < MinPartSize || partSize > MaxPartSize {
		return errors.New("ks3: part size invalid range (100KB, 5GB]")
	}

	cpConf := getCpConfig(options)
	routines := getRoutines(options)

	if cpConf != nil && cpConf.IsEnable {
		cpFilePath := getUploadCpFilePath(cpConf, filePath, bucket.BucketName, objectKey)
		if cpFilePath != "" {
			return bucket.uploadFileWithCp(objectKey, filePath, partSize, options, cpFilePath, routines)
		}
	}

	return bucket.uploadFile(objectKey, filePath, partSize, options, routines)
}

func getUploadCpFilePath(cpConf *cpConfig, srcFile, destBucket, destObject string) string {
	if cpConf.FilePath == "" && cpConf.DirPath != "" {
		dest := fmt.Sprintf("ks3://%v/%v", destBucket, destObject)
		absPath, _ := filepath.Abs(srcFile)
		cpFileName := getCpFileName(absPath, dest, "")
		cpConf.FilePath = cpConf.DirPath + string(os.PathSeparator) + cpFileName
	}
	return cpConf.FilePath
}

// ----- concurrent upload without checkpoint  -----

// getCpConfig gets checkpoint configuration
func getCpConfig(options []Option) *cpConfig {
	cpcOpt, err := FindOption(options, checkpointConfig, nil)
	if err != nil || cpcOpt == nil {
		return nil
	}

	return cpcOpt.(*cpConfig)
}

// getCpFileName return the name of the checkpoint file
func getCpFileName(src, dest, versionId string) string {
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(src))
	srcCheckSum := hex.EncodeToString(md5Ctx.Sum(nil))

	md5Ctx.Reset()
	md5Ctx.Write([]byte(dest))
	destCheckSum := hex.EncodeToString(md5Ctx.Sum(nil))

	if versionId == "" {
		return fmt.Sprintf("%v-%v.cp", srcCheckSum, destCheckSum)
	}

	md5Ctx.Reset()
	md5Ctx.Write([]byte(versionId))
	versionCheckSum := hex.EncodeToString(md5Ctx.Sum(nil))
	return fmt.Sprintf("%v-%v-%v.cp", srcCheckSum, destCheckSum, versionCheckSum)
}

// getRoutines gets the routine count. by default it's 1.
func getRoutines(options []Option) int {
	rtnOpt, err := FindOption(options, routineNum, nil)
	if err != nil || rtnOpt == nil {
		return 1
	}

	rs := rtnOpt.(int)
	if rs < 1 {
		rs = 1
	} else if rs > 100 {
		rs = 100
	}

	return rs
}

// getPayer return the request payer
func getPayer(options []Option) string {
	payerOpt, err := FindOption(options, HTTPHeaderKs3Requester, nil)
	if err != nil || payerOpt == nil {
		return ""
	}
	return payerOpt.(string)
}

// GetProgressListener gets the progress callback
func GetProgressListener(options []Option) ProgressListener {
	isSet, listener, _ := IsOptionSet(options, progressListener)
	if !isSet {
		return nil
	}
	return listener.(ProgressListener)
}

// uploadPartHook is for testing usage
type uploadPartHook func(id int, chunk FileChunk) error

var uploadPartHooker uploadPartHook = defaultUploadPart

func defaultUploadPart(id int, chunk FileChunk) error {
	return nil
}

// workerArg defines worker argument structure
type workerArg struct {
	bucket   *Bucket
	filePath string
	imur     InitiateMultipartUploadResult
	options  []Option
	hook     uploadPartHook
	listener ProgressListener
}

// worker is the worker coroutine function
type defaultUploadProgressListener struct {
}

// ProgressChanged no-ops
func (listener *defaultUploadProgressListener) ProgressChanged(event *ProgressEvent) {
}

func worker(id int, arg workerArg, jobs <-chan FileChunk, results chan<- UploadPart, failed chan<- error, die <-chan bool) {
	for chunk := range jobs {
		if err := arg.hook(id, chunk); err != nil {
			failed <- err
			break
		}
		var respHeader http.Header
		p := Progress(arg.listener)
		opts := make([]Option, len(arg.options)+2)
		opts = append(opts, arg.options...)

		opts = append(opts, p, GetResponseHeader(&respHeader))

		startT := time.Now().UnixNano() / 1000 / 1000 / 1000
		part, err := arg.bucket.UploadPartFromFile(arg.imur, arg.filePath, chunk.Offset, chunk.Size, chunk.Number, opts...)
		endT := time.Now().UnixNano() / 1000 / 1000 / 1000
		if err != nil {
			arg.bucket.Client.Config.WriteLog(Debug, "upload part error,cost:%d second,part number:%d,request id:%s,error:%s\n", endT-startT, chunk.Number, GetRequestId(respHeader), err.Error())
			failed <- err
			break
		}
		select {
		case <-die:
			return
		default:
		}
		results <- part
	}
}

// scheduler function
func scheduler(jobs chan FileChunk, chunks []FileChunk) {
	for _, chunk := range chunks {
		jobs <- chunk
	}
	close(jobs)
}

func getTotalBytes(chunks []FileChunk) int64 {
	var tb int64
	for _, chunk := range chunks {
		tb += chunk.Size
	}
	return tb
}

func combineCRCInUploadParts(parts []cpPart) uint64 {
	if parts == nil || len(parts) == 0 {
		return 0
	}

	crc, _ := strconv.ParseUint(parts[0].Part.Crc64, 10, 64)
	for i := 1; i < len(parts); i++ {
		crc2, _ := strconv.ParseUint(parts[i].Part.Crc64, 10, 64)
		crc = CRC64Combine(crc, crc2, (uint64)(parts[i].Chunk.Size))
	}

	return crc
}

// uploadFile is a concurrent upload, without checkpoint
func (bucket Bucket) uploadFile(objectKey, filePath string, partSize int64, options []Option, routines int) error {
	listener := GetProgressListener(options)

	chunks, err := SplitFileByPartSize(filePath, partSize)
	if err != nil {
		return err
	}

	partOptions := ChoiceTransferPartOption(options)
	completeOptions := ChoiceCompletePartOption(options)
	abortOptions := ChoiceAbortPartOption(options)

	// Initialize the multipart upload
	imur, err := bucket.InitiateMultipartUpload(objectKey, options...)
	if err != nil {
		return err
	}

	jobs := make(chan FileChunk, len(chunks))
	results := make(chan UploadPart, len(chunks))
	failed := make(chan error)
	die := make(chan bool)

	var completedBytes int64
	totalBytes := getTotalBytes(chunks)
	event := newProgressEvent(TransferStartedEvent, 0, totalBytes, 0)
	publishProgress(listener, event)

	// Start the worker coroutine
	arg := workerArg{&bucket, filePath, imur, partOptions, uploadPartHooker, listener}
	for w := 1; w <= routines; w++ {
		go worker(w, arg, jobs, results, failed, die)
	}

	// Schedule the jobs
	go scheduler(jobs, chunks)

	// Waiting for the upload finished
	completed := 0
	parts := make([]cpPart, len(chunks))
	for i := range parts {
		parts[i].Chunk = chunks[i]
	}
	for completed < len(chunks) {
		select {
		case part := <-results:
			completed++
			parts[part.PartNumber-1].Part = part
			completedBytes += chunks[part.PartNumber-1].Size

			// why RwBytes in ProgressEvent is 0 ?
			// because read or write event has been notified in teeReader.Read()
			event = newProgressEvent(TransferPartEvent, completedBytes, totalBytes, chunks[part.PartNumber-1].Size)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, totalBytes, 0)
			publishProgress(listener, event)
			bucket.AbortMultipartUpload(imur, abortOptions...)
			return err
		}

		if completed >= len(chunks) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, totalBytes, 0)
	publishProgress(listener, event)

	var ps []UploadPart
	for _, part := range parts {
		ps = append(ps, part.Part)
	}

	// Complete the multpart upload
	result, err := bucket.CompleteMultipartUpload(imur, ps, completeOptions...)
	if err != nil {
		bucket.AbortMultipartUpload(imur, abortOptions...)
		return err
	}

	if bucket.GetConfig().IsEnableCRC {
		clientCRC := combineCRCInUploadParts(parts)
		serverCRC, _ := strconv.ParseUint(result.Crc64, 10, 64)
		err = CheckDownloadCRC(clientCRC, serverCRC)
		bucket.Client.Config.WriteLog(Debug, "check file crc64, bucketName:%s, objectKey:%s, client crc:%d, server crc:%d", bucket.BucketName, objectKey, clientCRC, serverCRC)
		if err != nil {
			return err
		}
	}

	return nil
}

// ----- concurrent upload with checkpoint  -----
const uploadCpMagic = "FE8BB4EA-B593-4FAC-AD7A-2459A36E2E62"

type uploadCheckpoint struct {
	Magic     string   // Magic
	MD5       string   // Checkpoint file content's MD5
	FilePath  string   // Local file path
	FileStat  cpStat   // File state
	ObjectKey string   // Key
	UploadID  string   // Upload ID
	EnableCRC bool     // Whether it has CRC check
	Parts     []cpPart // All parts of the local file
}

type cpStat struct {
	Size         int64     // File size
	LastModified time.Time // File's last modified time
	MD5          string    // Local file's MD5
}

type cpPart struct {
	Chunk       FileChunk  // File chunk
	Part        UploadPart // Uploaded part
	IsCompleted bool       // Upload complete flag
}

// isValid checks if the uploaded data is valid---it's valid when the file is not updated and the checkpoint data is valid.
func (cp uploadCheckpoint) isValid(filePath string) (bool, error) {
	// Compare the CP's magic number and MD5.
	cpb := cp
	cpb.MD5 = ""
	js, _ := json.Marshal(cpb)
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	if cp.Magic != uploadCpMagic || b64 != cp.MD5 {
		return false, nil
	}

	// Make sure if the local file is updated.
	fd, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer fd.Close()

	st, err := fd.Stat()
	if err != nil {
		return false, err
	}

	md, err := calcFileMD5(filePath)
	if err != nil {
		return false, err
	}

	// Compare the file size, file's last modified time and file's MD5
	if cp.FileStat.Size != st.Size() ||
		!cp.FileStat.LastModified.Equal(st.ModTime()) ||
		cp.FileStat.MD5 != md {
		return false, nil
	}

	return true, nil
}

// load loads from the file
func (cp *uploadCheckpoint) load(filePath string) error {
	contents, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(contents, cp)
	return err
}

// dump dumps to the local file
func (cp *uploadCheckpoint) dump(filePath string) error {
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
	return os.WriteFile(filePath, js, FilePermMode)
}

// updatePart updates the part status
func (cp *uploadCheckpoint) updatePart(part UploadPart) {
	cp.Parts[part.PartNumber-1].Part = part
	cp.Parts[part.PartNumber-1].IsCompleted = true
}

// todoParts returns unfinished parts
func (cp *uploadCheckpoint) todoParts() []FileChunk {
	fcs := []FileChunk{}
	for _, part := range cp.Parts {
		if !part.IsCompleted {
			fcs = append(fcs, part.Chunk)
		}
	}
	return fcs
}

// allParts returns all parts
func (cp *uploadCheckpoint) allParts() []UploadPart {
	ps := []UploadPart{}
	for _, part := range cp.Parts {
		ps = append(ps, part.Part)
	}
	return ps
}

// getCompletedBytes returns completed bytes count
func (cp *uploadCheckpoint) getCompletedBytes() int64 {
	var completedBytes int64
	for _, part := range cp.Parts {
		if part.IsCompleted {
			completedBytes += part.Chunk.Size
		}
	}
	return completedBytes
}

// calcFileMD5 calculates the MD5 for the specified local file
func calcFileMD5(filePath string) (string, error) {
	return "", nil
}

// prepare initializes the multipart upload
func prepare(cp *uploadCheckpoint, objectKey, filePath string, partSize int64, bucket *Bucket, options []Option) error {
	// CP
	cp.Magic = uploadCpMagic
	cp.FilePath = filePath
	cp.ObjectKey = objectKey

	// Local file
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	st, err := fd.Stat()
	if err != nil {
		return err
	}
	cp.FileStat.Size = st.Size()
	cp.FileStat.LastModified = st.ModTime()
	md, err := calcFileMD5(filePath)
	if err != nil {
		return err
	}
	cp.FileStat.MD5 = md

	if bucket.GetConfig().IsEnableCRC {
		cp.EnableCRC = true
	}

	// Chunks
	parts, err := SplitFileByPartSize(filePath, partSize)
	if err != nil {
		return err
	}

	cp.Parts = make([]cpPart, len(parts))
	for i, part := range parts {
		cp.Parts[i].Chunk = part
		cp.Parts[i].IsCompleted = false
	}

	// Init load
	imur, err := bucket.InitiateMultipartUpload(objectKey, options...)
	if err != nil {
		return err
	}
	cp.UploadID = imur.UploadID

	return nil
}

// complete completes the multipart upload and deletes the local CP files
func complete(cp *uploadCheckpoint, bucket *Bucket, parts []UploadPart, cpFilePath string, options []Option) (CompleteMultipartUploadResult, error) {
	imur := InitiateMultipartUploadResult{Bucket: bucket.BucketName, Key: cp.ObjectKey, UploadID: cp.UploadID}
	result, err := bucket.CompleteMultipartUpload(imur, parts, options...)
	if err != nil {
		return CompleteMultipartUploadResult{}, err
	}
	os.Remove(cpFilePath)
	return result, err
}

// isUploadIdExist 判断uploadId是否存在
// 只有当响应码为NoSuchUpload时，才返回false，其他情况均返回true，以防止因为网络或权限等问题，导致续传异常
func isUploadIdExist(imur InitiateMultipartUploadResult, bucket *Bucket) bool {
	_, err := bucket.ListUploadedParts(imur)
	if err != nil {
		var serviceError ServiceError
		isServiceError := errors.As(err, &serviceError)
		if isServiceError && serviceError.Code == "NoSuchUpload" {
			return false
		}
	}
	return true
}

// uploadFileWithCp handles concurrent upload with checkpoint
func (bucket Bucket) uploadFileWithCp(objectKey, filePath string, partSize int64, options []Option, cpFilePath string, routines int) error {
	listener := GetProgressListener(options)

	partOptions := ChoiceTransferPartOption(options)
	completeOptions := ChoiceCompletePartOption(options)

	ucp := uploadCheckpoint{}

	// 判断checkpoint文件是否存在
	fileExist, _ := IsFileExist(cpFilePath)
	if fileExist {
		// Load CP data
		err := ucp.load(cpFilePath)
		if err == nil {
			// 判断uploadId是否存在，若不存在，则删除checkpoint文件，重新上传
			uploadIdExist := isUploadIdExist(InitiateMultipartUploadResult{
				Bucket:   bucket.BucketName,
				Key:      objectKey,
				UploadID: ucp.UploadID,
			}, &bucket)
			if !uploadIdExist {
				bucket.Client.Config.WriteLog(Info, "uploadId: %s is not exist, delete checkpoint file", ucp.UploadID)
				os.Remove(cpFilePath)
				ucp = uploadCheckpoint{}
			}
		} else {
			os.Remove(cpFilePath)
		}
	}

	// Load error or the CP data is invalid.
	valid, err := ucp.isValid(filePath)
	if err != nil || !valid {
		if err = prepare(&ucp, objectKey, filePath, partSize, &bucket, options); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	chunks := ucp.todoParts()
	imur := InitiateMultipartUploadResult{
		Bucket:   bucket.BucketName,
		Key:      objectKey,
		UploadID: ucp.UploadID}

	jobs := make(chan FileChunk, len(chunks))
	results := make(chan UploadPart, len(chunks))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := ucp.getCompletedBytes()

	// why RwBytes in ProgressEvent is 0 ?
	// because read or write event has been notified in teeReader.Read()
	event := newProgressEvent(TransferStartedEvent, completedBytes, ucp.FileStat.Size, 0)
	publishProgress(listener, event)

	// Start the workers
	arg := workerArg{&bucket, filePath, imur, partOptions, uploadPartHooker, listener}
	for w := 1; w <= routines; w++ {
		go worker(w, arg, jobs, results, failed, die)
	}

	// Schedule jobs
	go scheduler(jobs, chunks)

	// Waiting for the job finished
	completed := 0
	for completed < len(chunks) {
		select {
		case part := <-results:
			completed++
			ucp.updatePart(part)
			ucp.dump(cpFilePath)
			completedBytes += ucp.Parts[part.PartNumber-1].Chunk.Size
			event = newProgressEvent(TransferPartEvent, completedBytes, ucp.FileStat.Size, ucp.Parts[part.PartNumber-1].Chunk.Size)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, ucp.FileStat.Size, 0)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(chunks) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, ucp.FileStat.Size, 0)
	publishProgress(listener, event)

	// Complete the multipart upload
	result, err := complete(&ucp, &bucket, ucp.allParts(), cpFilePath, completeOptions)

	if ucp.EnableCRC {
		clientCRC := combineCRCInUploadParts(ucp.Parts)
		serverCRC, _ := strconv.ParseUint(result.Crc64, 10, 64)
		err = CheckDownloadCRC(clientCRC, serverCRC)
		bucket.Client.Config.WriteLog(Debug, "check file crc64, bucketName:%s, objectKey:%s, client crc:%d, server crc:%d", bucket.BucketName, objectKey, clientCRC, serverCRC)
		if err != nil {
			return err
		}
	}

	return err
}
