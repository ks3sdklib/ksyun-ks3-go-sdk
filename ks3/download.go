package ks3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// DownloadFile downloads files with multipart download.
//
// objectKey    the object key.
// filePath    the local file to download from objectKey in KS3.
// partSize    the part size in bytes.
// options    object's constraints, check out GetObject for the reference.
//
// error    it's nil when the call succeeds, otherwise it's an error object.
func (bucket Bucket) DownloadFile(objectKey, filePath string, partSize int64, options ...Option) error {
	if partSize < 1 {
		return errors.New("ks3: part size smaller than 1")
	}

	uRange, err := GetRangeConfig(options)
	if err != nil {
		return err
	}

	cpConf := getCpConfig(options)
	routines := getRoutines(options)

	var strVersionId string
	versionId, _ := FindOption(options, "versionId", nil)
	if versionId != nil {
		strVersionId = versionId.(string)
	}

	if cpConf != nil && cpConf.IsEnable {
		cpFilePath := getDownloadCpFilePath(cpConf, bucket.BucketName, objectKey, strVersionId, filePath)
		if cpFilePath != "" {
			return bucket.downloadFileWithCp(objectKey, filePath, partSize, options, cpFilePath, routines, uRange)
		}
	}

	return bucket.downloadFile(objectKey, filePath, partSize, options, routines, uRange)
}

// DownloadFileWithURL 根据预签名URL下载文件
func (bucket Bucket) DownloadFileWithURL(objectKey, filePath string, partSize int64, options ...Option) error {
	if partSize < 1 {
		return errors.New("ks3: part size smaller than 1")
	}

	uRange, err := GetRangeConfig(options)
	if err != nil {
		return err
	}

	cpConf := getCpConfig(options)
	routines := getRoutines(options)

	var strVersionId string
	versionId, _ := FindOption(options, "versionId", nil)
	if versionId != nil {
		strVersionId = versionId.(string)
	}

	if cpConf != nil && cpConf.IsEnable {
		cpFilePath := getDownloadCpFilePath(cpConf, bucket.BucketName, objectKey, strVersionId, filePath)
		if cpFilePath != "" {
			return bucket.downloadFileWithURL(objectKey, filePath, partSize, options, cpFilePath, routines, uRange)
		}
	}

	return bucket.downloadFileWithURL(objectKey, filePath, partSize, options, "", routines, uRange)
}

func getDownloadCpFilePath(cpConf *cpConfig, srcBucket, srcObject, versionId, destFile string) string {
	if cpConf.FilePath == "" && cpConf.DirPath != "" {
		src := fmt.Sprintf("ks3://%v/%v", srcBucket, srcObject)
		absPath, _ := filepath.Abs(destFile)
		cpFileName := getCpFileName(src, absPath, versionId)
		cpConf.FilePath = cpConf.DirPath + string(os.PathSeparator) + cpFileName
	}
	return cpConf.FilePath
}

// downloadWorkerArg is download worker's parameters
type downloadWorkerArg struct {
	bucket    *Bucket
	key       string
	signedURL string
	filePath  string
	options   []Option
	hook      downloadPartHook
	enableCRC bool
	listener  ProgressListener
}

// downloadPartHook is hook for test
type downloadPartHook func(part downloadPart) error

var downloadPartHooker downloadPartHook = defaultDownloadPartHook

func defaultDownloadPartHook(part downloadPart) error {
	return nil
}

// downloadWorker
func downloadWorker(arg downloadWorkerArg, jobs <-chan downloadPart, results chan<- downloadPart, failed chan<- error, die <-chan bool) {
	for part := range jobs {
		if err := arg.hook(part); err != nil {
			failed <- err
			break
		}

		var respHeader http.Header
		opts := make([]Option, len(arg.options)+3)
		opts = append(opts, arg.options...)
		opts = append(opts, Range(part.Start, part.End), Progress(arg.listener), GetResponseHeader(&respHeader))

		var rd io.ReadCloser
		var err error
		if arg.signedURL != "" {
			rd, err = arg.bucket.GetObjectWithURL(arg.signedURL, opts...)
		} else {
			rd, err = arg.bucket.GetObject(arg.key, opts...)
		}
		if err != nil {
			failed <- err
			break
		}

		var crcCalc hash.Hash64
		if arg.enableCRC {
			crcCalc = crc64.New(CrcTable())
			contentLen := part.End - part.Start + 1
			rd = io.NopCloser(TeeReader(rd, crcCalc, contentLen, nil, nil))
		}

		select {
		case <-die:
			return
		default:
		}

		fd, err := os.OpenFile(arg.filePath, os.O_WRONLY, FilePermMode)
		if err != nil {
			rd.Close()
			failed <- err
			break
		}

		_, err = fd.Seek(part.Start-part.Offset, io.SeekStart)
		if err != nil {
			fd.Close()
			rd.Close()
			failed <- err
			break
		}

		startT := time.Now()
		_, err = io.Copy(fd, rd)
		cost := time.Now().UnixNano()/1000/1000 - startT.UnixNano()/1000/1000
		if err != nil {
			arg.bucket.Client.Config.WriteLog(Debug, "download part error, cost:%d(ms), partNumber:%d, requestId:%s, error:%s\n", cost, part.Index, GetRequestId(respHeader), err.Error())
			fd.Close()
			rd.Close()
			failed <- err
			break
		}
		arg.bucket.Client.Config.WriteLog(Debug, "download part success, cost:%d(ms), partNumber:%d, requestId:%s\n", cost, part.Index, GetRequestId(respHeader))

		if arg.enableCRC {
			part.CRC64 = crcCalc.Sum64()
		}

		fd.Close()
		rd.Close()

		results <- part
	}
}

// downloadScheduler
func downloadScheduler(jobs chan downloadPart, parts []downloadPart) {
	for _, part := range parts {
		jobs <- part
	}
	close(jobs)
}

// downloadPart defines download part
type downloadPart struct {
	Index  int    // Part number, starting from 0
	Start  int64  // Start index
	End    int64  // End index
	Offset int64  // Offset
	CRC64  uint64 // CRC check value of part
}

// getDownloadParts gets download parts
func getDownloadParts(objectSize, partSize int64, uRange *UnpackedRange) []downloadPart {
	var parts []downloadPart
	part := downloadPart{}
	i := 0
	start, end := AdjustRange(uRange, objectSize)
	for offset := start; offset < end; offset += partSize {
		part.Index = i
		part.Start = offset
		part.End = GetPartEnd(offset, end, partSize)
		part.Offset = start
		part.CRC64 = 0
		parts = append(parts, part)
		i++
	}
	return parts
}

// getObjectBytes gets object bytes length
func getObjectBytes(parts []downloadPart) int64 {
	var ob int64
	for _, part := range parts {
		ob += part.End - part.Start + 1
	}
	return ob
}

func combineCRCInDownloadParts(parts []downloadPart) uint64 {
	if parts == nil || len(parts) == 0 {
		return 0
	}

	crc := parts[0].CRC64
	for i := 1; i < len(parts); i++ {
		crc = CRC64Combine(crc, parts[i].CRC64, (uint64)(parts[i].End-parts[i].Start+1))
	}

	return crc
}

func rename(tempFilePath string, filePath string, disableTempFile bool) error {
	if disableTempFile {
		return nil
	}
	
	return os.Rename(tempFilePath, filePath)
}

// downloadFile downloads file concurrently without checkpoint.
func (bucket Bucket) downloadFile(objectKey, filePath string, partSize int64, options []Option, routines int, uRange *UnpackedRange) error {
	disableTempFile := getDisableTempFile(options)
	tempFilePath := filePath + TempFileSuffix
	if disableTempFile {
		tempFilePath = filePath
	}
	listener := GetProgressListener(options)

	// If the file does not exist, create one. If exists, the download will overwrite it.
	fd, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, FilePermMode)
	if err != nil {
		return err
	}
	fd.Close()

	// Get the object detailed meta for object whole size
	// must delete header:range to get whole object size
	skipOptions := DeleteOption(options, HTTPHeaderRange)
	meta, err := bucket.GetObjectDetailedMeta(objectKey, skipOptions...)
	if err != nil {
		return err
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return err
	}

	enableCRC := false
	if bucket.GetConfig().IsEnableCRC && meta.Get(HTTPHeaderKs3CRC64) != "" {
		if uRange == nil || (!uRange.HasStart && !uRange.HasEnd) {
			enableCRC = true
		}
	}

	// Get the parts of the file
	parts := getDownloadParts(objectSize, partSize, uRange)
	jobs := make(chan downloadPart, len(parts))
	results := make(chan downloadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	var completedBytes int64
	totalBytes := getObjectBytes(parts)
	event := newProgressEvent(TransferStartedEvent, 0, totalBytes, 0)
	publishProgress(listener, event)

	// Start the download workers
	arg := downloadWorkerArg{&bucket, objectKey, "", tempFilePath, options, downloadPartHooker, enableCRC, listener}
	for w := 1; w <= routines; w++ {
		go downloadWorker(arg, jobs, results, failed, die)
	}

	// Download parts concurrently
	go downloadScheduler(jobs, parts)

	// Waiting for parts download finished
	completed := 0
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			downBytes := part.End - part.Start + 1
			completedBytes += downBytes
			parts[part.Index].CRC64 = part.CRC64
			event = newProgressEvent(TransferPartEvent, completedBytes, totalBytes, downBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
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

	if enableCRC {
		clientCRC := combineCRCInDownloadParts(parts)
		serverCRC, _ := strconv.ParseUint(meta.Get(HTTPHeaderKs3CRC64), 10, 64)
		err = CheckDownloadCRC(clientCRC, serverCRC)
		bucket.Client.Config.WriteLog(Debug, "check file crc64, bucketName:%s, objectKey:%s, client crc:%d, server crc:%d", bucket.BucketName, objectKey, clientCRC, serverCRC)
		if err != nil {
			return err
		}
	}

	return rename(tempFilePath, filePath, disableTempFile)
}

// ----- Concurrent download with checkpoint  -----

const downloadCpMagic = "92611BED-89E2-46B6-89E5-72F273D4B0A3"

type downloadCheckpoint struct {
	Magic     string         // Magic
	MD5       string         // Checkpoint content MD5
	FilePath  string         // Local file
	Object    string         // Key
	Start     int64          // Start point of the file
	End       int64          // End point of the file
	EnableCRC bool           // Whether it has CRC check
	CRC       uint64         // CRC check value
	ObjStat   objectStat     // Object status
	Parts     []downloadPart // All download parts
	PartStat  []bool         // Parts' download status
}

type objectStat struct {
	Size         int64  // Object size
	LastModified string // Last modified time
	Etag         string // Etag
}

// isValid flags of checkpoint data is valid. It returns true when the data is valid and the checkpoint is valid and the object is not updated.
func (cp *downloadCheckpoint) isValid(meta http.Header, uRange *UnpackedRange) (bool, error) {
	newMd5, _ := cp.checksum()

	if cp.Magic != downloadCpMagic || newMd5 != cp.MD5 {
		return false, nil
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return false, err
	}

	// Compare the object size, last modified time and etag
	if cp.ObjStat.Size != objectSize ||
		cp.ObjStat.LastModified != meta.Get(HTTPHeaderLastModified) ||
		cp.ObjStat.Etag != meta.Get(HTTPHeaderEtag) {
		return false, nil
	}

	// Check the download range
	if uRange != nil {
		start, end := AdjustRange(uRange, objectSize)
		if start != cp.Start || end != cp.End {
			return false, nil
		}
	}

	return true, nil
}

// load checkpoint from local file
func (cp *downloadCheckpoint) load(filePath string) error {
	contents, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(contents, cp)
	return err
}

// dump function dumps to file
func (cp *downloadCheckpoint) dump(filePath string) error {
	if filePath == "" {
		return nil
	}

	newMd5, err := cp.checksum()
	if err != nil {
		return err
	}
	cp.MD5 = newMd5

	// Serialize
	js, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	// Dump
	return os.WriteFile(filePath, js, FilePermMode)
}

func (cp *downloadCheckpoint) checksum() (string, error) {
	str := cp.MD5
	cp.MD5 = ""
	js, err := json.Marshal(cp)
	if err != nil {
		return "", err
	}
	sum := md5.Sum(js)
	newMd5 := base64.StdEncoding.EncodeToString(sum[:])
	cp.MD5 = str
	return newMd5, nil
}

// todoParts gets unfinished parts
func (cp *downloadCheckpoint) todoParts() []downloadPart {
	var dps []downloadPart
	for i, ps := range cp.PartStat {
		if !ps {
			dps = append(dps, cp.Parts[i])
		}
	}
	return dps
}

// getCompletedBytes gets completed size
func (cp *downloadCheckpoint) getCompletedBytes() int64 {
	var completedBytes int64
	for i, part := range cp.Parts {
		if cp.PartStat[i] {
			completedBytes += part.End - part.Start + 1
		}
	}
	return completedBytes
}

// prepare initiates download tasks
func (cp *downloadCheckpoint) prepare(meta http.Header, bucket *Bucket, objectKey, filePath string, partSize int64, uRange *UnpackedRange) error {
	cp.Magic = downloadCpMagic
	cp.FilePath = filePath
	cp.Object = objectKey

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return err
	}

	cp.ObjStat.Size = objectSize
	cp.ObjStat.LastModified = meta.Get(HTTPHeaderLastModified)
	cp.ObjStat.Etag = meta.Get(HTTPHeaderEtag)

	if bucket.GetConfig().IsEnableCRC && meta.Get(HTTPHeaderKs3CRC64) != "" {
		if uRange == nil || (!uRange.HasStart && !uRange.HasEnd) {
			cp.EnableCRC = true
			cp.CRC, _ = strconv.ParseUint(meta.Get(HTTPHeaderKs3CRC64), 10, 64)
		}
	}

	cp.Parts = getDownloadParts(objectSize, partSize, uRange)
	cp.PartStat = make([]bool, len(cp.Parts))
	for i := range cp.PartStat {
		cp.PartStat[i] = false
	}

	return nil
}

func (cp *downloadCheckpoint) complete(cpFilePath, downFilepath string, disableTempFile bool) error {
	err := rename(downFilepath, cp.FilePath, disableTempFile)
	if err != nil {
		return err
	}

	return os.Remove(cpFilePath)
}

// downloadFileWithCp downloads files with checkpoint.
func (bucket Bucket) downloadFileWithCp(objectKey, filePath string, partSize int64, options []Option, cpFilePath string, routines int, uRange *UnpackedRange) error {
	disableTempFile := getDisableTempFile(options)
	tempFilePath := filePath + TempFileSuffix
	if disableTempFile {
		tempFilePath = filePath
	}
	listener := GetProgressListener(options)

	// Load checkpoint data.
	dcp := downloadCheckpoint{}

	// 判断checkpoint文件是否存在
	fileExist, _ := IsFileExist(cpFilePath)
	if fileExist {
		// Load CP data
		err := dcp.load(cpFilePath)
		if err == nil {
			// 判断.temp是否存在，若不存在，则删除checkpoint文件，重新下载
			tempFileExist, _ := IsFileExist(tempFilePath)
			if !tempFileExist {
				bucket.Client.Config.WriteLog(Info, ".temp is not exist, delete checkpoint file")
				os.Remove(cpFilePath)
				dcp = downloadCheckpoint{}
			}
		} else {
			os.Remove(cpFilePath)
		}
	}

	// Get the object detailed meta for object whole size
	// must delete header:range to get whole object size
	skipOptions := DeleteOption(options, HTTPHeaderRange)
	meta, err := bucket.GetObjectDetailedMeta(objectKey, skipOptions...)
	if err != nil {
		return err
	}

	// Load error or data invalid. Re-initialize the download.
	valid, err := dcp.isValid(meta, uRange)
	if err != nil || !valid {
		if err = dcp.prepare(meta, &bucket, objectKey, filePath, partSize, uRange); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	// Create the file if not exists, otherwise the parts download will overwrite it.
	fd, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, FilePermMode)
	if err != nil {
		return err
	}
	fd.Close()

	// Unfinished parts
	parts := dcp.todoParts()
	jobs := make(chan downloadPart, len(parts))
	results := make(chan downloadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := dcp.getCompletedBytes()
	event := newProgressEvent(TransferStartedEvent, completedBytes, dcp.ObjStat.Size, 0)
	publishProgress(listener, event)

	// Start the download workers routine
	arg := downloadWorkerArg{&bucket, objectKey, "", tempFilePath, options, downloadPartHooker, dcp.EnableCRC, listener}
	for w := 1; w <= routines; w++ {
		go downloadWorker(arg, jobs, results, failed, die)
	}

	// Concurrently downloads parts
	go downloadScheduler(jobs, parts)

	// Wait for the parts download finished
	completed := 0
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			dcp.PartStat[part.Index] = true
			dcp.Parts[part.Index].CRC64 = part.CRC64
			dcp.dump(cpFilePath)
			downBytes := part.End - part.Start + 1
			completedBytes += downBytes
			event = newProgressEvent(TransferPartEvent, completedBytes, dcp.ObjStat.Size, downBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, dcp.ObjStat.Size, 0)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, dcp.ObjStat.Size, 0)
	publishProgress(listener, event)

	if dcp.EnableCRC {
		clientCRC := combineCRCInDownloadParts(dcp.Parts)
		serverCRC, _ := strconv.ParseUint(meta.Get(HTTPHeaderKs3CRC64), 10, 64)
		err = CheckDownloadCRC(clientCRC, serverCRC)
		bucket.Client.Config.WriteLog(Debug, "check file crc64, bucketName:%s, objectKey:%s, client crc:%d, server crc:%d", bucket.BucketName, objectKey, clientCRC, serverCRC)
		if err != nil {
			return err
		}
	}

	return dcp.complete(cpFilePath, tempFilePath, disableTempFile)
}

// downloadFileWithURL 根据预签名URL下载文件
func (bucket Bucket) downloadFileWithURL(signedURL, filePath string, partSize int64, options []Option, cpFilePath string, routines int, uRange *UnpackedRange) error {
	disableTempFile := getDisableTempFile(options)
	tempFilePath := filePath + TempFileSuffix
	if disableTempFile {
		tempFilePath = filePath
	}
	listener := GetProgressListener(options)

	// Load checkpoint data.
	dcp := downloadCheckpoint{}

	if cpFilePath != "" {
		// 判断checkpoint文件是否存在
		fileExist, _ := IsFileExist(cpFilePath)
		if fileExist {
			// Load CP data
			err := dcp.load(cpFilePath)
			if err == nil {
				// 判断.temp是否存在，若不存在，则删除checkpoint文件，重新下载
				tempFileExist, _ := IsFileExist(tempFilePath)
				if !tempFileExist {
					bucket.Client.Config.WriteLog(Info, ".temp is not exist, delete checkpoint file")
					os.Remove(cpFilePath)
					dcp = downloadCheckpoint{}
				}
			} else {
				os.Remove(cpFilePath)
			}
		}
	}

	// Get the object detailed meta for object whole size
	// must delete header:range to get whole object size
	skipOptions := DeleteOption(options, HTTPHeaderRange)
	meta, err := bucket.GetObjectMetaWithURL(signedURL, skipOptions...)
	if err != nil {
		return err
	}

	// Load error or data invalid. Re-initialize the download.
	valid, err := dcp.isValid(meta, uRange)
	if err != nil || !valid {
		if err = dcp.prepare(meta, &bucket, signedURL, filePath, partSize, uRange); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	// Create the file if not exists, otherwise the parts download will overwrite it.
	fd, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, FilePermMode)
	if err != nil {
		return err
	}
	fd.Close()

	// Unfinished parts
	parts := dcp.todoParts()
	jobs := make(chan downloadPart, len(parts))
	results := make(chan downloadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := dcp.getCompletedBytes()
	event := newProgressEvent(TransferStartedEvent, completedBytes, dcp.ObjStat.Size, 0)
	publishProgress(listener, event)

	// Start the download workers routine
	arg := downloadWorkerArg{&bucket, "", signedURL, tempFilePath, options, downloadPartHooker, dcp.EnableCRC, listener}
	for w := 1; w <= routines; w++ {
		go downloadWorker(arg, jobs, results, failed, die)
	}

	// Concurrently downloads parts
	go downloadScheduler(jobs, parts)

	// Wait for the parts download finished
	completed := 0
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			dcp.PartStat[part.Index] = true
			dcp.Parts[part.Index].CRC64 = part.CRC64
			dcp.dump(cpFilePath)
			downBytes := part.End - part.Start + 1
			completedBytes += downBytes
			event = newProgressEvent(TransferPartEvent, completedBytes, dcp.ObjStat.Size, downBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, dcp.ObjStat.Size, 0)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, dcp.ObjStat.Size, 0)
	publishProgress(listener, event)

	if dcp.EnableCRC {
		clientCRC := combineCRCInDownloadParts(dcp.Parts)
		serverCRC, _ := strconv.ParseUint(meta.Get(HTTPHeaderKs3CRC64), 10, 64)
		err = CheckDownloadCRC(clientCRC, serverCRC)
		bucket.Client.Config.WriteLog(Debug, "check file crc64, bucketName:%s, signedURL:%s, client crc:%d, server crc:%d", bucket.BucketName, signedURL, clientCRC, serverCRC)
		if err != nil {
			return err
		}
	}

	return dcp.complete(cpFilePath, tempFilePath, disableTempFile)
}
