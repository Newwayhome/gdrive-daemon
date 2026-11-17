package main

// High-speed uploader for Google Drive resumable sessions.
//
// Drive stores resumable-session data strictly in order: a PUT whose
// Content-Range starts past the acknowledged offset is silently discarded
// with a 308, so firing concurrent out-of-range PUTs wastes bandwidth.
// This uploader keeps the session strictly in order and maximizes throughput
// with:
//  - Adaptive large chunks (1024 MB for >=10GB files) to halve GFE commit latency
//  - Reusable zero-allocation chunk buffer pool to eliminate GC pauses
//  - Multi-MB copy buffer pool for network streaming
//  - HTTP/1.1 wire mode bypassing HTTP/2 stream flow-control window caps
//  - Windows TCP stack auto-tuning compatibility for 10Gbps links
//  - Pre-fetching read-ahead pipeline overlapping disk I/O with network transfer
//  - Byte-accurate wire progress tracking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var rangeAckRe = regexp.MustCompile(`bytes=0-(\d+)`)

const chunkMultiple = 256 << 10 // Drive requires chunk sizes multiple of 256KB

var copyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 4*1024*1024)
		return &b
	},
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func (d *Daemon) addUploaded(taskID string, uploaded, total int64) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	state, exists := d.progressMap[taskID]
	if !exists || state == nil {
		return
	}
	state.UploadedBytes = uploaded
	if total > 0 {
		state.Percentage = float64(uploaded) / float64(total) * 100.0
	}
	dt := now.Sub(state.speedMark).Seconds()
	if dt >= 0.25 {
		if dt > 0 {
			state.SpeedBps = float64(uploaded-state.speedMarkByte) / dt
		}
		state.speedMark = now
		state.speedMarkByte = uploaded
		if state.SpeedBps > 0 && total > uploaded {
			state.EtaSeconds = float64(total-uploaded) / state.SpeedBps
		} else {
			state.EtaSeconds = 0
		}
	}
}

// wireCounter wraps a chunk body and reports running totals off the wire,
// so progress moves smoothly instead of jumping one chunk at a time.
type wireCounter struct {
	r        io.Reader
	base     int64
	total    int64
	taskID   string
	daemon   *Daemon
	n        int64
	lastBeat time.Time
}

func (wc *wireCounter) Read(p []byte) (int, error) {
	n, err := wc.r.Read(p)
	if n > 0 {
		wc.n += int64(n)
		now := time.Now()
		if now.Sub(wc.lastBeat) >= 150*time.Millisecond {
			wc.lastBeat = now
			wc.daemon.addUploaded(wc.taskID, wc.base+wc.n, wc.total)
		}
	}
	return n, err
}

func (wc *wireCounter) WriteTo(w io.Writer) (int64, error) {
	bufPtr := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufPtr)
	buf := *bufPtr

	var written int64
	for {
		nr, rerr := wc.r.Read(buf)
		if nr > 0 {
			nw, werr := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				wc.n += int64(nw)
				now := time.Now()
				if now.Sub(wc.lastBeat) >= 150*time.Millisecond {
					wc.lastBeat = now
					wc.daemon.addUploaded(wc.taskID, wc.base+wc.n, wc.total)
				}
			}
			if werr != nil {
				return written, werr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return written, rerr
		}
	}
	return written, nil
}

// querySession asks the session how many bytes it currently holds.
func querySession(ctx context.Context, client *http.Client, sessionURI string, totalSize int64) (int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURI, nil)
	if err != nil {
		return -1, "", err
	}
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
	resp, err := client.Do(req)
	if err != nil {
		return -1, "", err
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		var res struct {
			Id string `json:"id"`
		}
		if json.Unmarshal(respBody, &res) == nil && res.Id != "" {
			return totalSize, res.Id, nil
		}
		return -1, "", fmt.Errorf("session completed without file id (status %d)", resp.StatusCode)
	}
	if resp.StatusCode == 308 {
		if m := rangeAckRe.FindStringSubmatch(resp.Header.Get("Range")); m != nil {
			parsed, perr := strconv.ParseInt(m[1], 10, 64)
			if perr != nil {
				return 0, "", nil
			}
			return parsed + 1, "", nil
		}
		return 0, "", nil
	}
	return -1, "", fmt.Errorf("session status query returned %d %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
}

func (d *Daemon) parallelResumableUpload(ctx context.Context, metaClient, uploadClient *http.Client, file *os.File, totalSize int64, fileName, folderID, taskID string) (string, error) {
	if uploadClient == nil {
		uploadClient = metaClient
	}
	if metaClient == nil {
		metaClient = uploadClient
	}
	if totalSize <= 0 {
		return "", fmt.Errorf("empty file")
	}

	meta := map[string]interface{}{"name": fileName}
	if folderID != "" {
		meta["parents"] = []string{folderID}
	}
	metaJSON, _ := json.Marshal(meta)
	initReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true",
		bytes.NewReader(metaJSON))
	if err != nil {
		return "", err
	}
	initReq.Header.Set("Content-Type", "application/json; charset=UTF-8")
	initReq.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	initReq.Header.Set("X-Upload-Content-Length", strconv.FormatInt(totalSize, 10))
	initResp, err := metaClient.Do(initReq)
	if err != nil {
		return "", err
	}
	sessionURI := initResp.Header.Get("Location")
	initBody, _ := io.ReadAll(io.LimitReader(initResp.Body, 1<<16))
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK || sessionURI == "" {
		return "", fmt.Errorf("resumable session init failed: %d %s", initResp.StatusCode, strings.TrimSpace(string(initBody)))
	}

	defaultChunkMB := 1024
	if totalSize >= 4<<30 { // >= 4 GB: 1024 MB chunks keep wire saturated with minimal GFE pauses
		defaultChunkMB = 1024
	} else if totalSize >= 1<<30 { // 1 - 4 GB
		defaultChunkMB = 512
	} else if totalSize >= 256<<20 { // 256 MB - 1 GB
		defaultChunkMB = 256
	} else {
		defaultChunkMB = 128
	}
	chunkSize := int64(envInt("GDRIVE_UPLOAD_CHUNK_MB", defaultChunkMB)) << 20
	if chunkSize < 16<<20 {
		chunkSize = 16 << 20
	}
	chunkSize -= chunkSize % chunkMultiple
	readAhead := envInt("GDRIVE_UPLOAD_READAHEAD", 2)
	if readAhead < 1 {
		readAhead = 1
	}
	log.Printf("⚡ In-order chunk upload for [%s]: %d MB chunks, read-ahead %d (HTTP/1.1 wire mode)", fileName, chunkSize>>20, readAhead)

	type chunkBuf struct {
		start   int64
		end     int64 // inclusive
		data    []byte
		rawBuf  []byte
		readErr error
	}

	// Zero-allocation buffer pool: recycle (readAhead + 1) pre-allocated memory buffers
	bufPool := make(chan []byte, readAhead+1)
	for i := 0; i < readAhead+1; i++ {
		bufPool <- make([]byte, chunkSize)
	}

	chunksCh := make(chan *chunkBuf, readAhead)
	go func() {
		defer close(chunksCh)
		for start := int64(0); start < totalSize; start += chunkSize {
			end := start + chunkSize - 1
			if end >= totalSize {
				end = totalSize - 1
			}
			size := end - start + 1

			var buf []byte
			select {
			case buf = <-bufPool:
			case <-ctx.Done():
				return
			}

			data := buf[:size]
			_, rerr := file.ReadAt(data, start)
			cb := &chunkBuf{start: start, end: end, data: data, rawBuf: buf, readErr: rerr}
			select {
			case chunksCh <- cb:
			case <-ctx.Done():
				return
			}
			if rerr != nil && rerr != io.EOF {
				return
			}
		}
	}()

	// put sends one byte range and interprets the resumable response.
	put := func(data []byte, start, end int64) (ackedTo int64, fileID string, err error) {
		size := end - start + 1
		body := &wireCounter{
			r:      bytes.NewReader(data[len(data)-int(size):]),
			base:   start,
			total:  totalSize,
			taskID: taskID,
			daemon: d,
		}
		req, rerr := http.NewRequestWithContext(ctx, http.MethodPut, sessionURI, body)
		if rerr != nil {
			return -1, "", rerr
		}
		req.ContentLength = size
		req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		resp, derr := uploadClient.Do(req)
		if derr != nil {
			return -1, "", derr
		}
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
			var res struct {
				Id string `json:"id"`
			}
			if json.Unmarshal(respBody, &res) == nil && res.Id != "" {
				return end + 1, res.Id, nil
			}
			return -1, "", fmt.Errorf("final chunk status %d without file id", resp.StatusCode)
		case resp.StatusCode == 308:
			// Only the contiguous prefix up to the acked Range is stored.
			if m := rangeAckRe.FindStringSubmatch(resp.Header.Get("Range")); m != nil {
				parsed, _ := strconv.ParseInt(m[1], 10, 64)
				return parsed + 1, "", nil
			}
			return start, "", nil
		default:
			return -1, "", fmt.Errorf("chunk PUT returned %d %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		}
	}

	var (
		resultID string
		lastErr  error
		offset   = int64(0) // next byte the session needs
		current  *chunkBuf  // chunk currently being filled into the session
	)

	recycleCurrent := func() {
		if current != nil && current.rawBuf != nil {
			select {
			case bufPool <- current.rawBuf:
			default:
			}
			current = nil
		}
	}

	defer func() {
		recycleCurrent()
		go func() {
			for c := range chunksCh {
				if c != nil && c.rawBuf != nil {
					select {
					case bufPool <- c.rawBuf:
					default:
					}
				}
			}
		}()
	}()

	staleRetries := 0
	for offset < totalSize {
		// Advance past chunks the session already holds (post-resync).
		if current != nil && offset > current.end {
			recycleCurrent()
		}
		if current == nil {
			select {
			case current = <-chunksCh:
			case <-ctx.Done():
				lastErr = ctx.Err()
			}
			if current == nil {
				break
			}
			if current.readErr != nil && current.readErr != io.EOF {
				lastErr = current.readErr
				break
			}
			if current.end < offset {
				recycleCurrent()
				continue // already stored by a previous attempt
			}
		}

		var (
			acked int64
			fid   string
			uerr  error
		)
		for attempt := 0; attempt < 5; attempt++ {
			if offset > current.end {
				break // resync moved the session past this chunk
			}
			start := offset
			if start < current.start {
				start = current.start
			}
			data := current.data
			if start != current.start {
				size := current.end - start + 1
				fresh := make([]byte, size)
				if _, rerr := file.ReadAt(fresh, start); rerr != nil && rerr != io.EOF {
					uerr = rerr
					break
				}
				data = fresh
			}
			acked, fid, uerr = put(data, start, current.end)
			if uerr == nil {
				break
			}
			// Transport or server error: resync with the session
			time.Sleep(time.Duration(attempt+1) * time.Second)
			if q, _, qerr := querySession(ctx, uploadClient, sessionURI, totalSize); qerr == nil && q >= 0 {
				offset = q
			}
		}
		if uerr != nil && fid == "" && offset <= current.end {
			lastErr = uerr
			break
		}
		if fid != "" {
			resultID = fid
			offset = totalSize
			break
		}
		if acked > offset {
			d.addUploaded(taskID, acked, totalSize)
			offset = acked
			staleRetries = 0
		} else if acked == offset {
			staleRetries++
			if staleRetries > 8 {
				lastErr = fmt.Errorf("session made no progress at offset %d", offset)
				break
			}
		}
	}

	if resultID != "" {
		return resultID, nil
	}
	if ctx.Err() != nil {
		return "", fmt.Errorf("upload cancelled at offset %d", offset)
	}

	// Finalize check
	q, fid, qerr := querySession(ctx, uploadClient, sessionURI, totalSize)
	if qerr == nil && fid != "" {
		return fid, nil
	}
	if lastErr != nil {
		return "", fmt.Errorf("chunked upload failed at offset %d: %w", offset, lastErr)
	}
	if qerr != nil {
		return "", qerr
	}
	return "", fmt.Errorf("upload session did not complete (session holds %d/%d bytes)", q, totalSize)
}
