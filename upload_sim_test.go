package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
)

// simDrive mimics Google's resumable protocol: strictly in-order chunks,
// 308 + Range for partial acks, 200 + JSON id when the last byte lands.
func simDrive(t *testing.T, chunkRejectEvery int, fileSize int64) (*httptest.Server, *int64, *[]int64) {
	var (
		mu      sync.Mutex
		offset  int64
		order   []int64
		rejectN int
	)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", srv.URL+"/session")
			w.WriteHeader(http.StatusOK)
			return
		}
		cr := regexp.MustCompile(`bytes (\d+)-(\d+)/(\d+)`).FindStringSubmatch(r.Header.Get("Content-Range"))
		if cr == nil {
			// status query: completed sessions return the file id
			mu.Lock()
			done := offset >= fileSize
			mu.Unlock()
			if done {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"id":"file123"}`)
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", offset-1))
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		}
		start, _ := strconv.ParseInt(cr[1], 10, 64)
		end, _ := strconv.ParseInt(cr[2], 10, 64)
		total, _ := strconv.ParseInt(cr[3], 10, 64)

		mu.Lock()
		inOrder := start == offset
		if inOrder {
			order = append(order, start)
			// optionally simulate a short-ack (server stored less than sent)
			rejectN++
			if chunkRejectEvery > 0 && rejectN%chunkRejectEvery == 0 && end-start > 1024 {
				offset = start + (end-start)/2
			} else {
				offset = end + 1
			}
		}
		cur := offset
		mu.Unlock()

		if !inOrder {
			t.Errorf("out-of-order PUT: start=%d but session offset=%d", start, cur)
		}
		if cur >= total {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"file123"}`)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", cur-1))
		w.WriteHeader(http.StatusPermanentRedirect)
	}))
	return srv, &offset, &order
}

func TestInOrderChunkUpload(t *testing.T) {
	srv, _, order := simDrive(t, 0, int64(5*1024*1024))
	defer srv.Close()

	dir := t.TempDir()
	fpath := filepath.Join(dir, "test.bin")
	data := make([]byte, 5*1024*1024) // 5MB
	for i := range data {
		data[i] = byte(i)
	}
	os.WriteFile(fpath, data, 0644)
	f, _ := os.Open(fpath)
	defer f.Close()

	d := &Daemon{progressMap: map[string]*ProgressState{}}
	// Point the uploader at the simulated endpoint by overriding the client's transport base.
	client := &http.Client{Transport: &rewriteTransport{base: http.DefaultTransport, from: "https://www.googleapis.com", to: srv.URL}}

	fileID, err := d.parallelResumableUpload(context.Background(), client, client, f, int64(len(data)), "test.bin", "", "task-sim")
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	if fileID != "file123" {
		t.Fatalf("unexpected file id: %s", fileID)
	}
	if len(*order) == 0 {
		t.Fatal("no chunks uploaded")
	}
	prev := int64(-1)
	for _, s := range *order {
		if s <= prev {
			t.Fatalf("chunks not strictly in order: %v", *order)
		}
		prev = s
	}
	t.Logf("OK: %d chunks uploaded strictly in order", len(*order))
}

type rewriteTransport struct {
	base     http.RoundTripper
	from, to string
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "www.googleapis.com" {
		req.URL.Scheme = "http"
		req.URL.Host = rt.to[len("http://"):]
	}
	return rt.base.RoundTrip(req)
}

func TestMultiChunkWithShortAcks(t *testing.T) {
	srv, _, order := simDrive(t, 2, 40*1024*1024) // every 2nd PUT short-acks
	defer srv.Close()

	dir := t.TempDir()
	fpath := filepath.Join(dir, "multi.bin")
	size := 40 * 1024 * 1024
	data := make([]byte, size)
	for i := 0; i < size; i += 4096 {
		data[i] = byte(i / 4096)
	}
	os.WriteFile(fpath, data, 0644)
	f, _ := os.Open(fpath)
	defer f.Close()

	t.Setenv("GDRIVE_UPLOAD_CHUNK_MB", "16")
	d := &Daemon{progressMap: map[string]*ProgressState{}}
	client := &http.Client{Transport: &rewriteTransport{base: http.DefaultTransport, from: "https://www.googleapis.com", to: srv.URL}}

	fileID, err := d.parallelResumableUpload(context.Background(), client, client, f, int64(size), "multi.bin", "", "task-multi")
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	if fileID != "file123" {
		t.Fatalf("unexpected file id: %s", fileID)
	}
	if len(*order) < 3 {
		t.Fatalf("expected >=3 in-order chunk starts, got %d", len(*order))
	}
	prev := int64(-1)
	for _, s := range *order {
		if s <= prev {
			t.Fatalf("chunks not strictly in order: %v", *order)
		}
		prev = s
	}
	t.Logf("OK: %d in-order PUTs across 3 chunks with short-ack resyncs", len(*order))
}
