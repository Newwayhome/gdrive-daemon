package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type UploadRequest struct {
	TaskID   string `json:"task_id"`
	FilePath string `json:"file_path"`
	FolderID string `json:"folder_id,omitempty"`
	CredPath string `json:"cred_path,omitempty"`
}

type UploadResponse struct {
	FileID      string `json:"file_id"`
	WebViewLink string `json:"web_view_link"`
	Error       string `json:"error,omitempty"`
}

type ProgressState struct {
	TaskID        string  `json:"task_id"`
	UploadedBytes int64   `json:"uploaded_bytes"`
	TotalBytes    int64   `json:"total_bytes"`
	Percentage    float64 `json:"percentage"`
	SpeedBps      float64 `json:"speed_bps"`
	EtaSeconds    float64 `json:"eta_seconds"`
	Status        string  `json:"status"`
	Error         string  `json:"error,omitempty"`

	speedMark     time.Time // unexported
	speedMarkByte int64
}

type PythonUserToken struct {
	Token        string   `json:"token"`
	RefreshToken string   `json:"refresh_token"`
	TokenURI     string   `json:"token_uri"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	Scopes       []string `json:"scopes"`
}

type Daemon struct {
	mu              sync.RWMutex
	progressMap     map[string]*ProgressState
	customTransport *http.Transport
	h1Transport     *http.Transport
}

type DaemonConfig struct {
	FolderID string `json:"folder_id"`
	Port     string `json:"port"`
	CredPath string `json:"cred_path"`
}

func loadConfig() DaemonConfig {
	var cfg DaemonConfig
	candidates := []string{"config.json", "../config.json"}
	for _, c := range candidates {
		if data, err := os.ReadFile(c); err == nil {
			_ = json.Unmarshal(data, &cfg)
			break
		}
	}
	return cfg
}

func parseFolderID(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		cfg := loadConfig()
		if cfg.FolderID != "" {
			return parseFolderID(cfg.FolderID)
		}
		if envID := os.Getenv("GDRIVE_DEFAULT_FOLDER_ID"); envID != "" {
			return parseFolderID(envID)
		}
		if envID := os.Getenv("GDRIVE_FOLDER_ID"); envID != "" {
			return parseFolderID(envID)
		}
		return ""
	}
	// Extract folder ID if user passed a full Google Drive URL
	if strings.Contains(input, "drive.google.com") {
		if idx := strings.Index(input, "/folders/"); idx != -1 {
			part := input[idx+len("/folders/"):]
			if qIdx := strings.IndexAny(part, "?/&#"); qIdx != -1 {
				return part[:qIdx]
			}
			return part
		}
		if idx := strings.Index(input, "id="); idx != -1 {
			part := input[idx+3:]
			if qIdx := strings.IndexAny(part, "?/&#"); qIdx != -1 {
				return part[:qIdx]
			}
			return part
		}
	}
	return input
}

type ProgressReader struct {
	r          io.Reader
	taskID     string
	totalBytes int64
	daemon     *Daemon
	uploaded   int64
	lastTime   time.Time
	lastBytes  int64
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 {
		pr.uploaded += int64(n)
		now := time.Now()

		pr.daemon.mu.Lock()
		state, exists := pr.daemon.progressMap[pr.taskID]
		if !exists {
			state = &ProgressState{TaskID: pr.taskID, TotalBytes: pr.totalBytes, Status: "uploading"}
			pr.daemon.progressMap[pr.taskID] = state
		}

		state.UploadedBytes = pr.uploaded
		if pr.totalBytes > 0 {
			state.Percentage = (float64(pr.uploaded) / float64(pr.totalBytes)) * 100.0
		}

		dt := now.Sub(pr.lastTime).Seconds()
		if dt >= 0.15 {
			db := pr.uploaded - pr.lastBytes
			if dt > 0 {
				state.SpeedBps = float64(db) / dt
			}
			pr.lastBytes = pr.uploaded
			pr.lastTime = now

			if state.SpeedBps > 0 && pr.totalBytes > pr.uploaded {
				rem := float64(pr.totalBytes - pr.uploaded)
				state.EtaSeconds = rem / state.SpeedBps
			} else {
				state.EtaSeconds = 0
			}
		}
		pr.daemon.mu.Unlock()
	}
	return n, err
}

func findCredentials(customPath string) (string, string) {
	if customPath != "" && strings.Contains(customPath, "token.json") {
		if _, err := os.Stat(customPath); err == nil {
			return customPath, "user"
		}
	}

	tokenCandidates := []string{
		"token.json",
		"../token.json",
		"../../token.json",
	}
	for _, c := range tokenCandidates {
		if _, err := os.Stat(c); err == nil {
			return c, "user"
		}
	}

	if customPath != "" {
		if _, err := os.Stat(customPath); err == nil {
			return customPath, "sa"
		}
	}

	saCandidates := []string{
		"credentials.json",
		"../credentials.json",
		"../../credentials.json",
	}
	for _, c := range saCandidates {
		if _, err := os.Stat(c); err == nil {
			return c, "sa"
		}
	}

	return "", ""
}

func (d *Daemon) getAuthenticatedClients(ctx context.Context, credPath string) (*http.Client, *http.Client, string, error) {
	credFile, credType := findCredentials(credPath)
	if credFile == "" {
		return nil, nil, "", fmt.Errorf("no token.json or credentials.json found")
	}

	log.Printf("🔑 Loading GDrive credentials from [%s] (type: %s)", credFile, credType)

	data, err := os.ReadFile(credFile)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to read credential file: %w", err)
	}

	var tokenSource oauth2.TokenSource
	accountName := ""

	if credType == "user" {
		var ut PythonUserToken
		if err := json.Unmarshal(data, &ut); err == nil && ut.RefreshToken != "" && ut.ClientID != "" {
			cfg := &oauth2.Config{
				ClientID:     ut.ClientID,
				ClientSecret: ut.ClientSecret,
				Endpoint:     google.Endpoint,
				Scopes:       ut.Scopes,
			}
			if len(cfg.Scopes) == 0 {
				cfg.Scopes = []string{drive.DriveScope}
			}
			tok := &oauth2.Token{
				AccessToken:  ut.Token,
				RefreshToken: ut.RefreshToken,
				TokenType:    "Bearer",
				Expiry:       time.Now().Add(-1 * time.Hour),
			}
			tokenSource = cfg.TokenSource(ctx, tok)
			freshTok, err := tokenSource.Token()
			if err == nil && freshTok != nil {
				log.Printf("🔑 Refreshed OAuth Access Token! Valid until: %v", freshTok.Expiry.Format(time.Kitchen))
			}
			accountName = "User OAuth (" + ut.ClientID[:10] + "...)"
		}
	}

	if tokenSource == nil {
		creds, err := google.CredentialsFromJSON(ctx, data, drive.DriveScope)
		if err != nil {
			return nil, nil, "", fmt.Errorf("failed to parse credentials JSON: %w", err)
		}
		tokenSource = creds.TokenSource

		var js map[string]interface{}
		if json.Unmarshal(data, &js) == nil {
			if email, ok := js["client_email"].(string); ok {
				accountName = email
			}
		}
	}

	metaClient := &http.Client{
		Transport: &oauth2.Transport{
			Source: tokenSource,
			Base:   d.customTransport,
		},
		Timeout: 0,
	}

	uploadBase := d.h1Transport
	if uploadBase == nil {
		uploadBase = d.customTransport
	}
	uploadClient := &http.Client{
		Transport: &oauth2.Transport{
			Source: tokenSource,
			Base:   uploadBase,
		},
		Timeout: 0,
	}

	return metaClient, uploadClient, accountName, nil
}

func (d *Daemon) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (d *Daemon) handleProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "task_id parameter required"})
		return
	}

	d.mu.RLock()
	state, ok := d.progressMap[taskID]
	var copyState ProgressState
	if ok && state != nil {
		copyState = *state
	}
	d.mu.RUnlock()

	if !ok {
		json.NewEncoder(w).Encode(ProgressState{TaskID: taskID, Status: "not_found"})
		return
	}
	json.NewEncoder(w).Encode(copyState)
}

func (d *Daemon) handleRemoveTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	taskID := r.URL.Query().Get("task_id")
	if taskID != "" {
		d.mu.Lock()
		delete(d.progressMap, taskID)
		d.mu.Unlock()
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "removed"})
}

func (d *Daemon) handleAllProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	d.mu.RLock()
	defer d.mu.RUnlock()

	tasks := make([]ProgressState, 0, len(d.progressMap))
	for _, st := range d.progressMap {
		if st != nil {
			tasks = append(tasks, *st)
		}
	}
	json.NewEncoder(w).Encode(tasks)
}

func (d *Daemon) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/dashboard" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(htmlDashboard))
}

func (d *Daemon) handleUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(UploadResponse{Error: "Method not allowed"})
		return
	}

	var req UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(UploadResponse{Error: err.Error()})
		return
	}

	if req.FilePath == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(UploadResponse{Error: "file_path is required"})
		return
	}
	req.FolderID = parseFolderID(req.FolderID)

	file, err := os.Open(req.FilePath)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(UploadResponse{Error: fmt.Sprintf("cannot open file: %v", err)})
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(UploadResponse{Error: fmt.Sprintf("cannot stat file: %v", err)})
		return
	}

	totalSize := fileInfo.Size()
	fileName := filepath.Base(req.FilePath)
	ctx := r.Context()

	metaClient, uploadClient, accountName, err := d.getAuthenticatedClients(ctx, req.CredPath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(UploadResponse{Error: fmt.Sprintf("gdrive auth error: %v", err)})
		return
	}

	taskID := req.TaskID
	if taskID == "" {
		taskID = fileName
	}

	d.mu.Lock()
	d.progressMap[taskID] = &ProgressState{
		TaskID:        taskID,
		TotalBytes:    totalSize,
		UploadedBytes: 0,
		Percentage:    0.0,
		Status:        "uploading",
	}
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		if state, ok := d.progressMap[taskID]; ok && state != nil {
			if state.Status != "failed" {
				state.Status = "completed"
				state.Percentage = 100.0
				state.UploadedBytes = totalSize
				state.EtaSeconds = 0
			}
		}
		d.mu.Unlock()
	}()

	log.Printf("🚀 Starting Ultra-Fast Stream Upload for [%s] (%0.2f GB) using %s", fileName, float64(totalSize)/(1024*1024*1024), accountName)

	if fileID, perr := d.parallelResumableUpload(ctx, metaClient, uploadClient, file, totalSize, fileName, req.FolderID, taskID); perr == nil {
		srv, serr := drive.NewService(ctx, option.WithHTTPClient(metaClient))
		if serr == nil {
			_, _ = srv.Permissions.Create(fileID, &drive.Permission{
				Role: "reader",
				Type: "anyone",
			}).SupportsAllDrives(true).Do()
		}
		webViewLink := fmt.Sprintf("https://drive.google.com/file/d/%s/view?usp=sharing", fileID)
		log.Printf("⚡ Parallel Upload Complete! [%s] -> File ID: %s", fileName, fileID)
		json.NewEncoder(w).Encode(UploadResponse{FileID: fileID, WebViewLink: webViewLink})
		return
	} else {
		log.Printf("↩️ Parallel upload unavailable (%v); using sequential stream.", perr)
		if _, serr := file.Seek(0, io.SeekStart); serr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(UploadResponse{Error: fmt.Sprintf("cannot rewind file: %v", serr)})
			return
		}
	}

	srv, serr := drive.NewService(ctx, option.WithHTTPClient(metaClient))
	if serr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(UploadResponse{Error: fmt.Sprintf("drive service error: %v", serr)})
		return
	}

	pr := &ProgressReader{
		r:          file,
		taskID:     taskID,
		totalBytes: totalSize,
		daemon:     d,
		lastTime:   time.Now(),
	}

	bufReader := bufio.NewReaderSize(pr, 64*1024*1024)

	driveFile := &drive.File{
		Name: fileName,
	}
	if req.FolderID != "" {
		driveFile.Parents = []string{req.FolderID}
	}

	call := srv.Files.Create(driveFile).
		Media(bufReader, googleapi.ChunkSize(256*1024*1024)).
		SupportsAllDrives(true)

	res, err := call.Do()

	if err != nil && req.FolderID != "" {
		log.Printf("⚠️ Target folder '%s' upload failed (%v). Retrying upload to root My Drive...", req.FolderID, err)
		file.Seek(0, 0)
		pr.uploaded = 0
		pr.lastBytes = 0
		pr.lastTime = time.Now()
		bufReader.Reset(pr)

		rootDriveFile := &drive.File{Name: fileName}
		res, err = srv.Files.Create(rootDriveFile).
			Media(bufReader, googleapi.ChunkSize(256*1024*1024)).
			SupportsAllDrives(true).
			Do()
	}

	d.mu.Lock()
	state := d.progressMap[taskID]
	if err != nil {
		errMsg := err.Error()
		if state != nil {
			state.Status = "failed"
			state.Error = errMsg
		}
		d.mu.Unlock()

		log.Printf("❌ Upload failed for %s: %v", fileName, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(UploadResponse{Error: errMsg})
		return
	}

	if state != nil {
		state.Percentage = 100.0
		state.UploadedBytes = totalSize
		state.Status = "completed"
	}
	d.mu.Unlock()

	if res != nil && res.Id != "" {
		_, _ = srv.Permissions.Create(res.Id, &drive.Permission{
			Role: "reader",
			Type: "anyone",
		}).SupportsAllDrives(true).Do()
	}

	webViewLink := fmt.Sprintf("https://drive.google.com/file/d/%s/view?usp=sharing", res.Id)
	log.Printf("⚡ Upload Complete! [%s] -> File ID: %s", fileName, res.Id)

	json.NewEncoder(w).Encode(UploadResponse{
		FileID:      res.Id,
		WebViewLink: webViewLink,
	})
}

const htmlDashboard = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GDrive Upload Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
  body { background: #0f172a; color: #f8fafc; padding: 2rem; display: flex; flex-direction: column; align-items: center; min-height: 100vh; }
  .container { max-width: 900px; width: 100%; }
  header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; border-bottom: 1px solid #1e293b; padding-bottom: 1rem; }
  h1 { font-size: 1.5rem; font-weight: 700; color: #38bdf8; display: flex; align-items: center; gap: 0.5rem; }
  .status-badge { background: #166534; color: #4ade80; padding: 0.25rem 0.75rem; border-radius: 9999px; font-size: 0.875rem; font-weight: 600; }
  
  .analytics-bar { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 1.5rem; }
  .analytics-box { background: #1e293b; padding: 1rem; border-radius: 0.75rem; border: 1px solid #334155; text-align: center; }
  .analytics-label { color: #94a3b8; font-size: 0.75rem; font-weight: 700; margin-bottom: 0.25rem; letter-spacing: 0.05em; }
  .analytics-value { font-size: 1.35rem; font-weight: 800; color: #38bdf8; }

  .card { background: #1e293b; border-radius: 0.75rem; padding: 1.5rem; margin-bottom: 1rem; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); border: 1px solid #334155; }
  .task-header { display: flex; justify-content: space-between; margin-bottom: 0.75rem; align-items: center; }
  .task-title { font-weight: 600; font-size: 1.1rem; color: #f1f5f9; word-break: break-all; }
  .task-status { font-size: 0.85rem; font-weight: 600; text-transform: uppercase; padding: 0.2rem 0.5rem; border-radius: 0.25rem; }
  .status-uploading { background: #0284c7; color: #e0f2fe; }
  .status-completed { background: #15803d; color: #dcfce7; }
  .status-failed { background: #991b1b; color: #fee2e2; }
  
  .remove-btn { background: #334155; color: #94a3b8; border: none; width: 26px; height: 26px; border-radius: 50%; cursor: pointer; font-weight: bold; font-size: 0.85rem; display: flex; align-items: center; justify-content: center; transition: all 0.2s; }
  .remove-btn:hover { background: #ef4444; color: #ffffff; }

  .progress-bar-bg { width: 100%; height: 14px; background: #334155; border-radius: 7px; overflow: hidden; margin: 0.75rem 0; }
  .progress-bar-fill { height: 100%; background: linear-gradient(90deg, #38bdf8, #818cf8); width: 0%; transition: width 0.3s; }
  
  .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.75rem; margin-top: 0.75rem; }
  .stat-box { background: #0f172a; padding: 0.5rem 0.75rem; border-radius: 0.5rem; }
  .stat-label { color: #94a3b8; font-size: 0.75rem; margin-bottom: 0.25rem; }
  .stat-value { font-weight: 600; font-size: 0.95rem; color: #f8fafc; }
  .link-box { margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #334155; word-break: break-all; font-size: 0.9rem; }
  .link-box a { color: #38bdf8; text-decoration: none; font-weight: 500; }
  .link-box a:hover { text-decoration: underline; }
  .empty { text-align: center; padding: 3rem; color: #64748b; font-size: 1.1rem; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>⚡ GDrive Upload Dashboard</h1>
    <span class="status-badge">● ONLINE</span>
  </header>
  
  <div class="analytics-bar">
    <div class="analytics-box">
      <div class="analytics-label">🚀 ACTIVE TRANSFERS</div>
      <div class="analytics-value" id="statActive">0</div>
    </div>
    <div class="analytics-box">
      <div class="analytics-label">⚡ LIVE SPEED</div>
      <div class="analytics-value" id="statTotalSpeed">0.00 MB/s</div>
    </div>
    <div class="analytics-box">
      <div class="analytics-label">💾 TOTAL TRANSFERRED</div>
      <div class="analytics-value" id="statTotalData">0 B</div>
    </div>
  </div>

  <div id="taskList"></div>
</div>
<script>
const removedTasks = new Set();
const taskMaxUploaded = {};

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
function formatTime(secs) {
  if (!secs || secs <= 0) return '0s';
  const m = Math.floor(secs / 60);
  const s = Math.floor(secs % 60);
  return (m > 0 ? m + 'm ' : '') + s + 's';
}
async function removeTask(taskId) {
  removedTasks.add(taskId);
  const card = document.getElementById('card-' + taskId);
  if (card) card.remove();
  try {
    await fetch('/remove_task?task_id=' + encodeURIComponent(taskId), { method: 'POST' });
  } catch (e) { console.error(e); }
}
async function fetchProgress() {
  try {
    const res = await fetch('/all_progress');
    const tasks = await res.json();
    const list = document.getElementById('taskList');

    let activeCount = 0;
    let totalSpeedBps = 0;
    let grandTotalBytes = 0;

    tasks.forEach(t => {
      const currentUp = t.uploaded_bytes || 0;
      if (!taskMaxUploaded[t.task_id] || currentUp > taskMaxUploaded[t.task_id]) {
        taskMaxUploaded[t.task_id] = currentUp;
      }
    });

    for (const tid in taskMaxUploaded) {
      grandTotalBytes += taskMaxUploaded[tid];
    }
    document.getElementById('statTotalData').innerText = formatBytes(grandTotalBytes);

    if (!tasks || tasks.length === 0) {
      if (list.children.length === 0) {
        list.innerHTML = '<div class="empty">No active uploads. Submit a file to get started!</div>';
      }
      document.getElementById('statActive').innerText = '0';
      document.getElementById('statTotalSpeed').innerText = '0.00 MB/s';
      return;
    }

    const empty = list.querySelector('.empty');
    if (empty) empty.remove();

    tasks.forEach(t => {
      if (removedTasks.has(t.task_id)) return;
      if (t.status === 'uploading') {
        activeCount++;
        totalSpeedBps += (t.speed_bps || 0);
      }
    });

    document.getElementById('statActive').innerText = activeCount;
    document.getElementById('statTotalSpeed').innerText = (totalSpeedBps / (1024*1024)).toFixed(2) + ' MB/s';

    tasks.forEach(t => {
      if (removedTasks.has(t.task_id)) return;

      const pct = (t.percentage || 0).toFixed(1);
      const speedMB = ((t.speed_bps || 0) / (1024*1024)).toFixed(2);
      const statusClass = 'status-' + (t.status || 'uploading');

      let card = document.getElementById('card-' + t.task_id);
      if (!card) {
        card = document.createElement('div');
        card.id = 'card-' + t.task_id;
        card.className = 'card';
        list.appendChild(card);
      }

      card.innerHTML = 
        '<div class="task-header">' +
          '<div class="task-title">' + t.task_id + '</div>' +
          '<div style="display:flex; align-items:center; gap:0.75rem;">' +
            '<div class="task-status ' + statusClass + '">' + t.status + '</div>' +
            '<button class="remove-btn" onclick="removeTask(\'' + t.task_id + '\')" title="Remove task">✕</button>' +
          '</div>' +
        '</div>' +
        '<div class="progress-bar-bg">' +
          '<div class="progress-bar-fill" style="width: ' + pct + '%"></div>' +
        '</div>' +
        '<div class="stats-grid">' +
          '<div class="stat-box"><div class="stat-label">PROGRESS</div><div class="stat-value">' + pct + '%</div></div>' +
          '<div class="stat-box"><div class="stat-label">SPEED</div><div class="stat-value">' + speedMB + ' MB/s</div></div>' +
          '<div class="stat-box"><div class="stat-label">UPLOADED</div><div class="stat-value">' + formatBytes(t.uploaded_bytes) + ' / ' + formatBytes(t.total_bytes) + '</div></div>' +
          '<div class="stat-box"><div class="stat-label">ETA</div><div class="stat-value">' + formatTime(t.eta_seconds) + '</div></div>' +
        '</div>';
    });
  } catch (e) {
    console.error(e);
  }
}
setInterval(fetchProgress, 1000);
fetchProgress();
</script>
</body>
</html>`

func createDaemonTransports() (*http.Transport, *http.Transport) {
	customTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       120 * time.Second,
		WriteBufferSize:       4 * 1024 * 1024,
		ReadBufferSize:        8 * 1024 * 1024,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	_, _ = http2.ConfigureTransports(customTransport)

	h1Transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetNoDelay(true)
				_ = tcpConn.SetKeepAlive(true)
				_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
			}
			return conn, nil
		},
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		WriteBufferSize:       8 * 1024 * 1024,
		ReadBufferSize:        8 * 1024 * 1024,
		ExpectContinueTimeout: 0,
		ForceAttemptHTTP2:     false,
		TLSClientConfig: &tls.Config{
			NextProtos: []string{"http/1.1"},
		},
	}

	return customTransport, h1Transport
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatTime(secs float64) string {
	s := int64(secs)
	if s <= 0 {
		return "0s"
	}
	m := s / 60
	s = s % 60
	if m > 0 {
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func runCLIUpload(args []string) {
	var (
		folderFlag string
		credFlag   string
		targets    []string
	)

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-f" || arg == "--folder" || arg == "-folder" {
			if i+1 < len(args) {
				folderFlag = args[i+1]
				i++
			}
		} else if strings.HasPrefix(arg, "--folder=") {
			folderFlag = strings.TrimPrefix(arg, "--folder=")
		} else if arg == "-c" || arg == "--cred" || arg == "-cred" {
			if i+1 < len(args) {
				credFlag = args[i+1]
				i++
			}
		} else if strings.HasPrefix(arg, "--cred=") {
			credFlag = strings.TrimPrefix(arg, "--cred=")
		} else if strings.HasPrefix(arg, "-") {
			// skip unknown flags
		} else {
			targets = append(targets, arg)
		}
	}

	if len(targets) == 0 {
		fmt.Println("❌ No file or directory specified for upload.")
		fmt.Println("\nUsage:")
		fmt.Println("  gdrive-daemon upload <file_or_dir> [--folder <folder_id_or_url>] [--cred <path>]")
		return
	}

	folderID := parseFolderID(folderFlag)
	if folderID == "" {
		fmt.Println("ℹ️  No target folder specified. Uploading directly to root of 'My Drive'.")
	} else {
		fmt.Printf("📁 Destination Google Drive Folder: %s\n", folderID)
	}

	var filesToUpload []string
	for _, target := range targets {
		info, err := os.Stat(target)
		if err != nil {
			fmt.Printf("⚠️  Cannot access path '%s': %v\n", target, err)
			continue
		}
		if info.IsDir() {
			_ = filepath.Walk(target, func(p string, fi os.FileInfo, err error) error {
				if err == nil && !fi.IsDir() {
					filesToUpload = append(filesToUpload, p)
				}
				return nil
			})
		} else {
			filesToUpload = append(filesToUpload, target)
		}
	}

	if len(filesToUpload) == 0 {
		fmt.Println("❌ No valid files found to upload.")
		return
	}

	customTransport, h1Transport := createDaemonTransports()
	daemon := &Daemon{
		progressMap:     make(map[string]*ProgressState),
		customTransport: customTransport,
		h1Transport:     h1Transport,
	}

	ctx := context.Background()
	metaClient, uploadClient, accountName, err := daemon.getAuthenticatedClients(ctx, credFlag)
	if err != nil {
		fmt.Printf("❌ Authentication error: %v\n", err)
		fmt.Println("Please run 'python generate_token.py' or provide 'credentials.json' / 'token.json'.")
		return
	}
	fmt.Printf("🔑 Authenticated as: %s\n", accountName)
	fmt.Printf("📦 Files to upload: %d\n", len(filesToUpload))

	for i, fpath := range filesToUpload {
		fi, err := os.Stat(fpath)
		if err != nil {
			fmt.Printf("⚠️  Cannot stat file %s: %v\n", fpath, err)
			continue
		}
		totalSize := fi.Size()
		fileName := filepath.Base(fpath)
		taskID := fmt.Sprintf("cli-%s-%d", fileName, time.Now().Unix())

		file, err := os.Open(fpath)
		if err != nil {
			fmt.Printf("⚠️  Cannot open file %s: %v\n", fpath, err)
			continue
		}

		daemon.mu.Lock()
		daemon.progressMap[taskID] = &ProgressState{
			TaskID:     taskID,
			TotalBytes: totalSize,
			Status:     "uploading",
		}
		daemon.mu.Unlock()

		fmt.Printf("\n🚀 [%d/%d] Starting upload: %s (%s)\n", i+1, len(filesToUpload), fileName, formatBytes(totalSize))

		stopProgress := make(chan struct{})
		go func(tid string, tot int64, fname string) {
			ticker := time.NewTicker(400 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopProgress:
					return
				case <-ticker.C:
					daemon.mu.RLock()
					st, exists := daemon.progressMap[tid]
					daemon.mu.RUnlock()
					if exists && st != nil && st.TotalBytes > 0 {
						spdMB := st.SpeedBps / (1024 * 1024)
						spdMbps := (st.SpeedBps * 8) / 1000000
						fmt.Printf("\r   ⏳ %5.1f%% | %s / %s | %6.2f MB/s (%6.1f Mbps) | ETA: %s   ",
							st.Percentage, formatBytes(st.UploadedBytes), formatBytes(st.TotalBytes),
							spdMB, spdMbps, formatTime(st.EtaSeconds))
					}
				}
			}
		}(taskID, totalSize, fileName)

		t0 := time.Now()
		var fileID string
		var upErr error

		fileID, upErr = daemon.parallelResumableUpload(ctx, metaClient, uploadClient, file, totalSize, fileName, folderID, taskID)
		if upErr != nil {
			if _, serr := file.Seek(0, io.SeekStart); serr == nil {
				srv, serr := drive.NewService(ctx, option.WithHTTPClient(metaClient))
				if serr == nil {
					pr := &ProgressReader{
						r:          file,
						taskID:     taskID,
						totalBytes: totalSize,
						daemon:     daemon,
						lastTime:   time.Now(),
					}
					bufReader := bufio.NewReaderSize(pr, 64*1024*1024)
					driveFile := &drive.File{Name: fileName}
					if folderID != "" {
						driveFile.Parents = []string{folderID}
					}
					res, ferr := srv.Files.Create(driveFile).
						Media(bufReader, googleapi.ChunkSize(256*1024*1024)).
						SupportsAllDrives(true).
						Do()
					if ferr == nil && res != nil {
						fileID = res.Id
						upErr = nil
					} else {
						upErr = ferr
					}
				}
			}
		}
		close(stopProgress)
		file.Close()

		elapsed := time.Since(t0).Seconds()
		fmt.Printf("\r                                                                               \r")

		if upErr != nil {
			fmt.Printf("❌ Upload failed for [%s]: %v\n", fileName, upErr)
		} else {
			srv, serr := drive.NewService(ctx, option.WithHTTPClient(metaClient))
			if serr == nil {
				_, _ = srv.Permissions.Create(fileID, &drive.Permission{
					Role: "reader",
					Type: "anyone",
				}).SupportsAllDrives(true).Do()
			}
			link := fmt.Sprintf("https://drive.google.com/file/d/%s/view?usp=sharing", fileID)
			avgSpd := float64(totalSize) / elapsed
			fmt.Printf("✅ Upload Complete: %s\n", fileName)
			fmt.Printf("   File ID:   %s\n", fileID)
			fmt.Printf("   Link:      %s\n", link)
			fmt.Printf("   Duration:  %s (Avg: %.2f MB/s / %.1f Mbps)\n", formatTime(elapsed), avgSpd/(1024*1024), (avgSpd*8)/1000000)
		}
	}
}

func printHelp() {
	fmt.Println("GDrive Daemon ⚡ High-Speed Google Drive Upload Engine & CLI")
	fmt.Println("\nUsage:")
	fmt.Println("  gdrive-daemon upload <file_or_dir> [flags]   Upload file(s) directly from terminal")
	fmt.Println("  gdrive-daemon serve                         Start HTTP daemon & web dashboard (default)")
	fmt.Println("  gdrive-daemon help                          Show this help message")
	fmt.Println("\nUpload Flags:")
	fmt.Println("  -f, --folder <id_or_url>   Target Google Drive folder ID or full browser folder link")
	fmt.Println("  -c, --cred <path>          Custom path to token.json or credentials.json")
	fmt.Println("\nQuick Examples:")
	fmt.Println("  gdrive-daemon upload movie.mkv")
	fmt.Println("  gdrive-daemon upload movie.mkv --folder \"1a2b3c4d5e6f7g8h9i0j_example\"")
	fmt.Println("  gdrive-daemon upload \"C:\\My Videos\\\" --folder \"https://drive.google.com/drive/folders/1abc...\"")
	fmt.Println("  gdrive-daemon serve")
	fmt.Println("\nConfiguration File:")
	fmt.Println("  Place 'config.json' in this directory to set your default folder_id and port.")
}

func main() {
	if len(os.Args) > 1 {
		cmd := os.Args[1]
		switch cmd {
		case "upload", "up", "u":
			runCLIUpload(os.Args[2:])
			return
		case "help", "-h", "--help", "-help":
			printHelp()
			return
		case "serve", "server", "daemon", "d":
			// start HTTP daemon server below
		default:
			// If first argument is a file or directory path, treat as upload directly!
			if _, err := os.Stat(cmd); err == nil {
				runCLIUpload(os.Args[1:])
				return
			}
			fmt.Printf("Unknown command '%s'. Run 'gdrive-daemon help' for usage.\n", cmd)
			return
		}
	}

	// Default mode: start daemon & web dashboard
	customTransport, h1Transport := createDaemonTransports()
	daemon := &Daemon{
		progressMap:     make(map[string]*ProgressState),
		customTransport: customTransport,
		h1Transport:     h1Transport,
	}

	http.HandleFunc("/", daemon.handleDashboard)
	http.HandleFunc("/health", daemon.handleHealth)
	http.HandleFunc("/progress", daemon.handleProgress)
	http.HandleFunc("/all_progress", daemon.handleAllProgress)
	http.HandleFunc("/remove_task", daemon.handleRemoveTask)
	http.HandleFunc("/upload", daemon.handleUpload)

	cfg := loadConfig()
	port := "8080"
	if cfg.Port != "" {
		port = cfg.Port
	}
	if envPort := os.Getenv("DAEMON_PORT"); envPort != "" {
		port = envPort
	}

	log.Printf("⚡ Go GDrive Web Dashboard & Daemon listening on http://127.0.0.1:%s", port)
	if err := http.ListenAndServe("127.0.0.1:"+port, nil); err != nil {
		log.Fatalf("Daemon error: %v", err)
	}
}
