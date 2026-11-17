# GDrive Daemon ⚡

High-performance, standalone Google Drive resumable upload engine, CLI tool, and REST API built with Go.

Designed for high-bandwidth servers (1 Gbps – 10 Gbps), **GDrive Daemon** overcomes cloud SDK throughput bottlenecks by utilizing HTTP/1.1 wire streaming, adaptive 1,024 MB chunks, pre-fetching read-ahead pipelines, and zero-allocation memory recycling.

You can use it either as a **direct CLI tool** (like a high-speed uploader) or as a **background daemon with a live web dashboard**.

---

## 📋 Table of Contents
- [Features](#-features)
- [Installing the Go Language](#-installing-the-go-language)
- [How to Get Google Drive Credentials](#-how-to-get-google-drive-credentials)
  - [Method 1: User OAuth 2.0 (token.json) — Recommended](#method-1-user-oauth-20-tokenjson--recommended)
  - [Method 2: Service Account (credentials.json)](#method-2-service-account-credentialsjson)
- [Where to Configure Your Google Drive Folder](#-where-to-configure-your-google-drive-folder)
- [Build Instructions](#-build-instructions)
- [CLI Commands (Direct Terminal Upload)](#-cli-commands-direct-terminal-upload)
- [Running as a Background Daemon & Web Dashboard](#-running-as-a-background-daemon--web-dashboard)
- [HTTP REST API Commands](#-http-rest-api-commands)
  - [1. Upload via cURL](#1-upload-via-curl)
  - [2. Upload via PowerShell](#2-upload-via-powershell)
  - [3. Upload via Python](#3-upload-via-python)
  - [4. Query Live Progress](#4-query-live-progress)
  - [5. Stop / Kill the Running Daemon](#5-stop--kill-the-running-daemon)
- [Environment Knobs](#-environment-knobs)

---

## 🚀 Features

- **Multi-Gigabit Line Saturation**: Bypasses HTTP/2 stream flow-control limits using a tuned HTTP/1.1 engine. Reaches 100–150 MB/s on single streams and 2–3+ Gbps on parallel folder uploads.
- **Direct Terminal CLI Mode**: Upload files or entire directories directly from command line with a live progress bar, speed meter, and auto-generated share links.
- **Adaptive In-Order Chunking**: Automatically scales chunk sizes to 1,024 MB for large files, halving edge commit stalls.
- **Zero-Allocation Buffer Recycling**: Ring-buffer pool recycles chunk memory directly, eliminating Go GC pauses during large 10GB+ transfers.
- **Prefetched Disk Read-Ahead**: Disk I/O runs ahead of the network pipeline, ensuring socket writes never wait on storage reads.
- **Dual Credential Engine**: Seamlessly accepts either User OAuth `token.json` or Service Account `credentials.json`.
- **Integrated Web UI**: Real-time dark-mode dashboard showing progress bars, throughput rates, and Google Drive links.

---

## 🐹 Installing the Go Language

If Go is not yet installed on your system, install it using the instructions below:

### Windows:
- **Via Windows Package Manager (PowerShell)**:
  ```powershell
  winget install GoLang.Go
  ```
- **Or via Official Installer**:
  Download and run the MSI installer from [https://go.dev/dl/](https://go.dev/dl/).

### Linux (Ubuntu / Debian):
```bash
sudo apt update && sudo apt install -y golang
```
*Or install the latest official release:*
```bash
wget https://go.dev/dl/go1.23.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.4.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
```

### macOS:
```bash
brew install go
```

### Verify Installation:
```bash
go version
# Example output: go version go1.23.4 windows/amd64
```

---

## 📁 Where to Configure Your Google Drive Folder

You can specify where uploaded files should be stored using any of these simple methods:

### Method A: Set it in `config.json` (Recommended — Set Once, Use Everywhere)
Create or edit `config.json` in the daemon directory (see `config.example.json`):
```json
{
  "folder_id": "YOUR_GDRIVE_FOLDER_ID_OR_LINK",
  "port": "8080"
}
```
You can paste either the **Folder ID** (`1a2b3c4d5e6f...`) OR the **Full Browser Link** (`https://drive.google.com/drive/folders/1a2b3c4d5e6f...`).
Once set, all CLI uploads and API requests automatically upload directly into that folder!

### Method B: Pass the `--folder` Flag in CLI
You can specify or override the target folder per command:
```bash
gdrive-daemon upload movie.mkv --folder "1a2b3c4d5e6f7g8h9i0j_example"
```
Or paste the full browser URL:
```bash
gdrive-daemon upload movie.mkv --folder "https://drive.google.com/drive/folders/1a2b3c4d5e6f7g8h9i0j_example"
```

### Method C: Environment Variable
```bash
export GDRIVE_DEFAULT_FOLDER_ID="YOUR_FOLDER_ID_OR_LINK"
```

### 🔍 How to find your Google Drive Folder ID:
1. Open [Google Drive](https://drive.google.com/) in your browser.
2. Open the destination folder.
3. Look at the address bar:
   `https://drive.google.com/drive/folders/`**`1a2b3c4d5e6f7g8h9i0j_example`**
4. Copy the full link or the string at the end. Both are supported.

---

## 🔑 How to Get Google Drive Credentials

The daemon supports two credential types. You only need **one** of them:

---

### Method 1: User OAuth 2.0 (`token.json`) — [Recommended]
*Best for uploading files directly to your personal Google Drive or Shared Drives without storage quota issues.*

#### Step 1: Create a Google Cloud Project
1. Open the [Google Cloud Console](https://console.cloud.google.com/).
2. Click the project dropdown at the top and click **New Project** (e.g. `GDrive-Uploader`).

#### Step 2: Enable Google Drive API
1. Search for **Google Drive API** in the top search bar.
2. Click on it and select **Enable**.

#### Step 3: Configure OAuth Consent Screen
1. Go to **APIs & Services** > **OAuth consent screen**.
2. User Type: Select **External** and click **Create**.
3. Fill in the required fields:
   - **App name**: `GDrive Uploader`
   - **User support email**: Your email address
   - **Developer contact email**: Your email address
4. Click **Save and Continue**.
5. Under **Scopes**, click **Add or Remove Scopes**:
   - Search for `drive` and check `.../auth/drive` (Full access to Google Drive).
   - Click **Update**, then **Save and Continue**.
6. Under **Test users**:
   - Click **Add Users** and enter your Google account email address.
   - Click **Save and Continue**.

#### Step 4: Create Desktop OAuth Credentials
1. Go to **APIs & Services** > **Credentials**.
2. Click **+ Create Credentials** > **OAuth client ID**.
3. Application type: Select **Desktop app**.
4. Name: `GDrive Client` (or any name).
5. Click **Create**.
6. Click **Download JSON** on the created credential.
7. Rename the downloaded JSON file to **`client_secret.json`** and move it into this project directory.

#### Step 5: Generate `token.json` using the included Python script
1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the interactive token generator:
   ```bash
   python generate_token.py
   ```
3. A browser window will open automatically asking you to log in to your Google account and grant Drive access.
4. Once approved, the script automatically saves **`token.json`** in this folder!

---

### Method 2: Service Account (`credentials.json`)
*Best for automated server-to-server workflows and Google Workspace Shared Drives.*

#### Step 1: Create a Service Account
1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. Go to **IAM & Admin** > **Service Accounts**.
3. Click **+ Create Service Account**.
4. Name: `gdrive-daemon` -> Click **Create and Continue** -> Click **Done**.

#### Step 2: Generate and Download the JSON Key
1. Find your newly created service account in the list and click on it.
2. Go to the **Keys** tab > **Add Key** > **Create new key** > Select **JSON**.
3. Rename the downloaded JSON file to **`credentials.json`** and place it in this project folder.

#### Step 3: Share the Destination Folder with the Service Account
1. Open `credentials.json` and copy the `"client_email"`:
   *(e.g., `gdrive-daemon@your-project-id.iam.gserviceaccount.com`)*
2. Open Google Drive in your web browser.
3. Right-click the folder where you want files uploaded, click **Share**.
4. Paste the Service Account's `client_email` into the add people field.
5. Set permission to **Editor**, uncheck "Notify people", and click **Share**.

---

## 🔨 Build Instructions

Once Go is installed:

### Linux / macOS:
```bash
go build -o gdrive-daemon main.go parallel_upload.go
```

### Windows (PowerShell / Command Prompt):
```powershell
go build -o gdrive-daemon.exe main.go parallel_upload.go
```

---

## 💻 CLI Commands (Direct Terminal Upload)

After building the binary, you can upload files and folders directly from your command line without starting a server:

### 1. Upload a Single File
```bash
./gdrive-daemon upload /path/to/movie.mkv
```
On Windows:
```powershell
.\gdrive-daemon.exe upload "C:\path\to\movie.mkv"
```
*(Or simply: `.\gdrive-daemon.exe "C:\path\to\movie.mkv"`)*

### 2. Upload to a Specific Folder (Folder ID or URL)
```bash
./gdrive-daemon upload movie.mkv --folder "1a2b3c4d5e6f7g8h9i0j_example"
```
Or paste the full browser URL:
```bash
./gdrive-daemon upload movie.mkv --folder "https://drive.google.com/drive/folders/1a2b3c4d5e6f7g8h9i0j_example"
```

### 3. Upload Multiple Files or an Entire Directory
```bash
./gdrive-daemon upload file1.mkv file2.mkv
```
Or upload all files inside a folder:
```bash
./gdrive-daemon upload "C:\My Videos\Season 01\"
```

### 4. CLI Terminal Output Example
```
⚡ [movie.mkv]  64.2% | 5.86 GB / 9.10 GB | 118.50 MB/s (994.0 Mbps) | ETA: 28s
✅ Upload Complete: movie.mkv
   File ID:   1x2y3z4a5b6c7d8e9f0g_example
   Link:      https://drive.google.com/file/d/1x2y3z4a5b6c7d8e9f0g_example/view?usp=sharing
   Duration:  1m 16s (Avg: 119.7 MB/s / 1004.2 Mbps)
```

---

## 🌐 Running as a Background Daemon & Web Dashboard

To run as an HTTP background daemon with a live web dashboard:

```bash
./gdrive-daemon serve
```
*(Or run without arguments: `./gdrive-daemon`)*

Open your browser at:
```
http://localhost:8080/
```
The dashboard shows real-time progress bars, live transfer speed, ETA, and clickable share links.

---

## 🔌 HTTP REST API Commands

When running in daemon mode (`serve`), you can submit upload jobs via HTTP:

### 1. Upload via cURL
```bash
curl -X POST http://127.0.0.1:8080/upload \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "C:\\path\\to\\video.mkv",
    "folder_id": "YOUR_GDRIVE_FOLDER_ID_OR_LINK"
  }'
```

### 2. Upload via PowerShell
```powershell
$body = @{
    file_path = "C:\path\to\movie.mkv"
    folder_id = "YOUR_GDRIVE_FOLDER_ID"
} | ConvertTo-Json

$res = Invoke-RestMethod -Uri "http://127.0.0.1:8080/upload" -Method Post -Body $body -ContentType "application/json"
Write-Host "Uploaded! Link: $($res.web_view_link)"
```

### 3. Upload via Python
```python
import requests

payload = {
    "file_path": r"C:\path\to\movie.mkv",
    "folder_id": "YOUR_GDRIVE_FOLDER_ID"
}

resp = requests.post("http://127.0.0.1:8080/upload", json=payload).json()
print("Link:", resp.get("web_view_link"))
```

### 4. Query Live Progress
```bash
curl http://127.0.0.1:8080/progress?task_id=movie.mkv
```

### 5. Stop / Kill the Running Daemon
- **Windows (PowerShell)**:
  ```powershell
  Get-Process | Where-Object { $_.ProcessName -like "*gdrive-daemon*" } | Stop-Process -Force
  ```
- **Linux / macOS**:
  ```bash
  pkill -f gdrive-daemon
  ```

---

## ⚙️ Environment Knobs

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `GDRIVE_DEFAULT_FOLDER_ID` | `""` | Default Google Drive folder ID or URL for all uploads. |
| `DAEMON_PORT` | `8080` | Port for the HTTP API and Web Dashboard. |
| `GDRIVE_UPLOAD_CHUNK_MB` | Adaptive (`128`–`1024`) | Size of in-order byte chunks sent to Google. Files $\ge$ 4 GB default to 1,024 MB. |
| `GDRIVE_UPLOAD_READAHEAD`| `2` | Number of chunks pre-read into RAM memory buffer ahead of active socket transmission. |

---

## 📄 License

MIT License.
