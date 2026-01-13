# Google Colab Setup Guide

This guide covers everything you need to run `colab-ingest` in Google Colab's terminal environment.

## 📋 Prerequisites

Before you begin, ensure you have:

1. **Google Account** - Required for Google Colab and Google Drive access
2. **Colab Access** - Go to [colab.research.google.com](https://colab.research.google.com)
3. **Pixeldrain API Key** (optional) - Required only if downloading from Pixeldrain
   - Get one at [pixeldrain.com/user/api_keys](https://pixeldrain.com/user/api_keys)

## 🚀 One-Command Setup

The fastest way to get started. Open a Colab terminal and paste:

```bash
# Clone, setup, and run in one command
cd /content && \
git clone https://github.com/yourusername/colab-ingest.git && \
cd colab-ingest && \
PIXELDRAIN_API_KEY="your-key-here" bash scripts/colab_one_command.sh \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads"
```

> **Note:** Replace `your-key-here` with your actual Pixeldrain API key, or remove the `PIXELDRAIN_API_KEY=` part if you're not using Pixeldrain.

### What the One-Command Script Does

1. ✅ Checks if running in Colab environment
2. ✅ Installs system dependencies (`git`, `p7zip-full`, `unrar`)
3. ✅ Initializes git submodules for third-party downloaders
4. ✅ Installs Python dependencies
5. ✅ Mounts Google Drive (if not already mounted)
6. ✅ Runs the pipeline with your specified options

## 📝 Manual Setup Steps

If you prefer step-by-step control, follow these instructions:

### Step 1: Open a Terminal

In your Colab notebook:
1. Click the **folder icon** (📁) in the left sidebar
2. Click the **terminal icon** at the top of the file browser
3. Or run `!bash` in a cell to get a terminal

### Step 2: Clone the Repository

```bash
cd /content
git clone https://github.com/yourusername/colab-ingest.git
cd colab-ingest
```

### Step 3: Initialize Submodules

```bash
git submodule update --init --recursive
```

This downloads the third-party downloaders (BunkrDownloader, buzzheavier-downloader).

### Step 4: Install System Dependencies

```bash
apt-get update -qq
apt-get install -y -qq p7zip-full unrar
```

### Step 5: Install Python Package

```bash
pip install -e .
```

### Step 6: Install Submodule Dependencies

```bash
pip install -r third_party/BunkrDownloader/requirements.txt
pip install -r third_party/buzzheavier-downloader/requirements.txt
```

### Step 7: Mount Google Drive

```bash
python scripts/mount_drive.py
```

Or in Python:
```python
from google.colab import drive
drive.mount('/content/drive')
```

### Step 8: Set Environment Variables

```bash
export PIXELDRAIN_API_KEY="your-api-key-here"
```

### Step 9: Run the Tool

```bash
colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads"
```

## 🔐 Google Drive Authentication

### How OAuth Works in Terminal

When you run the mount script, you'll see a prompt like:

```
Go to this URL in a browser: https://accounts.google.com/o/oauth2/...

Enter your authorization code:
```

**Steps:**
1. Copy the URL and open it in a browser
2. Sign in with your Google account
3. Grant access to your Google Drive
4. Copy the authorization code shown
5. Paste it back in the terminal and press Enter

### Authentication Persistence

- Authentication lasts for the duration of your Colab session
- When Colab disconnects (timeout, browser close), you'll need to re-authenticate
- Your files on Google Drive remain safe and unchanged

### Troubleshooting Authentication

**"Drive already mounted"** - This is fine! Your Drive is ready to use.

**Authorization code not working:**
1. Make sure you copied the entire code
2. Try unmounting and remounting:
   ```bash
   python -c "from google.colab import drive; drive.flush_and_unmount()"
   python scripts/mount_drive.py
   ```

## 🔑 Environment Variables in Colab

### Setting PIXELDRAIN_API_KEY

**Option 1: Export in terminal**
```bash
export PIXELDRAIN_API_KEY="your-api-key-here"
colab-ingest run ...
```

**Option 2: Inline with command**
```bash
PIXELDRAIN_API_KEY="your-key" colab-ingest run ...
```

**Option 3: Create a .env file**
```bash
echo 'PIXELDRAIN_API_KEY=your-api-key-here' > .env
colab-ingest run ...
```

**Option 4: Use Colab secrets (recommended for security)**

In your Colab notebook:
```python
from google.colab import userdata
import os
os.environ['PIXELDRAIN_API_KEY'] = userdata.get('PIXELDRAIN_API_KEY')
```

First, add the secret in Colab:
1. Click the 🔑 key icon in the left sidebar
2. Click "Add a new secret"
3. Name: `PIXELDRAIN_API_KEY`
4. Value: your API key
5. Enable "Notebook access"

## 📄 Working with Links Files

### Creating a Links File on Google Drive

1. Open Google Drive in your browser
2. Click **New → Google Docs** or **New → File upload**
3. Create a text file with one URL per line:

```text
# My download list
https://pixeldrain.com/u/abc12345
https://bunkr.si/a/MyAlbum
https://buzzheavier.com/f/xyz789
```

4. Save as `links.txt` in your desired location

### Uploading a Links File to Colab

**Method 1: From Google Drive (recommended)**

Your Google Drive is mounted at `/content/drive/MyDrive/`:
```bash
# Use directly from Drive
colab-ingest run --links /content/drive/MyDrive/links.txt ...
```

**Method 2: Upload through Colab UI**

1. Click the folder icon in Colab's left sidebar
2. Click the upload button
3. Select your `links.txt` file
4. Use path `/content/links.txt`

**Method 3: Create directly in terminal**

```bash
cat > /content/links.txt << 'EOF'
# My downloads
https://pixeldrain.com/u/abc12345
https://bunkr.si/a/MyAlbum
EOF
```

### File Path Reference

| Location | Path |
|----------|------|
| Colab root | `/content/` |
| Google Drive root | `/content/drive/MyDrive/` |
| Repository | `/content/colab-ingest/` |
| Working directory | `/content/work/` (default) |

## 📊 Monitoring Progress

### Understanding Console Output

```
Pipeline Configuration
  Links file:      /content/drive/MyDrive/links.txt
  Drive dest:      /content/drive/MyDrive/Downloads
  Working dir:     /content/work
  Concurrency:     3
  Max retries:     3

Overall Progress ━━━━━━━━━━━━━━━━━━━━ 40%  2/5 tasks

[OK] Completed: https://pixeldrain.com/u/abc123
[DOWNLOADING] https://bunkr.si/a/album456
[PENDING] https://buzzheavier.com/f/xyz789
```

### Progress Indicators

| Status | Meaning |
|--------|---------|
| `PENDING` | Waiting to be processed |
| `DOWNLOADING` | Currently downloading |
| `EXTRACTING` | Extracting archives |
| `UPLOADING` | Uploading to Google Drive |
| `DONE` | Successfully completed |
| `FAILED` | Error occurred (see error message) |

### Checking Status Anytime

```bash
colab-ingest status
```

Shows a table of all tasks with their current status.

### Viewing Logs

Logs are saved to the working directory:

```bash
# View recent logs
tail -100 /content/work/logs/colab_ingest.log

# Watch logs in real-time
tail -f /content/work/logs/colab_ingest.log
```

## 🔄 Resume After Disconnect

Colab sessions can timeout (usually after 90 minutes of inactivity or 12 hours total). Here's how to resume:

### Automatic Resume

Simply run the same command again:

```bash
cd /content/colab-ingest
PIXELDRAIN_API_KEY="your-key" colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads"
```

**What happens:**
- ✅ Completed tasks (`DONE`) are skipped
- ✅ Failed tasks remain failed (use `--retry-failed` to retry)
- ✅ In-progress tasks restart from the beginning
- ✅ Pending tasks are processed normally

### If Repository Was Deleted

Colab clears `/content/` on session restart. Re-clone and run:

```bash
cd /content
git clone https://github.com/yourusername/colab-ingest.git
cd colab-ingest
bash scripts/colab_one_command.sh \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads"
```

The state database is in `/content/work/` which persists during a session but is lost on restart. To persist state across sessions, you can specify a workdir on Google Drive:

```bash
colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads" \
  --workdir /content/drive/MyDrive/.colab_ingest_work
```

> **Warning:** Using Drive for workdir is slower but preserves state across sessions.

### Retry Failed Tasks

```bash
# Retry all failed tasks
colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads" \
  --retry-failed

# Or reset failed tasks first, then run
colab-ingest reset
colab-ingest run ...
```

## 💾 Disk Space Management

### Colab Disk Limits

| Resource | Limit |
|----------|-------|
| Disk space | ~78 GB (varies) |
| RAM | 12 GB (free) / 25+ GB (Pro) |
| Session time | ~12 hours max |

### Checking Disk Usage

```bash
# Check overall disk usage
df -h /content

# Check working directory size
du -sh /content/work/*

# Check Google Drive usage
du -sh /content/drive/MyDrive/
```

### Automatic Cleanup

By default, temporary files are deleted after successful upload. To keep them for debugging:

```bash
colab-ingest run ... --keep-temp
```

### Manual Cleanup

```bash
# Clean temporary files
colab-ingest clean

# Force cleanup without confirmation
colab-ingest clean --force
```

### If Disk Is Full

```bash
# 1. Clean up temporary files
colab-ingest clean --force

# 2. Remove completed downloads manually
rm -rf /content/work/downloads/*
rm -rf /content/work/extracted/*

# 3. Check what's using space
du -sh /content/* | sort -hr

# 4. Consider using a fresh runtime
# Runtime → Disconnect and delete runtime
```

## ⚠️ Known Limitations

### Colab-Specific Limitations

| Limitation | Workaround |
|------------|------------|
| Session timeout (~90 min idle) | Keep browser tab active, or use resume feature |
| Max runtime (~12 hours) | Split large jobs across multiple sessions |
| Files in `/content/` lost on restart | Store important files on Google Drive |
| No persistent background processes | Use Colab Pro for longer runtimes |
| Limited GPU/TPU for this use case | Not needed - this is CPU/network bound |

### Tool Limitations

| Limitation | Details |
|------------|---------|
| No folder structure preservation | Files are flattened in destination |
| One URL per line | Can't specify per-URL options |
| No pause/resume mid-file | Downloads restart from beginning if interrupted |
| Archive password not supported | Password-protected archives fail to extract |

### Host-Specific Limitations

| Host | Limitation |
|------|------------|
| Pixeldrain | API key required; rate limits may apply |
| Bunkr | Some CDN servers may be slow or blocked |
| BuzzHeavier | May have CAPTCHAs for some files |

## 📚 Complete Workflow Examples

### Example 1: Basic Download to Drive

```bash
# 1. Open terminal in Colab

# 2. Clone and setup
cd /content
git clone https://github.com/yourusername/colab-ingest.git
cd colab-ingest
bash scripts/colab_one_command.sh \
  --links /content/drive/MyDrive/my_links.txt \
  --drive-dest "MyDrive/Downloads"
```

### Example 2: With Pixeldrain API Key

```bash
# Set API key and run
cd /content/colab-ingest
PIXELDRAIN_API_KEY="abc123xyz" bash scripts/colab_one_command.sh \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Media/Downloads" \
  --concurrency 5
```

### Example 3: Persistent State Across Sessions

```bash
# Use Google Drive for working directory
colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads" \
  --workdir /content/drive/MyDrive/.work

# On next session, state is preserved
# Just run the same command to resume
```

### Example 4: Debug a Failed Download

```bash
# Check what failed
colab-ingest status

# Enable verbose logging
colab-ingest run \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads" \
  --retry-failed \
  --verbose \
  --keep-temp

# View logs
cat /content/work/logs/colab_ingest.log

# Check downloaded files
ls -la /content/work/downloads/
```

### Example 5: Large Batch Processing

```bash
# For large batches, use higher concurrency and split files

# Day 1: First half of links
colab-ingest run \
  --links /content/drive/MyDrive/links_part1.txt \
  --drive-dest "MyDrive/Batch1" \
  --workdir /content/drive/MyDrive/.work1 \
  --concurrency 5

# Day 2: Second half (new session)
colab-ingest run \
  --links /content/drive/MyDrive/links_part2.txt \
  --drive-dest "MyDrive/Batch2" \
  --workdir /content/drive/MyDrive/.work2 \
  --concurrency 5
```

## 🆘 Getting Help

### In-Tool Help

```bash
# General help
colab-ingest --help

# Command-specific help
colab-ingest run --help
colab-ingest status --help
colab-ingest reset --help
colab-ingest clean --help
colab-ingest check --help
```

### Check System Status

```bash
colab-ingest check
```

### File an Issue

If you encounter a bug or have a feature request:
1. Check [existing issues](https://github.com/yourusername/colab-ingest/issues)
2. Create a new issue with:
   - Colab environment details
   - Full error message
   - Steps to reproduce
   - Relevant log snippets

---

← Back to [README](../README.md)
