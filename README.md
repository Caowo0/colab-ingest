# colab-ingest

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A CLI tool for downloading files from Pixeldrain, BuzzHeavier, and Bunkr, then uploading them to Google Drive. Designed primarily for Google Colab's terminal environment.

## ✨ Features

- **Multi-host support** - Download from Pixeldrain, BuzzHeavier, and Bunkr
- **Google Drive integration** - Automatic upload to your Google Drive
- **Resume support** - SQLite-based state tracking for interrupted sessions
- **Concurrent downloads** - Process multiple URLs in parallel
- **Archive extraction** - Automatic extraction of ZIP, RAR, and 7z files
- **Progress tracking** - Rich console output with progress bars
- **One-command setup** - Single script to install and run on Colab
- **Idempotent operations** - Safe to re-run; completed tasks are skipped

## 🚀 Quick Start (Google Colab)

Open a terminal in Google Colab and run:

```bash
# Clone and run with one command
git clone https://github.com/yourusername/colab-ingest.git /content/colab-ingest
cd /content/colab-ingest
bash scripts/colab_one_command.sh --links /content/drive/MyDrive/links.txt --drive-dest "MyDrive/Downloads"
```

Or if you've already cloned:

```bash
cd /content/colab-ingest
PIXELDRAIN_API_KEY=your_key_here bash scripts/colab_one_command.sh \
  --links /content/drive/MyDrive/links.txt \
  --drive-dest "MyDrive/Downloads"
```

> 📖 For detailed Colab instructions, see [docs/COLAB.md](docs/COLAB.md)

## 📦 Installation

### Google Colab (Recommended)

The one-command setup script handles everything:

```bash
bash scripts/colab_one_command.sh --help
```

This script:
1. Installs system dependencies (git, p7zip-full, unrar)
2. Installs Python dependencies
3. Mounts Google Drive
4. Runs the CLI

### Local Development

```bash
# Clone repository
git clone https://github.com/yourusername/colab-ingest.git
cd colab-ingest

# Install in development mode
pip install -e ".[dev]"

# Verify installation
colab-ingest check
```

## 📖 Usage

### Commands

#### `colab-ingest run` - Main Pipeline

Execute the download → extract → upload pipeline.

```bash
colab-ingest run \
  --links /path/to/links.txt \
  --drive-dest "MyDrive/Uploads" \
  [options]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-l, --links` | Path to file containing URLs (required) | - |
| `-d, --drive-dest` | Destination on Google Drive (required) | - |
| `-w, --workdir` | Working directory for downloads | `/content/work` |
| `-c, --concurrency` | Number of concurrent downloads | `3` |
| `--pixeldrain-api-key` | Pixeldrain API key | `$PIXELDRAIN_API_KEY` |
| `--max-retries` | Maximum retry attempts per task | `3` |
| `--retry-failed` | Retry previously failed tasks | `False` |
| `--keep-temp` | Keep temporary files after upload | `False` |
| `--dry-run` | Show what would be done without executing | `False` |
| `-v, --verbose` | Enable verbose logging | `False` |

**Examples:**

```bash
# Basic usage
colab-ingest run -l links.txt -d "MyDrive/Downloads"

# With Pixeldrain API key
colab-ingest run -l links.txt -d "MyDrive/Downloads" --pixeldrain-api-key YOUR_KEY

# Higher concurrency with retry
colab-ingest run -l links.txt -d "MyDrive/Downloads" -c 5 --retry-failed

# Dry run to see what would happen
colab-ingest run -l links.txt -d "MyDrive/Downloads" --dry-run
```

#### `colab-ingest status` - Check Task Status

View the status of all tasks in the state database.

```bash
colab-ingest status [--workdir /path/to/workdir]
```

**Output example:**
```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
┃ URL                              ┃ Host       ┃ Status   ┃ Retries ┃ Updated          ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
│ https://pixeldrain.com/u/abc... │ pixeldrain │ done     │ 0       │ 2024-01-15 10:30 │
│ https://bunkr.si/a/xyz...       │ bunkr      │ failed   │ 2       │ 2024-01-15 10:35 │
└──────────────────────────────────┴────────────┴──────────┴─────────┴──────────────────┘

Summary: Total: 2, Done: 1, Failed: 1, Pending: 0
```

#### `colab-ingest reset` - Reset Failed Tasks

Reset failed tasks to pending status for retry.

```bash
# Reset all failed tasks
colab-ingest reset

# Reset a specific URL
colab-ingest reset --url "https://pixeldrain.com/u/abc123"

# Reset ALL tasks (use with caution!)
colab-ingest reset --all
```

#### `colab-ingest clean` - Cleanup Temp Files

Remove temporary download and extraction directories.

```bash
# Interactive cleanup
colab-ingest clean

# Force cleanup without confirmation
colab-ingest clean --force
```

This removes `downloads/` and `extracted/` directories but keeps `logs/` and `state.db`.

#### `colab-ingest check` - Verify Dependencies

Check system dependencies and configuration.

```bash
colab-ingest check
```

**Checks include:**
- Python version (3.9+)
- Required Python packages
- Third-party downloader scripts
- Extraction tools (unrar, 7z)
- Google Drive mount status
- Environment variables

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `PIXELDRAIN_API_KEY` | API key for Pixeldrain authentication | For Pixeldrain URLs |

### Setting Environment Variables

**In Colab terminal:**
```bash
export PIXELDRAIN_API_KEY="your-api-key-here"
```

**Using a `.env` file:**
```bash
# Create .env in the project root
echo 'PIXELDRAIN_API_KEY=your-api-key-here' > .env
```

**Inline with command:**
```bash
PIXELDRAIN_API_KEY=your-key colab-ingest run -l links.txt -d "MyDrive/Downloads"
```

### CLI Flags

All configuration can also be passed via CLI flags, which take precedence over environment variables:

```bash
colab-ingest run \
  --links links.txt \
  --drive-dest "MyDrive/Downloads" \
  --pixeldrain-api-key "your-key" \
  --concurrency 5 \
  --max-retries 5
```

## 🌐 Supported Hosts

### Pixeldrain

- **URL formats:**
  - Single file: `https://pixeldrain.com/u/XXXXXXXX`
  - List/folder: `https://pixeldrain.com/l/XXXXXXXX`
- **Authentication:** Requires API key
- **Get API key:** [pixeldrain.com/user/api_keys](https://pixeldrain.com/user/api_keys)

**Why API key is needed:** Pixeldrain requires authentication for download access. Without an API key, downloads will fail with authentication errors.

### BuzzHeavier

- **URL formats:**
  - `https://buzzheavier.com/XXXXXXXX`
  - `https://buzzheavier.com/f/XXXXXXXX`
- **Authentication:** None required
- **Note:** Uses bundled third-party downloader

### Bunkr

- **URL formats:**
  - Albums: `https://bunkr.si/a/XXXXXXXX`
  - Videos: `https://bunkr.si/v/XXXXXXXX`
  - Images: `https://bunkr.si/i/XXXXXXXX`
- **Authentication:** None required
- **Note:** Uses bundled third-party downloader
- **Domains:** Also supports `bunkr.la`, `bunkr.su`, `bunkrr.su`, etc.

## 📝 Links File Format

Create a text file with one URL per line:

```text
# Comments start with #
# Empty lines are ignored

# Pixeldrain files
https://pixeldrain.com/u/abc12345
https://pixeldrain.com/l/xyz98765

# BuzzHeavier files
https://buzzheavier.com/f/def67890

# Bunkr albums
https://bunkr.si/a/MyAlbum123
```

### Format Rules

- One URL per line
- Lines starting with `#` are comments (ignored)
- Empty lines are ignored
- Whitespace is trimmed automatically
- URLs are validated before processing

### Example File

See [`examples/links.example.txt`](examples/links.example.txt) for a complete example.

## 🔄 Resume & Idempotent Behavior

The tool uses an SQLite database (`state.db`) to track task progress:

### How State Tracking Works

1. **First run:** Each URL creates a task with status `PENDING`
2. **During processing:** Status updates through `DOWNLOADING` → `EXTRACTING` → `UPLOADING` → `DONE`
3. **On failure:** Status becomes `FAILED` with error message stored
4. **On re-run:** 
   - `DONE` tasks are skipped
   - `FAILED` tasks are skipped (unless `--retry-failed` is used)
   - `PENDING` or in-progress tasks are processed

### Resume After Interruption

If Colab disconnects or the process is interrupted:

```bash
# Simply run the same command again
colab-ingest run -l links.txt -d "MyDrive/Downloads"

# Completed tasks are automatically skipped
# In-progress tasks are retried
```

### Retry Failed Tasks

```bash
# Option 1: Use --retry-failed flag
colab-ingest run -l links.txt -d "MyDrive/Downloads" --retry-failed

# Option 2: Reset failed tasks first
colab-ingest reset
colab-ingest run -l links.txt -d "MyDrive/Downloads"
```

### State Database Location

The state database is stored at `{workdir}/state.db` (default: `/content/work/state.db`).

## 🔧 Troubleshooting

### Common Issues

#### "Google Drive not mounted"

```bash
# Mount manually in Python
python -c "from google.colab import drive; drive.mount('/content/drive')"

# Or use the mount script
python scripts/mount_drive.py
```

#### "Pixeldrain API key not configured"

```bash
# Set the environment variable
export PIXELDRAIN_API_KEY="your-api-key"

# Or pass via CLI
colab-ingest run ... --pixeldrain-api-key "your-key"
```

#### "BunkrDownloader not found"

```bash
# Verify installation
colab-ingest check
```

The BunkrDownloader and buzzheavier-downloader modules are bundled with the package
in [`colab_ingest/downloaders/bunkr/`](colab_ingest/downloaders/bunkr/) and [`colab_ingest/downloaders/buzzheavier/`](colab_ingest/downloaders/buzzheavier/).

#### "No space left on device"

```bash
# Clean up temporary files
colab-ingest clean --force

# Check disk usage
df -h
```

#### "Task stuck in DOWNLOADING status"

```bash
# Reset the task
colab-ingest reset --url "https://..."

# Or reset all non-completed tasks
colab-ingest reset --all
```

### Debug Mode

Enable verbose logging for more details:

```bash
colab-ingest run -l links.txt -d "MyDrive/Downloads" --verbose
```

Logs are saved to `{workdir}/logs/` directory.

### Check System Status

```bash
colab-ingest check
```

This will show which components are working and which need attention.

## 👨‍💻 Development

### Project Structure

```
colab-ingest/
├── colab_ingest/           # Main package
│   ├── cli.py              # CLI entry point (Typer)
│   ├── core/               # Core logic
│   │   ├── pipeline.py     # Main orchestrator
│   │   └── state.py        # SQLite state management
│   ├── downloaders/        # Host-specific downloaders
│   │   ├── pixeldrain.py   # Pixeldrain API client
│   │   ├── bunkr_adapter.py
│   │   ├── buzzheavier_adapter.py
│   │   ├── bunkr/          # Bundled BunkrDownloader
│   │   └── buzzheavier/    # Bundled buzzheavier-downloader
│   └── utils/              # Utilities
│       ├── extract.py      # Archive extraction
│       ├── upload.py       # Google Drive upload
│       ├── url_detect.py   # URL parsing
│       ├── paths.py        # Path management
│       └── logging.py      # Logging setup
├── scripts/                # Helper scripts
│   ├── colab_one_command.sh
│   └── mount_drive.py
├── tests/                  # Test suite
├── docs/                   # Documentation
└── examples/               # Example files
```

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=colab_ingest
```

### Code Style

The project uses:
- [Ruff](https://docs.astral.sh/ruff/) for linting
- Type hints throughout
- Docstrings in Google style

```bash
# Check linting
ruff check .

# Format code
ruff format .
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `pytest`
5. Commit: `git commit -am 'Add my feature'`
6. Push: `git push origin feature/my-feature`
7. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [BunkrDownloader](https://github.com/Lysagxra/BunkrDownloader) by Lysagxra
- [buzzheavier-downloader](https://github.com/gongchandang49/buzzheavier-downloader) by gongchandang49
- [Typer](https://typer.tiangolo.com/) for the CLI framework
- [Rich](https://rich.readthedocs.io/) for beautiful terminal output
