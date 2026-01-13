#!/bin/bash
# One-command setup and run for Google Colab
# Usage: bash scripts/colab_one_command.sh --links /path/to/links.txt --drive-dest "MyDrive/Uploads" [options]
#
# This script handles complete setup and execution of colab_ingest:
# 1. Install system dependencies (git, p7zip-full, unrar)
# 2. Clone/update the repo if needed
# 3. Install Python dependencies
# 4. Mount Google Drive if not mounted
# 5. Run the CLI with provided arguments

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Repository URL (update this to your actual repo)
REPO_URL="https://github.com/your-username/colab-ingest.git"
REPO_NAME="colab-ingest"

# Print colored message
print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running in Colab environment
check_colab_environment() {
    print_step "Checking environment..."
    
    if [ -d "/content" ]; then
        print_success "Running in Google Colab environment"
        export IS_COLAB=1
    else
        print_warning "Not running in Colab - some features may not work"
        export IS_COLAB=0
    fi
}

# Install system dependencies if missing
install_system_deps() {
    print_step "Checking system dependencies..."
    
    local packages_to_install=()
    
    # Check git
    if ! command -v git &> /dev/null; then
        packages_to_install+=("git")
    else
        print_success "git is installed"
    fi
    
    # Check 7z
    if ! command -v 7z &> /dev/null; then
        packages_to_install+=("p7zip-full")
    else
        print_success "p7zip-full is installed"
    fi
    
    # Check unrar
    if ! command -v unrar &> /dev/null; then
        packages_to_install+=("unrar")
    else
        print_success "unrar is installed"
    fi
    
    # Install missing packages
    if [ ${#packages_to_install[@]} -gt 0 ]; then
        print_step "Installing missing packages: ${packages_to_install[*]}"
        apt-get update -qq
        apt-get install -y -qq "${packages_to_install[@]}"
        print_success "System dependencies installed"
    else
        print_success "All system dependencies already installed"
    fi
}

# Handle repository setup
setup_repository() {
    print_step "Setting up repository..."
    
    # Check if we're already in the repo directory
    if [ -f "pyproject.toml" ] && grep -q "colab-ingest\|colab_ingest" pyproject.toml 2>/dev/null; then
        print_success "Already in repository directory"
        
        # Pull latest changes if this is a git repo
        if [ -d ".git" ]; then
            print_step "Pulling latest changes..."
            git pull --ff-only 2>/dev/null || print_warning "Could not pull updates (may be on a different branch)"
        fi
    else
        # Not in repo, need to clone
        if [ "$IS_COLAB" = "1" ]; then
            cd /content
        fi
        
        if [ -d "$REPO_NAME" ]; then
            print_step "Repository exists, updating..."
            cd "$REPO_NAME"
            git pull --ff-only 2>/dev/null || print_warning "Could not pull updates"
        else
            print_step "Cloning repository..."
            git clone "$REPO_URL" "$REPO_NAME"
            cd "$REPO_NAME"
        fi
        print_success "Repository ready"
    fi
}

# Install Python dependencies
install_python_deps() {
    print_step "Installing Python dependencies..."
    
    # Use pip to install in editable mode
    pip install -e . -q
    
    # Install bundled downloader dependencies if they exist
    if [ -f "colab_ingest/downloaders/bunkr/requirements.txt" ]; then
        print_step "Installing BunkrDownloader dependencies..."
        pip install -r colab_ingest/downloaders/bunkr/requirements.txt -q
    fi
    
    if [ -f "colab_ingest/downloaders/buzzheavier/requirements.txt" ]; then
        print_step "Installing buzzheavier-downloader dependencies..."
        pip install -r colab_ingest/downloaders/buzzheavier/requirements.txt -q
    fi
    
    print_success "Python dependencies installed"
}

# Mount Google Drive
mount_google_drive() {
    print_step "Checking Google Drive mount..."
    
    # Check if already mounted
    if [ -d "/content/drive/MyDrive" ]; then
        print_success "Google Drive already mounted"
        return 0
    fi
    
    # Use the mount_drive.py helper
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    if [ -f "$SCRIPT_DIR/mount_drive.py" ]; then
        print_step "Mounting Google Drive..."
        python "$SCRIPT_DIR/mount_drive.py"
        
        if [ -d "/content/drive/MyDrive" ]; then
            print_success "Google Drive mounted successfully"
        else
            print_error "Failed to mount Google Drive"
            return 1
        fi
    elif [ -f "scripts/mount_drive.py" ]; then
        python scripts/mount_drive.py
    else
        print_warning "mount_drive.py not found, attempting inline mount..."
        python -c "from google.colab import drive; drive.mount('/content/drive')" || {
            print_error "Failed to mount Google Drive"
            return 1
        }
    fi
}

# Run the CLI
run_cli() {
    print_step "Running colab-ingest..."
    echo ""
    
    # Check for PIXELDRAIN_API_KEY environment variable
    if [ -n "$PIXELDRAIN_API_KEY" ]; then
        print_success "Using PIXELDRAIN_API_KEY from environment"
    fi
    
    # Run the CLI with all provided arguments
    python -m colab_ingest run "$@"
    local exit_code=$?
    
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "colab-ingest completed successfully!"
    else
        print_error "colab-ingest exited with code $exit_code"
    fi
    
    return $exit_code
}

# Print usage information
print_usage() {
    echo "Usage: bash scripts/colab_one_command.sh [OPTIONS]"
    echo ""
    echo "One-command setup and run for colab_ingest on Google Colab."
    echo ""
    echo "Options are passed directly to 'colab-ingest run'. Common options:"
    echo "  --links FILE          Path to file containing URLs to process"
    echo "  --drive-dest PATH     Destination path in Google Drive (relative to MyDrive)"
    echo "  --work-dir DIR        Working directory for downloads (default: /content/colab_ingest_work)"
    echo "  --keep-work           Keep working directory after completion"
    echo "  -v, --verbose         Enable verbose output"
    echo "  --help                Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  PIXELDRAIN_API_KEY    API key for Pixeldrain uploads"
    echo ""
    echo "Examples:"
    echo "  bash scripts/colab_one_command.sh --links links.txt --drive-dest 'Downloads'"
    echo "  PIXELDRAIN_API_KEY=xxx bash scripts/colab_one_command.sh --links links.txt"
}

# Main execution
main() {
    echo "=============================================="
    echo "  colab_ingest - One Command Setup & Run"
    echo "=============================================="
    echo ""
    
    # Check for help flag
    for arg in "$@"; do
        if [ "$arg" = "--help" ] || [ "$arg" = "-h" ]; then
            print_usage
            exit 0
        fi
    done
    
    # Check if any arguments provided
    if [ $# -eq 0 ]; then
        print_warning "No arguments provided. Use --help for usage information."
        echo ""
    fi
    
    # Run setup steps
    check_colab_environment
    install_system_deps
    setup_repository
    install_python_deps
    mount_google_drive
    
    echo ""
    echo "=============================================="
    echo "  Setup Complete - Starting Processing"
    echo "=============================================="
    echo ""
    
    # Run the CLI with all arguments
    run_cli "$@"
    exit_code=$?
    
    echo ""
    echo "=============================================="
    echo "  Summary"
    echo "=============================================="
    if [ $exit_code -eq 0 ]; then
        echo -e "Status: ${GREEN}SUCCESS${NC}"
    else
        echo -e "Status: ${RED}FAILED${NC} (exit code: $exit_code)"
    fi
    echo "=============================================="
    
    exit $exit_code
}

# Run main function with all script arguments
main "$@"
