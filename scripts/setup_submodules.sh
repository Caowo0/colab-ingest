#!/bin/bash
# Initialize and update git submodules for third-party downloaders
#
# This script handles:
# - Fresh clone (submodules empty)
# - Existing submodules (pull updates)
# - Missing git (error message)
#
# Usage:
#   bash scripts/setup_submodules.sh [--update]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Submodule paths
SUBMODULES=(
    "third_party/BunkrDownloader"
    "third_party/buzzheavier-downloader"
)

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

# Check if git is installed
check_git() {
    if ! command -v git &> /dev/null; then
        print_error "git is not installed"
        echo "Please install git first:"
        echo "  apt-get install git    (Debian/Ubuntu)"
        echo "  yum install git        (CentOS/RHEL)"
        echo "  brew install git       (macOS)"
        exit 1
    fi
    print_success "git is installed ($(git --version))"
}

# Check if we're in a git repository
check_git_repo() {
    if ! git rev-parse --is-inside-work-tree &> /dev/null; then
        print_error "Not inside a git repository"
        echo "Please run this script from within the colab_ingest repository"
        exit 1
    fi
    print_success "Inside git repository"
}

# Get repository root
get_repo_root() {
    git rev-parse --show-toplevel
}

# Initialize submodules
init_submodules() {
    print_step "Initializing git submodules..."
    
    # Check if .gitmodules exists
    if [ ! -f ".gitmodules" ]; then
        print_warning "No .gitmodules file found"
        echo "This repository may not have any submodules configured"
        return 0
    fi
    
    # Initialize and update submodules
    if git submodule update --init --recursive; then
        print_success "Submodules initialized"
    else
        print_error "Failed to initialize submodules"
        echo ""
        echo "This might be due to network issues. Please try:"
        echo "  1. Check your internet connection"
        echo "  2. Try again later"
        echo "  3. Manually clone the submodules:"
        for submodule in "${SUBMODULES[@]}"; do
            echo "     git clone <repo-url> $submodule"
        done
        return 1
    fi
}

# Update existing submodules
update_submodules() {
    print_step "Updating git submodules..."
    
    if git submodule update --remote --merge; then
        print_success "Submodules updated to latest"
    else
        print_warning "Could not update submodules to latest (using current versions)"
    fi
}

# Verify submodules are properly initialized
verify_submodules() {
    print_step "Verifying submodules..."
    
    local all_ok=true
    
    for submodule in "${SUBMODULES[@]}"; do
        if [ -d "$submodule" ] && [ "$(ls -A "$submodule" 2>/dev/null)" ]; then
            print_success "$submodule is initialized"
        else
            print_warning "$submodule is empty or missing"
            all_ok=false
        fi
    done
    
    if [ "$all_ok" = true ]; then
        print_success "All submodules verified"
        return 0
    else
        print_warning "Some submodules may need attention"
        return 1
    fi
}

# Install submodule dependencies
install_submodule_deps() {
    print_step "Checking submodule dependencies..."
    
    for submodule in "${SUBMODULES[@]}"; do
        if [ -f "$submodule/requirements.txt" ]; then
            print_step "Installing dependencies for $submodule..."
            pip install -r "$submodule/requirements.txt" -q 2>/dev/null || {
                print_warning "Could not install dependencies for $submodule"
            }
        fi
    done
}

# Print usage
print_usage() {
    echo "Usage: bash scripts/setup_submodules.sh [OPTIONS]"
    echo ""
    echo "Initialize and update git submodules for third-party downloaders."
    echo ""
    echo "Options:"
    echo "  --update          Update submodules to their latest remote versions"
    echo "  --install-deps    Also install Python dependencies from submodules"
    echo "  --verify-only     Only verify submodules, don't initialize"
    echo "  --help            Show this help message"
    echo ""
    echo "Submodules managed:"
    for submodule in "${SUBMODULES[@]}"; do
        echo "  - $submodule"
    done
}

# Main execution
main() {
    local do_update=false
    local do_install_deps=false
    local verify_only=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --update)
                do_update=true
                shift
                ;;
            --install-deps)
                do_install_deps=true
                shift
                ;;
            --verify-only)
                verify_only=true
                shift
                ;;
            --help|-h)
                print_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
    
    echo "=============================================="
    echo "  Git Submodule Setup"
    echo "=============================================="
    echo ""
    
    # Run checks
    check_git
    check_git_repo
    
    # Change to repo root
    cd "$(get_repo_root)"
    print_success "Working in $(pwd)"
    
    if [ "$verify_only" = true ]; then
        verify_submodules
        exit $?
    fi
    
    # Initialize submodules
    init_submodules || exit 1
    
    # Update if requested
    if [ "$do_update" = true ]; then
        update_submodules
    fi
    
    # Verify
    verify_submodules
    
    # Install dependencies if requested
    if [ "$do_install_deps" = true ]; then
        install_submodule_deps
    fi
    
    echo ""
    echo "=============================================="
    echo "  Submodule Setup Complete"
    echo "=============================================="
}

# Run main function
main "$@"
