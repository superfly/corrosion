#!/usr/bin/env bash
set -euo pipefail

# Script to manage vendoring of dependencies using git subtree
# Supports both complex (orphan branch) and simple (squashed) workflows

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

warn() {
    echo -e "${YELLOW}WARN: $1${NC}"
}

# Get the current branch name
get_current_branch() {
    git rev-parse --abbrev-ref HEAD
}

# Check if a remote exists
remote_exists() {
    git remote | grep -q "^$1$"
}

# Check if a branch exists (local or remote)
branch_exists() {
    git rev-parse --verify "$1" >/dev/null 2>&1
}

# Get dependency configuration
get_dep_config() {
    local dep_name=$1
    
    case "$dep_name" in
        libsqlite3-sys)
            DEP_TYPE="complex"
            DEP_REPO="https://github.com/rusqlite/rusqlite.git"
            # 0.38.0
            DEP_COMMIT="35b3be2436a63d21701d1d110661e6392831fea0"
            DEP_PREFIX="vendor/libsqlite3-sys"
            DEP_REMOTE="rusqlite"
            DEP_ORPHAN_BRANCH="vendor/rusqlite"
            DEP_SPLIT_BRANCH="vendor/libsqlite3-sys"
            DEP_ORPHAN_PATH="rusqlite"           # Path on orphan branch where full repo is added
            DEP_SPLIT_PATH="rusqlite/libsqlite3-sys"  # Path to split out
            ;;
        cr-sqlite)
            DEP_TYPE="simple"
            DEP_REPO="https://github.com/superfly/cr-sqlite.git"
            # main branch
            DEP_COMMIT="414b7959ffe2470c86541fa18cec2de4d72b1fb1"
            DEP_PREFIX="vendor/libsqlite3-sys/vendor/cr-sqlite"
            DEP_REMOTE="cr-sqlite"
            ;;
        *)
            error "Unknown dependency: $dep_name"
            ;;
    esac
}

# Initialize a complex dependency (orphan branch workflow)
init_complex() {
    local dep_name=$1
    get_dep_config "$dep_name"
    
    info "Initializing complex vendor subtree for $dep_name"
    
    if [ -d "$DEP_PREFIX" ]; then
        error "Vendor directory $DEP_PREFIX already exists. Use 'update' command instead."
    fi
    
    local current_branch=$(get_current_branch)
    info "Current branch: $current_branch"
    
    # Add remote if it doesn't exist
    if ! remote_exists "$DEP_REMOTE"; then
        info "Adding $DEP_REMOTE remote..."
        git remote add "$DEP_REMOTE" "$DEP_REPO"
    fi
    
    info "Fetching $DEP_REMOTE remote..."
    git fetch "$DEP_REMOTE"
    
    # Create orphan branch
    info "Creating orphan branch $DEP_ORPHAN_BRANCH..."
    git switch --orphan "$DEP_ORPHAN_BRANCH"
    git commit --allow-empty -m "Initial commit"
    
    # Add the entire repository as a subtree (no --squash for split to work)
    # https://bugs.rockylinux.org/view.php?id=10660
    info "Adding $DEP_REMOTE repository at commit $DEP_COMMIT..."
    git subtree add -P "$DEP_ORPHAN_PATH" "$DEP_REMOTE" "$DEP_COMMIT"
    
    # Split out subdirectory into its own branch
    info "Splitting $DEP_SPLIT_PATH subdirectory into $DEP_SPLIT_BRANCH branch..."
    git subtree split --rejoin -P "$DEP_SPLIT_PATH" -b "$DEP_SPLIT_BRANCH"
    
    # Push both vendor branches to origin
    info "Pushing $DEP_ORPHAN_BRANCH to origin..."
    git push origin "$DEP_ORPHAN_BRANCH"
    
    info "Pushing $DEP_SPLIT_BRANCH to origin..."
    git push origin "$DEP_SPLIT_BRANCH"
    
    # Switch back to original branch
    info "Switching back to $current_branch..."
    git checkout "$current_branch"
    
    # Add the subtree to the main branch
    info "Adding $DEP_PREFIX from $DEP_SPLIT_BRANCH..."
    git subtree add -P "$DEP_PREFIX" origin "$DEP_SPLIT_BRANCH"
    
    info "✓ Initialization complete for $dep_name!"
}

# Update a complex dependency
update_complex() {
    local dep_name=$1
    get_dep_config "$dep_name"
    
    info "Updating complex vendor subtree for $dep_name"
    
    if [ ! -d "$DEP_PREFIX" ]; then
        error "Vendor directory $DEP_PREFIX does not exist. Use 'init' command first."
    fi
    
    local current_branch=$(get_current_branch)
    info "Current branch: $current_branch"
    
    # Ensure remote exists
    if ! remote_exists "$DEP_REMOTE"; then
        info "Adding $DEP_REMOTE remote..."
        git remote add "$DEP_REMOTE" "$DEP_REPO"
    fi
    
    info "Fetching $DEP_REMOTE remote..."
    git fetch "$DEP_REMOTE"
    
    # Switch to orphan branch
    info "Switching to $DEP_ORPHAN_BRANCH branch..."
    git checkout "$DEP_ORPHAN_BRANCH"
    
    # Pull updates
    info "Pulling updates from $DEP_REMOTE at commit $DEP_COMMIT..."
    git subtree pull -P "$DEP_ORPHAN_PATH" "$DEP_REMOTE" "$DEP_COMMIT"
    
    # Split out subdirectory, updating the branch
    info "Splitting $DEP_SPLIT_PATH subdirectory into $DEP_SPLIT_BRANCH branch..."
    git subtree split --rejoin -P "$DEP_SPLIT_PATH" -b "$DEP_SPLIT_BRANCH"
    
    # Push both vendor branches to origin
    info "Pushing $DEP_ORPHAN_BRANCH to origin..."
    git push origin "$DEP_ORPHAN_BRANCH"
    
    info "Pushing $DEP_SPLIT_BRANCH to origin..."
    git push origin "$DEP_SPLIT_BRANCH"
    
    # Switch back to original branch
    info "Switching back to $current_branch..."
    git checkout "$current_branch"
    
    # Pull the updated subtree
    info "Pulling updates into $DEP_PREFIX..."
    git subtree pull -P "$DEP_PREFIX" origin "$DEP_SPLIT_BRANCH"
    
    info "✓ Update complete for $dep_name!"
}

# Initialize a simple dependency (squashed subtree)
init_simple() {
    local dep_name=$1
    get_dep_config "$dep_name"
    
    info "Initializing simple vendor subtree for $dep_name"
    
    if [ -d "$DEP_PREFIX" ]; then
        error "Vendor directory $DEP_PREFIX already exists. Use 'update' command instead."
    fi
    
    # Add remote if it doesn't exist
    if ! remote_exists "$DEP_REMOTE"; then
        info "Adding $DEP_REMOTE remote..."
        git remote add "$DEP_REMOTE" "$DEP_REPO"
    fi
    
    info "Fetching $DEP_REMOTE remote..."
    git fetch "$DEP_REMOTE"
    
    # Add the subtree with --squash
    info "Adding $dep_name at $DEP_COMMIT..."
    git subtree add --squash -P "$DEP_PREFIX" "$DEP_REMOTE" "$DEP_COMMIT"
    
    info "✓ Initialization complete for $dep_name!"
}

# Update a simple dependency
update_simple() {
    local dep_name=$1
    get_dep_config "$dep_name"
    
    info "Updating simple vendor subtree for $dep_name"
    
    if [ ! -d "$DEP_PREFIX" ]; then
        error "Vendor directory $DEP_PREFIX does not exist. Use 'init' command first."
    fi
    
    # Ensure remote exists
    if ! remote_exists "$DEP_REMOTE"; then
        info "Adding $DEP_REMOTE remote..."
        git remote add "$DEP_REMOTE" "$DEP_REPO"
    fi
    
    info "Fetching $DEP_REMOTE remote..."
    git fetch "$DEP_REMOTE"
    
    # Pull updates with --squash
    info "Pulling updates from $DEP_REMOTE at $DEP_COMMIT..."
    git subtree pull --squash -P "$DEP_PREFIX" "$DEP_REMOTE" "$DEP_COMMIT"
    
    info "✓ Update complete for $dep_name!"
}

# Main init command
cmd_init() {
    local target="${1:-}"
    
    if [ -z "$target" ]; then
        error "Please specify a target: {libsqlite3-sys|cr-sqlite|all}"
    fi
    
    if [ "$target" = "all" ]; then
        init_complex "libsqlite3-sys"
        init_simple "cr-sqlite"
        return
    fi
    
    get_dep_config "$target"
    
    if [ "$DEP_TYPE" = "complex" ]; then
        init_complex "$target"
    elif [ "$DEP_TYPE" = "simple" ]; then
        init_simple "$target"
    else
        error "Unknown dependency type: $DEP_TYPE"
    fi
}

# Main update command
cmd_update() {
    local target="${1:-}"
    
    if [ -z "$target" ]; then
        error "Please specify a target: {libsqlite3-sys|cr-sqlite|sqlite3|all}"
    fi
    
    if [ "$target" = "all" ]; then
        update_complex "libsqlite3-sys"
        update_simple "cr-sqlite"
        return
    fi
    
    get_dep_config "$target"
    
    if [ "$DEP_TYPE" = "complex" ]; then
        update_complex "$target"
    elif [ "$DEP_TYPE" = "simple" ]; then
        update_simple "$target"
    else
        error "Unknown dependency type: $DEP_TYPE"
    fi
}

# Main command dispatcher
case "${1:-}" in
    init)
        shift
        cmd_init "$@"
        ;;
    update)
        shift
        cmd_update "$@"
        ;;
    *)
        echo "Usage: $0 {init|update} {libsqlite3-sys|cr-sqlite|all}"
        echo ""
        echo "Commands:"
        echo "  init <target>   - Initialize vendor dependency"
        echo "  update <target> - Update vendor dependency to pinned commit"
        echo ""
        echo "Targets:"
        echo "  libsqlite3-sys  - Complex: rusqlite/libsqlite3-sys with orphan branch workflow"
        echo "  cr-sqlite       - Simple: superfly/cr-sqlite (squashed subtree)"
        echo "  all             - Process all dependencies"
        echo ""
        echo "Examples:"
        echo "  $0 init libsqlite3-sys"
        echo "  $0 update cr-sqlite"
        echo "  $0 init all"
        exit 1
        ;;
esac
