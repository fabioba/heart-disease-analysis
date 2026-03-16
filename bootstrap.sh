#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh  —  one-time setup for pre-commit on any laptop
#
# Works on:  macOS (zsh/bash)  |  Windows Git Bash  |  Windows WSL
#
# Usage:
#   chmod +x bootstrap.sh && ./bootstrap.sh        (Mac / Git Bash / WSL)
# =============================================================================

set -e

BOLD=$(tput bold 2>/dev/null || echo "")
RESET=$(tput sgr0 2>/dev/null || echo "")
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
NC="\033[0m"

info()    { echo -e "${GREEN}▶ $*${NC}"; }
warn()    { echo -e "${YELLOW}⚠ $*${NC}"; }
heading() { echo -e "\n${BOLD}$*${RESET}"; }

# -----------------------------------------------------------------------------
# 1. Detect OS
# -----------------------------------------------------------------------------
heading "1/4  Detecting environment"
OS="$(uname -s)"
case "$OS" in
  Darwin) info "macOS detected" ;;
  Linux)  info "Linux / WSL detected" ;;
  MINGW*|CYGWIN*|MSYS*) info "Windows Git Bash detected" ;;
  *) warn "Unknown OS: $OS — proceeding anyway" ;;
esac

# -----------------------------------------------------------------------------
# 2. Install uv (cross-platform Python manager)
# -----------------------------------------------------------------------------
heading "2/4  Installing uv"
if command -v uv &>/dev/null; then
  info "uv already installed: $(uv --version)"
else
  info "Downloading uv installer..."
  curl -LsSf https://astral.sh/uv/install.sh | sh

  # Add uv to PATH for the rest of this script
  export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

  if ! command -v uv &>/dev/null; then
    warn "uv installed but not found in PATH. Please restart your terminal and re-run."
    exit 1
  fi
  info "uv installed: $(uv --version)"
fi

# -----------------------------------------------------------------------------
# 3. Install pre-commit via uv (isolated, no virtual env needed)
# -----------------------------------------------------------------------------
heading "3/4  Installing pre-commit"
if command -v pre-commit &>/dev/null; then
  info "pre-commit already installed: $(pre-commit --version)"
else
  uv tool install pre-commit
  # Ensure uv tool bin is on PATH
  export PATH="$(uv tool dir)/../bin:$PATH"
  info "pre-commit installed: $(pre-commit --version)"
fi

# -----------------------------------------------------------------------------
# 4. Install the git hook in this repo
# -----------------------------------------------------------------------------
heading "4/4  Installing git hook"

if [ ! -d ".git" ]; then
  warn "No .git directory found. Are you in the root of the repo? Skipping hook install."
  exit 1
fi

pre-commit install
info "Git pre-commit hook installed ✓"

# Optionally bootstrap the secrets baseline if not present
if [ ! -f ".secrets.baseline" ]; then
  info "Creating initial secrets baseline (.secrets.baseline)..."
  uv tool run detect-secrets scan > .secrets.baseline
  info ".secrets.baseline created — commit this file to the repo."
fi

echo ""
echo -e "${GREEN}${BOLD}All done!${RESET}"
echo "pre-commit will now run automatically on every 'git commit'."
echo ""
echo "Useful commands:"
echo "  pre-commit run --all-files   # run on entire codebase manually"
echo "  pre-commit autoupdate        # bump hook versions"
