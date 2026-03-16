# =============================================================================
# bootstrap.ps1  —  one-time setup for pre-commit (Windows PowerShell)
#
# Usage (run from repo root in PowerShell):
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\bootstrap.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "▶ $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Head($msg)  { Write-Host "`n$msg" -ForegroundColor White }

# -----------------------------------------------------------------------------
# 1. Install uv
# -----------------------------------------------------------------------------
Head "1/4  Installing uv"
if (Get-Command uv -ErrorAction SilentlyContinue) {
    Info "uv already installed: $(uv --version)"
} else {
    Info "Downloading uv..."
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
    # Reload PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path","User")
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        Warn "uv installed but not in PATH. Please restart PowerShell and re-run."
        exit 1
    }
    Info "uv installed: $(uv --version)"
}

# -----------------------------------------------------------------------------
# 2. Install pre-commit via uv
# -----------------------------------------------------------------------------
Head "2/4  Installing pre-commit"
if (Get-Command pre-commit -ErrorAction SilentlyContinue) {
    Info "pre-commit already installed: $(pre-commit --version)"
} else {
    uv tool install pre-commit
    # Reload PATH again
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path","User")
    Info "pre-commit installed: $(pre-commit --version)"
}

# -----------------------------------------------------------------------------
# 3. Install the git hook
# -----------------------------------------------------------------------------
Head "3/4  Installing git hook"
if (-not (Test-Path ".git")) {
    Warn "No .git directory found. Are you in the root of the repo?"
    exit 1
}
pre-commit install
Info "Git hook installed ✓"

# -----------------------------------------------------------------------------
# 4. Secrets baseline
# -----------------------------------------------------------------------------
Head "4/4  Secrets baseline"
if (-not (Test-Path ".secrets.baseline")) {
    Info "Creating .secrets.baseline..."
    uv tool run detect-secrets scan | Out-File -FilePath ".secrets.baseline" -Encoding utf8
    Info ".secrets.baseline created — commit this file to the repo."
}

Write-Host "`nAll done! pre-commit will run automatically on every git commit." -ForegroundColor Green
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  pre-commit run --all-files   # run on entire repo"
Write-Host "  pre-commit autoupdate        # bump hook versions"
