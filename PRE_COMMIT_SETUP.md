# Pre-commit setup guide

This repo uses [pre-commit](https://pre-commit.com) to enforce code quality checks automatically on every `git commit`.
You need to run the bootstrap **once** on each laptop. After that, everything is automatic.

---

## What runs on every commit

| Hook | What it checks |
|---|---|
| `ruff` | Python linting + import sorting (replaces flake8 / isort) |
| `ruff-format` | Python formatting (replaces black) |
| `sqlfluff-lint` | SQL style & syntax for all `.sql` / dbt models |
| `sqlfluff-fix` | Auto-fixes fixable SQL issues |
| `check-model-has-description` | Every dbt model must have a description in schema.yml |
| `check-model-has-tests` | Every dbt model must have at least 1 test |
| `detect-secrets` | Prevents accidental commit of API keys / passwords |
| `trailing-whitespace`, `end-of-file-fixer` | General hygiene |
| `check-merge-conflict` | Blocks commits with unresolved merge markers |
| `debug-statements` | Catches leftover `breakpoint()` / `pdb.set_trace()` |

---

## One-time setup

### macOS or Windows Git Bash / WSL

```bash
chmod +x bootstrap.sh
```

```bash
./bootstrap.sh
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\bootstrap.ps1
```

That's it. The script:
1. Installs **uv** (fast, cross-platform Python tool manager)
2. Installs **pre-commit** via uv (no virtual env needed)
3. Registers the git hook — pre-commit now runs automatically

---

## Daily workflow

Nothing changes. Just `git commit` as usual.

If a hook fails, the commit is blocked and you'll see what needs fixing.
Most issues (formatting, SQL style) are **auto-fixed** — just `git add` the changed files and commit again.

```
git commit -m "feat: add revenue model"
# → ruff-format fixed 2 files
# → sqlfluff-fix fixed models/finance/revenue.sql

git add -u
git commit -m "feat: add revenue model"
# → all checks pass ✓
```

---

## Running manually

```bash
# Run all hooks on all files (useful when first setting up)
pre-commit run --all-files

# Run a single hook
pre-commit run ruff --all-files
pre-commit run sqlfluff-lint --all-files

# Skip hooks for a one-off emergency commit (use sparingly)
git commit --no-verify -m "hotfix: ..."
```

---

## Updating hook versions

Hook versions are pinned in `.pre-commit-config.yaml`. To bump all to latest:

```bash
pre-commit autoupdate
git add .pre-commit-config.yaml
git commit -m "chore: bump pre-commit hook versions"
```

---

## Configuration files

| File | Purpose |
|---|---|
| `.pre-commit-config.yaml` | Hook definitions and versions |
| `.sqlfluff` | SQL dialect, capitalisation rules |
| `pyproject.toml` → `[tool.ruff]` | Python linting rules |
| `.secrets.baseline` | Known-safe patterns for detect-secrets |

---

## Troubleshooting

**"pre-commit not found" after bootstrap**
Restart your terminal so the new PATH takes effect, then run `bootstrap.sh` again.

**SQLFluff is too slow**
Add a `.sqlfluffignore` file (same syntax as `.gitignore`) to exclude generated or vendored SQL.

**I need to commit but a hook is broken**
Use `git commit --no-verify` as a last resort and open a ticket to fix the hook.

**detect-secrets is flagging a false positive**
Update the baseline: `detect-secrets scan > .secrets.baseline` and commit it.
