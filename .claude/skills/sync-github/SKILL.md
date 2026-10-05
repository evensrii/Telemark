---
name: sync-github
description: Commit and push code changes in this repo to GitHub, with checks tailored to this repo (secrets, Data/ pushed via API separately from git, commit message conventions, confirm-before-push). Use when the user asks to "sync", "commit and push", "push my changes", or similar.
---

# Sync to GitHub

Commits and pushes pending changes in this repo, with checks specific to how this repo works (see root `CLAUDE.md`).

## Steps

1. **Check status first.**
   Run `git status` (never a destructive command) and `git diff` to see what's actually changed. Do this before staging anything.

2. **Separate `Data/` changes from everything else.**
   `Data/` is normally written by query scripts pushing directly to GitHub via the Contents API (see `Helper_scripts/github_functions.py`), not via local commits. If `git status` shows changes under `Data/`, they are most likely from a `git pull`/fetch picking up those API-side commits, not something the user edited locally.
   - Do **not** silently stage/commit `Data/` changes as if they were the user's work.
   - If `Data/` has local diffs that look hand-edited, flag it explicitly and ask before including them — this is not the normal flow for that folder.
   - The rest of this skill is meant for code/content changes: `Python/`, `Kart/`, `Egne applikasjoner/`, `Nettsider/`, config, docs, etc.

3. **Screen for secrets before staging.**
   Never blanket `git add -A`/`git add .`. Stage specific files, then re-check with `git status`/`git diff --cached`. Pay special attention to:
   - `Python/token.env` (contains `GITHUB_TOKEN`) — must never be committed.
   - Anything under `Annet/` (environments, credentials, backups).
   - `Kart/sirkulare_telemark/config.js` — this file is normally rewritten by CI (`.github/workflows/deploy.yml`) to inject `MAPBOX_ACCESS_TOKEN`; don't hand-commit a version containing a real token.
   - Any new file whose name suggests credentials (`.env`, `*token*`, `*secret*`, `*credentials*`) — open and check contents even if the name looks innocuous.
   If anything suspicious turns up, stop and surface it to the user rather than continuing.

4. **Write a commit message that fits the file being changed.**
   - Code/script/content changes (`Python/Queries/...`, `Kart/`, `Nettsider/`, etc.): a normal, descriptive message about *why*, not the automated `"Updated <file>.csv - New data detected"` / `"Row count changed from X to Y"` style — that format is reserved for the data-publishing scripts themselves (see recent log for examples) and shouldn't be reused for hand-written commits.
   - Keep it concise, one focused change per commit where reasonable.
   - Append the attribution line already configured for this session (`Co-Authored-By: Claude ...`) if commits are being made by Claude Code.

5. **Show the user what will be pushed, then confirm before pushing.**
   Pushing affects shared state (origin/main) and — per `.github/workflows/deploy.yml` — a push to `main` triggers CI that injects the Mapbox secret and commits it back. Before running `git push`:
   - Show the commit(s) about to be pushed (`git log origin/main..HEAD` or equivalent) and a short diffstat.
   - Ask for explicit confirmation unless the user has already asked to push in this same turn.
   - Never force-push. If push is rejected (remote has diverged), pull/rebase and re-check rather than forcing.

6. **After pushing, report what happened.**
   State the commit(s) pushed and note if the CI workflow will run (i.e., if `main` was pushed to). Don't fetch/poll CI status unless asked.

## What this skill does NOT do

- It does not touch `Data/` publishing — that's the job of the query scripts and `handle_output_data`, not local git.
- It does not amend existing commits or rewrite history.
- It does not run `master_script.py` or any query scripts.
