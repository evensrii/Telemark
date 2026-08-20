@echo off
chcp 65001 > nul

:: Force the local repo to exactly match origin/main before running anything.
:: The server never commits its own changes, so any local drift here is
:: unexpected and safe to discard - this makes the daily run self-healing
:: instead of silently running stale code.

set "REPO_DIR=D:\Scripts\analyse\Telemark"
set "SYNC_LOG=%~dp0git_sync.log"

echo [%DATE% %TIME%] Syncing %REPO_DIR% with origin/main > "%SYNC_LOG%"
git -C "%REPO_DIR%" fetch origin main >> "%SYNC_LOG%" 2>&1
git -C "%REPO_DIR%" reset --hard origin/main >> "%SYNC_LOG%" 2>&1
echo [%DATE% %TIME%] Sync step finished >> "%SYNC_LOG%"

call conda activate analyse
python %*
