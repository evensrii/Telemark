@echo off
chcp 65001 > nul

:: Force the local repo to exactly match origin/main before running anything.
:: The server never commits its own changes, so any local drift here is
:: unexpected and safe to discard - this makes the daily run self-healing
:: instead of silently running stale code.

set "REPO_DIR=C:\Analyse\Telemark"
set "SYNC_LOG=%~dp0git_sync.log"

echo [%DATE% %TIME%] Syncing %REPO_DIR% with origin/main > "%SYNC_LOG%"
git -C "%REPO_DIR%" fetch origin main >> "%SYNC_LOG%" 2>&1
git -C "%REPO_DIR%" reset --hard origin/main >> "%SYNC_LOG%" 2>&1
echo [%DATE% %TIME%] Sync step finished >> "%SYNC_LOG%"

set "RUN_LOG=%~dp0run_debug.log"
echo [%DATE% %TIME%] whoami: %USERNAME% > "%RUN_LOG%"
echo [%DATE% %TIME%] PYTHONPATH=%PYTHONPATH% >> "%RUN_LOG%"
echo [%DATE% %TIME%] TEMP_FOLDER=%TEMP_FOLDER% >> "%RUN_LOG%"
echo [%DATE% %TIME%] LOG_FOLDER=%LOG_FOLDER% >> "%RUN_LOG%"
echo [%DATE% %TIME%] PATH=%PATH% >> "%RUN_LOG%"

call conda activate analyse >> "%RUN_LOG%" 2>&1
echo [%DATE% %TIME%] conda activate errorlevel=%ERRORLEVEL% >> "%RUN_LOG%"
where python >> "%RUN_LOG%" 2>&1

echo [%DATE% %TIME%] Running: python %* >> "%RUN_LOG%"
python %* >> "%RUN_LOG%" 2>&1
echo [%DATE% %TIME%] python errorlevel=%ERRORLEVEL% >> "%RUN_LOG%"
