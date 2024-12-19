@echo on
setlocal

:: Define a global debug log file
SET DEBUG_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\debug.log"

:: Use UTF-8 Encoding
chcp 65001 > nul >> %DEBUG_LOG% 2>&1

:: Define the email log file
SET EMAIL_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_email.log"

:: Log paths and environment variables
echo [%DATE% %TIME%] Email log path: %EMAIL_LOG% >> %DEBUG_LOG%
echo [%DATE% %TIME%] Debug log path: %DEBUG_LOG% >> %DEBUG_LOG%

:: Delete all log files except "readme.txt" and "00_email.log"
FOR %%F IN ("D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\*") DO (
    IF NOT "%%~nF"=="readme" (
        IF NOT "%%~nF"=="00_email" DEL "%%F" >> %DEBUG_LOG% 2>&1
    )
)

:: Define a master log file for debugging
SET LOGFILE="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run.log"
echo [%DATE% %TIME%] Master log path: %LOGFILE% >> %DEBUG_LOG%

:: Ensure conda is initialized
CALL "C:\Users\_eve1509\AppData\Local\anaconda3\Scripts\activate.bat" >> %DEBUG_LOG% 2>&1

:: Activate the conda environment
CALL conda activate analyse >> %DEBUG_LOG% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] Conda environment activation failed. >> %DEBUG_LOG%
    EXIT /B 1
)

:: Verify Python environment
python --version >> %DEBUG_LOG% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] Python environment verification failed. >> %DEBUG_LOG%
    EXIT /B 1
)

:: Run scripts with log separation

CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandrere_bosatt.py" "Innvandrere - Bosatt"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandringsgrunn.py" "Innvandrere - Innvandringsgrunn"

:: Run the email script and log its output
python -u "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\email_when_run_completed.py" >> %EMAIL_LOG% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] Email script failed. >> %DEBUG_LOG%
    EXIT /B 1
)

:: Log completion
echo [%DATE% %TIME%] Daily run completed. >> %LOGFILE%
GOTO :EOF

:RunScript
:: Arguments: %1 = Script Path, %2 = Script Name
SET SCRIPT=%~1
SET NAME=%~2

:: Log the script being executed
SET SCRIPT_FILENAME=%~nx1
SET SCRIPT_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\%NAME:_=oe%.log"
echo [%DATE% %TIME%] Running script: %SCRIPT_FILENAME% >> %DEBUG_LOG%

:: Execute the Python script
python %SCRIPT% >> %SCRIPT_LOG% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Failed. >> %DEBUG_LOG%
    EXIT /B 1
) ELSE (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Completed. >> %DEBUG_LOG%
)

EXIT /B 0
