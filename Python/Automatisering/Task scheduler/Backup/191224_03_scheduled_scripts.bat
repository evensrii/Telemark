@echo off

:: Use UTF-8 Encoding
chcp 65001 > nul

:: Define the email log file
SET EMAIL_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_email.log"

:: Delete all log files except "readme.txt" and "00_email.log"
FOR %%F IN ("D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\*") DO (
    IF NOT "%%~nF"=="readme" (
        IF NOT "%%~nF"=="00_email" DEL "%%F"
    )
)

:: Define a master log file for debugging
SET LOGFILE="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run.log"

:: Ensure conda is initialized
:: echo [%DATE% %TIME%] Initializing Conda >> %LOGFILE%
CALL C:\Users\_eve1509\AppData\Local\anaconda3\Scripts\activate.bat >> %LOGFILE% 2>&1

:: Activate the conda environment
:: echo [%DATE% %TIME%] Activating Conda environment >> %LOGFILE%
CALL conda activate analyse >> %LOGFILE% 2>&1

:: Verify Python environment
::  python --version >> %LOGFILE% 2>&1
::  IF %ERRORLEVEL% NEQ 0 (
::      echo [%DATE% %TIME%] Python initialization failed. Aborting. >> %LOGFILE%
::      EXIT /B 1
::  )

:: Run scripts with log separation

:: Innvandrere og inkludering
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandrere_bosatt.py" "Innvandrere - Bosatt"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandringsgrunn.py" "Innvandrere - Innvandringsgrunn"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\andel_flyktninger_og_arbeidsinnvandrere.py" "Innvandrere - Flyktninger og arbeidsinnvandrere"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\botid.py" "Innvandrere - Botid"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Bosetting_av_flyktninger\anmodninger_og_faktisk_bosetting.py" "Innvandrere - Anmodninger og faktisk bosetting"


:: Run the email script and log its output
python -u "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\email_when_run_completed.py" > %EMAIL_LOG% 2>&1

:: Log completion
echo [%DATE% %TIME%] Daily run completed. >> %LOGFILE%
GOTO :EOF

:RunScript
:: Arguments: %1 = Script Path, %2 = Script Name
SET SCRIPT=%~1
SET NAME=%~2

:: Extract only the file name with extension from %SCRIPT%
SET SCRIPT_FILENAME=%~nx1

:: Define individual log file for the script
SET SCRIPT_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\%NAME:_=oe%.log"

:: Start logging for the script
:: echo [%DATE% %TIME%] Running %NAME% >> %LOGFILE%
echo [%DATE% %TIME%] Running %SCRIPT_FILENAME% >> %SCRIPT_LOG%

:: Execute the Python script
python %SCRIPT% >> %SCRIPT_LOG% 2>&1

:: Check if new_data_status.log exists and append "New Data" status to the master log
IF EXIST "new_data_status.log" (
    FOR /F "tokens=3 delims=," %%A IN ('type "new_data_status.log"') DO (
        SET NEW_DATA_STATUS=%%A
    )
    DEL "new_data_status.log"
) ELSE (
    SET NEW_DATA_STATUS=No
)

:: Append status to the master log
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Failed, %NEW_DATA_STATUS% >> %LOGFILE%
    echo [%DATE% %TIME%] Script failed with error code %ERRORLEVEL% >> %SCRIPT_LOG%
) ELSE (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Completed, %NEW_DATA_STATUS% >> %LOGFILE%
    echo [%DATE% %TIME%] Script completed >> %SCRIPT_LOG%
)

EXIT /B 0