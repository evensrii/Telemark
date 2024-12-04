@echo off

:: Delete the master log file if it exists
IF EXIST "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run_log.txt" (
    DEL "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run_log.txt"
)

:: Define a master log file for debugging
SET LOGFILE="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run_log.txt"

:: Ensure conda is initialized
echo [%DATE% %TIME%] Initializing Conda >> %LOGFILE%
CALL C:\Users\_eve1509\AppData\Local\anaconda3\Scripts\activate.bat >> %LOGFILE% 2>&1

:: Activate the conda environment
echo [%DATE% %TIME%] Activating Conda environment >> %LOGFILE%
CALL conda activate analyse >> %LOGFILE% 2>&1

:: Verify Python environment
python --version >> %LOGFILE% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] Python initialization failed. Aborting. >> %LOGFILE%
    EXIT /B 1
)


:: Run scripts with log separation

:: Innvandrere og inkludering
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_innvandrere.py" "Innvandrere - Andel sysselsatte"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Innvandrerbefolkningen\botid.py" "Innvandrere - Botid"

:: Areal og stedsutvikling
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\10_Areal_og_stedsutvikling\Areal_til_jordbruk\jordbruksareal_per_kommune.py" "Areal - Jordbruksareal per kommune"



:: Log completion
echo [%DATE% %TIME%] All scripts completed. >> %LOGFILE%
GOTO :EOF

:RunScript
:: Arguments: %1 = Script Path, %2 = Script Name
SET SCRIPT=%~1
SET NAME=%~2

:: Define individual log file for the script
SET SCRIPT_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\%NAME%_log.txt"

:: Start logging for the script
echo [%DATE% %TIME%] Running %NAME% script >> %LOGFILE%
echo [%DATE% %TIME%] Running %NAME% script >> %SCRIPT_LOG%

:: Execute the Python script
python %SCRIPT% >> %SCRIPT_LOG% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] %NAME% script failed with error code %ERRORLEVEL% >> %LOGFILE%
    echo [%DATE% %TIME%] Script failed with error code %ERRORLEVEL% >> %SCRIPT_LOG%
)

:: Finish script logging
echo [%DATE% %TIME%] Completed %NAME% script >> %LOGFILE%
echo [%DATE% %TIME%] Completed script >> %SCRIPT_LOG%
EXIT /B 0