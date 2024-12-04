@echo off

:: Delete the log file if it exists
IF EXIST "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\task_scheduler_debug.txt" (
    DEL "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\task_scheduler_debug.txt"
)

:: Define a log file for debugging
SET LOGFILE="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\task_scheduler_debug.txt"

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

:: Run scripts

:: Innvandrere og inkludering
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_innvandrere.py" "Sysselsatte innvandrere"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Innvandrerbefolkningen\botid.py" "Botid"

:: Areal og stedsutvikling
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\10_Areal_og_stedsutvikling\Areal_til_jordbruk\jordbruksareal_per_kommune.py" "Jordbruksareal per kommune"

:: Log completion
echo [%DATE% %TIME%] All scripts completed. >> %LOGFILE%
GOTO :EOF

:RunScript
SET SCRIPT=%~1
SET NAME=%~2
echo [%DATE% %TIME%] ------------------------------------------- Running %NAME% >> %LOGFILE%
python %SCRIPT% >> %LOGFILE% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] %NAME% failed with error code %ERRORLEVEL% >> %LOGFILE%
)
EXIT /B 0
