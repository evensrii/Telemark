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

:: Check which Python is being used (logs the Python path to verify environment)
echo [%DATE% %TIME%] Checking Python versions and python paths >> %LOGFILE%
python --version >> %LOGFILE% 2>&1
where python >> %LOGFILE% 2>&1

echo [%DATE% %TIME%] Checking exactly which Python.exe is used >> %LOGFILE%
python -c "import sys; print(sys.executable)" >> %LOGFILE%

:: Run the Python script and log output
:: echo [%DATE% %TIME%] Running Python script >> %LOGFILE%
:: python "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\test.py" >> %LOGFILE% 2>&1
:: IF %ERRORLEVEL% NEQ 0 echo [%DATE% %TIME%] Script failed with error code %ERRORLEVEL% >> %LOGFILE%

:::::::: Innvandrere og inkludering

:: Andel sysselsatte innvandrere
echo [%DATE% %TIME%] ------------------------------ Running sysselsatte_innvandrere script >> %LOGFILE%
python "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_innvandrere.py" >> %LOGFILE% 2>&1
IF %ERRORLEVEL% NEQ 0 echo [%DATE% %TIME%] Script failed with error code %ERRORLEVEL% >> %LOGFILE%

:: Botid
echo [%DATE% %TIME%] ------------------------------ Running botid.py script >> %LOGFILE%
python "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Innvandrerbefolkningen\botid.py" >> %LOGFILE% 2>&1
IF %ERRORLEVEL% NEQ 0 echo [%DATE% %TIME%] Script failed with error code %ERRORLEVEL% >> %LOGFILE%

:: Log completion
echo [%DATE% %TIME%] Finished execution >> %LOGFILE%
