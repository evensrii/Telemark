@echo off

REM Ensure conda is initialized
CALL C:\ProgramData\anaconda3\Scripts\activate.bat

REM Activate the conda environment
CALL conda activate analyse

REM Run the first Python script
python "C:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_innvandrere.py" >> log_sysselsatte.txt 2>&1
IF %ERRORLEVEL% NEQ 0 echo "Script1.py failed with error code %ERRORLEVEL%"

REM Run the second Python script
python "C:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Queries\09_Innvadrere_og_inkludering\Innvandrerbefolkningen\botid.py" >> log_botid.txt 2>&1
IF %ERRORLEVEL% NEQ 0 echo "Script2.py failed with error code %ERRORLEVEL%"

REM Log completion
echo "All scripts completed (check individual error codes)."