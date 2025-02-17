@echo off
SETLOCAL EnableDelayedExpansion

:: Use UTF-8 Encoding
chcp 65001 > nul

:: Set up environment
SET PYTHONPATH=D:\Scripts\analyse\Telemark\Python
SET LOGFILE="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_master_run.log"
SET EMAIL_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\00_email.log"

:: Delete all log files except "readme.txt" and "00_email.log"
FOR %%F IN ("D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\*") DO (
    IF NOT "%%~nF"=="readme" (
        IF NOT "%%~nF"=="00_email" DEL "%%F"
    )
)

:: Initialize log file
echo [%DATE% %TIME%] Initializing Conda > %LOGFILE%

:: Activate Conda environment
echo [%DATE% %TIME%] Activating Conda environment >> %LOGFILE%
CALL conda activate analyse >> %LOGFILE% 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [%DATE% %TIME%] Failed to activate Conda environment. >> %LOGFILE%
    EXIT /B 1
)

:: Run scripts with log separation
echo [%DATE% %TIME%] Starting script execution >> %LOGFILE%

:: Run all scripts first
CALL :RunAllScripts

:: Only after all scripts are done, send email and log completion
echo [%DATE% %TIME%] Running email script >> %LOGFILE%
echo. > %EMAIL_LOG%
python -u "D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\email_when_run_completed.py" >> %EMAIL_LOG% 2>&1

:: Final status logging
echo [%DATE% %TIME%] Daily run completed. >> %LOGFILE%
EXIT /B 0

:RunAllScripts
:: Innvandrere og inkludering
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandrere_bosatt.py" "Innvandrere - Bosatt"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\innvandringsgrunn.py" "Innvandrere - Innvandringsgrunn"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\andel_flyktninger_og_arbeidsinnvandrere.py" "Innvandrere - Flyktninger og arbeidsinnvandrere"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Introduksjonsprogrammet\deltakere_introduksjonsprogram.py" "Innvandrere - Deltakere introdukjonsprogrammet"
CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Introduksjonsprogrammet\etter_introduksjonsprogram.py" "Innvandrere - Etter introduksjonsprogrammet"

:: Commented out scripts below
:: Innvandrere og inkludering
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Innvandrerbefolkningen\botid.py" "Innvandrere - Botid"

:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Bosetting_av_flyktninger\anmodninger_og_faktisk_bosetting.py" "Innvandrere - Anmodninger og faktisk bosetting"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Bosetting_av_flyktninger\enslige_mindreaarige.py" "Innvandrere - Enslige mindreaarige"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Bosetting_av_flyktninger\sekundaerflytting.py" "Innvandrere - Sekundaerflytting"

:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Utdanning\minoriteter_barnehage.py" "Innvandrere - Minoriteter i barnehage"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Utdanning\innv_fullfort_vgo.py" "Innvandrere - Fullfort VGO"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Utdanning\innv_hoyeste_utdanning.py" "Innvandrere - Hoyeste utdanning"

:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_innvandrere.py" "Innvandrere - Sysselsatte"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Arbeid_og_inntekt\andel_sysselsatte_etter_botid_og_landbakgrunn.py" "Innvandrere - Sysselsatte etter botid og bakgrunn"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\09_Innvandrere_og_inkludering\Arbeid_og_inntekt\andel_innvandrere_i_lavinntekt.py" "Innvandrere - Lavinntekt"

:: Klima og energi
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\04_Klima_og_energi\Klimagassutslipp\klimagassutslipp.py" "Klima og energi - Sektorvise utslipp"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\04_Klima_og_energi\Klimagassutslipp\norskeutslipp.py" "Klima og energi - Utslipp fra landbasert industri"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\04_Klima_og_energi\Ressursforvaltning\okologisk_tilstand_server_versjon.py" "Klima og energi - Okologisk tilstand vann"
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\04_Klima_og_energi\Ressursforvaltning\antall_felt.py" "Klima og energi - Felte hjortedyr"

:: Areal og stedsutvikling
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\10_Areal_og_stedsutvikling\Areal_til_jordbruk\jordbruksareal_per_kommune.py" "Areal - Jordbruksareal per kommune"

:: Idrett, friluftsliv og frivillighet
:: CALL :RunScript "D:\Scripts\analyse\Telemark\Python\Queries\07_Idrett_friluftsliv_og_frivillighet\Friluftsliv\andel_jegere.py" "Idrett, friluftsliv og frivillighet - Jegere"

EXIT /B 0

:RunScript
SET SCRIPT=%~1
SET NAME=%~2
SET SCRIPT_FILENAME=%~nx1
SET SCRIPT_LOG="D:\Scripts\analyse\Telemark\Python\Automatisering\Task scheduler\logs\%NAME:_=oe%.log"

echo [%DATE% %TIME%] Running %SCRIPT_FILENAME% > %SCRIPT_LOG%

CMD /C python %SCRIPT% >> %SCRIPT_LOG% 2>&1
SET PYTHON_ERROR=%ERRORLEVEL%

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
IF %PYTHON_ERROR% NEQ 0 (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Failed, %NEW_DATA_STATUS% >> %LOGFILE%
    echo [%DATE% %TIME%] Script failed with error code %PYTHON_ERROR% >> %SCRIPT_LOG%
) ELSE (
    echo [%DATE% %TIME%] %NAME% : %SCRIPT_FILENAME% : Completed, %NEW_DATA_STATUS% >> %LOGFILE%
    echo [%DATE% %TIME%] Script completed >> %SCRIPT_LOG%
)

EXIT /B %PYTHON_ERROR%