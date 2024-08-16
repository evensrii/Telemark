REM Disables the display of each command in the command prompt window as they are executed.
@echo off

REM Ensure conda is initialized (this is necessary to set up the environment). This file is default anaconda3.
CALL C:\ProgramData\anaconda3\Scripts\activate.bat

REM Activates the conda environment (if not already activated)
CALL conda activate analyse

REM Run the python script
python "C:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Queries\NVE\vannkraft.py"