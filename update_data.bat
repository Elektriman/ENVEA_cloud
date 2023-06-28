@echo off

rem Get the directory path of the script
for %%I in (%0) do set SCRIPT_DIR=%%~dpI

rem Specify the name of the virtual environment folder
set VENV_FOLDER=venv

rem Set the virtual environment path by combining the script directory and the virtual environment folder
set VENV_PATH=%SCRIPT_DIR%%VENV_FOLDER%

rem Activate the virtual environment
call %VENV_PATH%\Scripts\activate.bat

call cd %SCRIPT_DIR%

rem Run your Python script
python extraction.py

rem Deactivate the virtual environment
call %VENV_PATH%\Scripts\deactivate.bat

pause