@echo off
setlocal

cd /d "%~dp0"

set "PYTHON_CMD="
set "PYTHON_ARGS="

where py >nul 2>nul
if %ERRORLEVEL%==0 (
    set "PYTHON_CMD=py"
    set "PYTHON_ARGS=-3"
    goto :run
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
    set "PYTHON_CMD=python"
    set "PYTHON_ARGS="
    goto :run
)

where uv >nul 2>nul
if %ERRORLEVEL%==0 (
    set "PYTHON_CMD=uv"
    set "PYTHON_ARGS=run python"
    goto :run
)

echo No suitable Python launcher found (tried: py -3, python, uv run python).
set "EXIT_CODE=1"
goto :end

:run
if "%PYTHON_ARGS%"=="" (
    "%PYTHON_CMD%" "%~dp0run_report.py"
) else (
    "%PYTHON_CMD%" %PYTHON_ARGS% "%~dp0run_report.py"
)
set "EXIT_CODE=%ERRORLEVEL%"

:end
echo.
echo Script finished with exit code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
