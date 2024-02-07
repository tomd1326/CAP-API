@echo off
setlocal enabledelayedexpansion

set "python_script="
set "most_recent_time=0"

rem Loop through all .py files in the current directory
for %%f in (CAP_Pricing_Tom*.py) do (
    set "file=%%f"
    for %%g in (!file!) do (
        set "file_time=%%~tg"
        if !file_time! geq !most_recent_time! (
            set "most_recent_time=!file_time!"
            set "python_script=!file!"
        )
    )
)

if not "%python_script%"=="" (
    echo Running %python_script% %*
    python "%python_script%" %*
) else (
    echo No CAP_Pricing_Tom.py file found in the current directory.
)

pause
