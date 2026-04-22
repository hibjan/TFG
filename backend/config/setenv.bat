@echo off

:: %~dp0 points to the bin\ folder. We go up 3 levels to hit the TFG root.
:: The FOR loop forces Windows to calculate the clean absolute path immediately.
for %%I in ("%~dp0..\..\..\.env.db") do set "ENV_FILE=%%~fI"

if exist "%ENV_FILE%" (
    echo SUCCESS: Loading DB variables from %ENV_FILE%
    
    :: findstr /B "DB_" ensures we ONLY load database credentials
    :: The loop splits the line at the = sign and safely sets the variable in memory
    for /f "tokens=1,* delims==" %%A in ('findstr /B "DB_" "%ENV_FILE%"') do (
        set "%%A=%%B"
    )
) else (
    echo ERROR: .env.db file not found at %ENV_FILE%
)