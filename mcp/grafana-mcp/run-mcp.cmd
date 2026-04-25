@echo off
setlocal
set "SCRIPT=%~dp0src\server.mjs"

REM Optional: set NODE_EXE in Cursor MCP env to your node.exe full path.
if defined NODE_EXE if exist "%NODE_EXE%" (
  "%NODE_EXE%" "%SCRIPT%"
  exit /b %ERRORLEVEL%
)

REM Default Node.js installer locations (Windows).
if exist "%ProgramFiles%\nodejs\node.exe" (
  "%ProgramFiles%\nodejs\node.exe" "%SCRIPT%"
  exit /b %ERRORLEVEL%
)
if exist "%ProgramFiles(x86)%\nodejs\node.exe" (
  "%ProgramFiles(x86)%\nodejs\node.exe" "%SCRIPT%"
  exit /b %ERRORLEVEL%
)

where node >nul 2>nul
if %ERRORLEVEL%==0 (
  node "%SCRIPT%"
  exit /b %ERRORLEVEL%
)

echo [grafana-mcp] Node.js not found. Install Node 20+ from https://nodejs.org/ or set NODE_EXE to the full path of node.exe 1>&2
exit /b 1
