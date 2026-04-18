@echo off
setlocal
if exist "%LocalAppData%\Python\bin\python.exe" (
  "%LocalAppData%\Python\bin\python.exe" %*
) else (
  py -3 %*
)
exit /b %errorlevel%
