@echo off
rem Headless JMeter run: run-jmeter.bat [threads] [loops]
setlocal
set COURSE_DIR=%USERPROFILE%\Documents\School\semester4\ACIT3855
set THREADS=%1
if "%THREADS%"=="" set THREADS=10
set LOOPS=%2
if "%LOOPS%"=="" set LOOPS=10

set OUT_DIR=%COURSE_DIR%\project\jmeter
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"
if exist "%OUT_DIR%\results.jtl" del "%OUT_DIR%\results.jtl"
if exist "%OUT_DIR%\report" rmdir /s /q "%OUT_DIR%\report"

echo %THREADS% threads, %LOOPS% loops
java -jar "%COURSE_DIR%\apache-jmeter-5.6.3\bin\ApacheJMeter.jar" ^
  -t "%COURSE_DIR%\project\Thread Group.jmx" ^
  -Jthreads=%THREADS% -Jloops=%LOOPS% ^
  -n -l "%OUT_DIR%\results.jtl" -e -o "%OUT_DIR%\report"
