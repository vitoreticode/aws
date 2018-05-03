echo off
set file1=%1
set file2=%2
FC /B %1 %2
if errorlevel 1 echo Unsuccessful
