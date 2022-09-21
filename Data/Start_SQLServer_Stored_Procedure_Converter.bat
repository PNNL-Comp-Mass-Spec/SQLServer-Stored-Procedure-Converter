@echo off

set ExePath=SQLServer_Stored_Procedure_Converter.exe

if exist %ExePath% goto DoWork
if exist ..\%ExePath% set ExePath=..\%ExePath% && goto DoWork
if exist ..\bin\%ExePath% set ExePath=..\bin\%ExePath% && goto DoWork

echo Executable not found: %ExePath%
goto Done

:DoWork
echo.
echo Processing with %ExePath%
echo.
@echo On

%ExePath% /conf:StoredProcedureConverterOptions_ManagerControl.conf
