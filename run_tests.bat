@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat" >nul 2>&1
cd /d C:\Project\hid-ingest
cmake --build build --config Release --target spsc_test -- /m
build\Release\spsc_test.exe
