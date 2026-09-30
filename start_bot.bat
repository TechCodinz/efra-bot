@echo off
title EFRA ULTRA-PRECISION SNIPER v2 - Control Center
cd /d "%~dp0"
cls
echo =======================================================================
echo          EFRA ULTRA-PRECISION SNIPER v2 - CONTROL CENTER
echo =======================================================================
echo.
echo Select Run Mode:
echo   [1] Run Gate.io Ultra-Precision Sniper (200bps TP / adaptive profit lock) [RECOMMENDED]
echo   [2] Run Gate.io Maker Mode (post-only entry / exchange maker-fee schedule)
echo   [3] Run Kraken Mode with Terminal Dashboard
echo   [4] Run Gate.io Diagnostic (Test latency, fees, and 2,000+ spot pairs)
echo   [5] Run Performance & Edge Analytics Report
echo   [6] Reset State and Trade History (Start Clean $100 Session)
echo   [7] Exit
echo.
set choice=1
set /p choice="Enter choice [1-7] (Press ENTER for default [1]): "

if "%choice%"=="1" (
    echo Starting Gate.io High-Conviction Sniper Mode...
    python efra_bot.py --exchange gateio --paper --start-balance 100.0 --tp-bps 200 --sl-bps 40 --breakeven-bps 80 --trail-trigger-bps 100 --trail-bps 30 --min-confluence 55 --inter-trade-pause-s 8 --position-frac 0.40 --daily-loss-limit-frac 0.08 --dashboard
) else if "%choice%"=="2" (
    echo Starting Gate.io Maker Mode...
    python efra_bot.py --exchange gateio --mode maker --paper --start-balance 100.0 --tp-bps 200 --sl-bps 40 --breakeven-bps 80 --trail-trigger-bps 100 --trail-bps 30 --min-confluence 55 --inter-trade-pause-s 8 --position-frac 0.40 --daily-loss-limit-frac 0.08 --dashboard
) else if "%choice%"=="3" (
    echo Starting Kraken Mode...
    python efra_bot.py --exchange kraken --paper --start-balance 100.0 --dashboard
) else if "%choice%"=="4" (
    echo Starting Gate.io Diagnostic...
    python efra_bot.py --exchange gateio --diagnostic
) else if "%choice%"=="5" (
    python efra_report.py
) else if "%choice%"=="6" (
    echo Resetting state and trade logs...
    python efra_bot.py --exchange gateio --paper --reset --start-balance 100.0
) else (
    exit /b
)

echo.
echo =======================================================================
echo Engine process ended. Press any key to close this window.
echo =======================================================================
pause >nul
