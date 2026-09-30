@echo off
title EFRA MAKER - Gate.io Zero-Taker-Fee Compounding Engine
cd /d "%~dp0"
cls
echo =======================================================================
echo     LAUNCHING GATE.IO MAKER MODE (ZERO TAKER FEES + 80X COMPOUNDING)
echo =======================================================================
echo.
echo Exchange:   Gate.io (2,067 spot pairs)
echo Mode:       MAKER (Passive Post-Only Limit Orders at the Bid)
echo Fee Hurdle: 0 - 15 bps (SAVES 10-15 bps spread + Bypasses 42 bps taker fee)
echo Compounding: Dynamic 80%% allocation scaling up to 80x ($8,000+ equity)
echo.
echo Initializing order book streams...
python efra_bot.py --exchange gateio --mode maker --paper --start-balance 100.0 --dashboard

echo.
echo =======================================================================
echo Engine process ended. Press any key to close this window.
echo =======================================================================
pause >nul
