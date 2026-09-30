@echo off
title EFRA MAKER v2 - Gate.io Post-Only Compounding Engine
cd /d "%~dp0"
cls
echo =======================================================================
echo     LAUNCHING EFRA MAKER v2 - PASSIVE POST-ONLY EXECUTION
echo =======================================================================
echo.
echo Exchange:   Gate.io (2,067 spot pairs)
echo Mode:       MAKER (Passive Post-Only Limit Orders at the Bid)
echo Execution:  Post-only entry; maker fee schedule is read from the exchange when available
echo Compounding: +5%% tier steps; 2-5 dynamic slots with loss-streak defensive scaling
echo.
echo Initializing order book streams...
python efra_bot.py --exchange gateio --mode maker --paper --start-balance 100.0 --tp-bps 200 --sl-bps 40 --breakeven-bps 80 --trail-trigger-bps 100 --trail-bps 30 --min-confluence 55 --inter-trade-pause-s 8 --position-frac 0.40 --daily-loss-limit-frac 0.08 --dashboard

echo.
echo =======================================================================
echo Engine process ended. Press any key to close this window.
echo =======================================================================
pause >nul
