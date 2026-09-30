@echo off
title EFRA ULTRA-PRECISION SNIPER v2 - Gate.io
cd /d "%~dp0"
cls
echo =======================================================================
echo     LAUNCHING EFRA ULTRA-PRECISION SNIPER v2 - GATE.IO
echo =======================================================================
echo.
echo Exchange: Gate.io (2,067 spot pairs)
echo Mode:     Paper Simulation (Zero Risk)
echo Balance:  $100.00 USDT Clean Session
echo Strategy: +200 bps TP, -40 bps SL, +80 bps profit lock, +100/30 bps trailing
echo.
echo Initializing order book streams...
python efra_bot.py --exchange gateio --paper --start-balance 100.0 --tp-bps 200 --sl-bps 40 --breakeven-bps 80 --trail-trigger-bps 100 --trail-bps 30 --min-confluence 55 --inter-trade-pause-s 8 --position-frac 0.40 --daily-loss-limit-frac 0.08 --dashboard

echo.
echo =======================================================================
echo Bot stopped. Press any key to exit.
echo =======================================================================
pause >nul
