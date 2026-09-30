@echo off
title EFRA SNIPER - Gate.io Live Market Engine
cd /d "%~dp0"
cls
echo =======================================================================
echo     LAUNCHING GATE.IO HIGH-CONVICTION VOLATILITY SNIPER (3:1 R:R)
echo =======================================================================
echo.
echo Exchange: Gate.io (2,067 spot pairs)
echo Mode:     Paper Simulation (Zero Risk)
echo Balance:  $100.00 USDT Clean Session
echo Strategy: +150 bps TP, -40 bps SL, +55 bps Breakeven Lock, Dynamic Trailing Stop
echo.
echo Initializing order book streams...
python efra_bot.py --exchange gateio --paper --start-balance 100.0 --dashboard

echo.
echo =======================================================================
echo Bot stopped. Press any key to exit.
echo =======================================================================
pause >nul
