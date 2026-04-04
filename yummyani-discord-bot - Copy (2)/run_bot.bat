@echo off
cd /d "%~dp0"
title YummyAnime Discord Bot
echo Запуск бота... Окно можно свернуть. Закройте окно или Ctrl+C, чтобы остановить.
python bot.py
echo.
echo Процесс завершился с кодом %ERRORLEVEL%.
pause
