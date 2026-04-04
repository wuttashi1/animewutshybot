"""
Запуск бота через pythonw (без консоли).
Важно: весь код в bot.py — этот файл только подключает модуль, чтобы не было двух разных версий.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

import bot as bot_app  # noqa: E402

bot_app.main()
