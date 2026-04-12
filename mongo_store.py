"""Опциональное хранение состояния бота в MongoDB (токены Yummy/MAL и весь mal_state)."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_client: Any = None
STATE_COLL = "bot_documents"
DOC_MAIN_STATE = "main_state"
DOC_YUMMY_LINK = "yummy_link_state"


def mongo_enabled() -> bool:
    return bool((os.environ.get("MONGODB_URI") or os.environ.get("MONGO_URI") or "").strip())


def _db_name() -> str:
    return (os.environ.get("MONGODB_DATABASE") or "wutshy_bot").strip() or "wutshy_bot"


def database_name() -> str:
    return _db_name()


def _collection():
    global _client
    if not mongo_enabled():
        return None
    try:
        from pymongo import MongoClient
    except ImportError:
        logger.error(
            "Задан MONGODB_URI, но пакет pymongo не установлен. Установите: pip install pymongo"
        )
        return None
    if _client is None:
        uri = (os.environ.get("MONGODB_URI") or os.environ.get("MONGO_URI") or "").strip()
        _client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    return _client[_db_name()][STATE_COLL]


def load_json_document(doc_key: str) -> dict[str, Any] | None:
    coll = _collection()
    if coll is None:
        return None
    try:
        doc = coll.find_one({"_id": doc_key})
    except Exception:
        logger.exception("MongoDB read %s", doc_key)
        return None
    if not doc or not isinstance(doc.get("data"), dict):
        return None
    return doc["data"]


def save_json_document(doc_key: str, data: dict[str, Any]) -> bool:
    coll = _collection()
    if coll is None:
        return False
    try:
        coll.replace_one(
            {"_id": doc_key},
            {"_id": doc_key, "data": data},
            upsert=True,
        )
        return True
    except Exception:
        logger.exception("MongoDB write %s", doc_key)
        return False


def mirror_json_to_file_enabled() -> bool:
    raw = (os.environ.get("MONGODB_MIRROR_JSON") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def local_first_enabled() -> bool:
    """
    Если Mongo включён: сначала локальные JSON в data/, синхронизация в облако в фоне.
    Отключить: MONGODB_LOCAL_FIRST=0 (режим «облако первично», как раньше).
    """
    if not mongo_enabled():
        return False
    raw = (os.environ.get("MONGODB_LOCAL_FIRST") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def cloud_flush_interval_sec() -> float:
    raw = (os.environ.get("MONGODB_CLOUD_FLUSH_SEC") or "2").strip()
    try:
        v = float(raw.replace(",", "."))
    except ValueError:
        v = 2.0
    return max(0.5, min(120.0, v))


def ping() -> bool:
    coll = _collection()
    if coll is None:
        return False
    try:
        coll.database.client.admin.command("ping")
        return True
    except Exception:
        logger.exception("MongoDB ping")
        return False
