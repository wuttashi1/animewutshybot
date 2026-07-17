#!/usr/bin/env python3
"""Статическая проверка модулей и наличия ключевых функций (без Discord-токена)."""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        msg = f"FAIL {name}" + (f" — {detail}" if detail else "")
        print(msg)
        FAILURES.append(msg)


def main() -> int:
    print("=== Module imports ===")
    modules = {}
    for mod_name in (
        "guild_config",
        "yummy_api",
        "roaster",
        "roaster_automation",
        "personal_display",
        "register_commands",
    ):
        try:
            modules[mod_name] = __import__(mod_name)
            check(f"import {mod_name}", True)
        except Exception as e:
            check(f"import {mod_name}", False, str(e))

    print("\n=== guild_config ===")
    gc = modules.get("guild_config")
    if gc:
        sig = inspect.signature(gc.setup_guild_channels)
        check(
            "setup_guild_channels(category_name=...)",
            "category_name" in sig.parameters,
        )
        check("DEFAULT_CATEGORY_NAME", hasattr(gc, "DEFAULT_CATEGORY_NAME"))
        check("format_guild_status", callable(getattr(gc, "format_guild_status", None)))

    print("\n=== yummy_api ===")
    ya = modules.get("yummy_api")
    if ya:
        check("yani_login_password", callable(getattr(ya, "yani_login_password", None)))
        check("yani_get_profile", callable(getattr(ya, "yani_get_profile", None)))
        check("yani_fetch_lists_with_token_refresh", callable(
            getattr(ya, "yani_fetch_lists_with_token_refresh", None)
        ))

    print("\n=== roaster ===")
    ro = modules.get("roaster")
    if ro:
        check("build_roast_message", callable(getattr(ro, "build_roast_message", None)))
        check("ROAST_TEMPLATES", len(getattr(ro, "ROAST_TEMPLATES", ())) > 10)

    print("\n=== personal_display ===")
    pd = modules.get("personal_display")
    if pd:
        check("rebuild_display", callable(getattr(pd, "rebuild_display", None)))
        check("DISPLAY_MODES", "summary" in getattr(pd, "DISPLAY_MODES", ()))

    print("\n=== register_commands ===")
    rc = modules.get("register_commands")
    if rc:
        check("setup()", callable(getattr(rc, "setup", None)))

    print("\n=== bot (core, no Discord login) ===")
    try:
        import bot as core  # noqa: WPS433

        check("migrate_legacy_guild_config", callable(getattr(core, "migrate_legacy_guild_config", None)))
        check("is_roaster_active", callable(getattr(core, "is_roaster_active", None)))
        check("get_guild_cfg", callable(getattr(core, "get_guild_cfg", None)))
        check("save_guild_cfg", callable(getattr(core, "save_guild_cfg", None)))
        check("pick_roast_titles", callable(getattr(core, "pick_roast_titles", None)))
        check("rebuild_personal_list_display", callable(
            getattr(core, "rebuild_personal_list_display", None)
        ))
    except Exception as e:
        check("import bot", False, str(e))

    print("\n=== Summary ===")
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s)")
        for f in FAILURES:
            print(f"  {f}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
