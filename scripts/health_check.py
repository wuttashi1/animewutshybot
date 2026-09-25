#!/usr/bin/env python3
"""Read-only API checks: no gateway session, registration or messages."""
import asyncio
import json
import os
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import aiohttp
from dotenv import load_dotenv
import bot
from http_client import get_json

async def main():
    load_dotenv(ROOT / '.env')
    failed = False
    token = bot._normalize_discord_token(os.environ.get('DISCORD_BOT_TOKEN'))
    async with aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar()) as session:
        if not token:
            print('FAIL Discord: DISCORD_BOT_TOKEN missing')
            failed = True
        else:
            headers = {'Authorization': 'Bot '+token, 'User-Agent': 'DiscordBot (https://github.com/wuttashi1/animewutshybot, 1.0)'}
            _, status = await get_json(session, 'https://discord.com/api/v10/users/@me', headers=headers)
            print(f'Discord authentication: HTTP {status}')
            failed |= status != 200
            if status == 200:
                state = json.loads(bot.STATE_PATH.read_text(encoding='utf-8')) if bot.STATE_PATH.is_file() else {}
                for cfg in state.get('guilds', {}).values():
                    for key in ('forum_channel_id', 'list_forum_channel_id'):
                        channel = cfg.get(key)
                        if not channel:
                            print(f'FAIL {key}: missing')
                            failed = True
                            continue
                        data, status = await get_json(session, f'https://discord.com/api/v10/channels/{int(channel)}', headers=headers)
                        ok = status == 200 and isinstance(data, dict) and data.get('type') == 15
                        print(f'{key}: {"OK" if ok else "FAIL"} (HTTP {status})')
                        failed |= not ok
                if not state.get('guilds'):
                    print('Channels: not configured yet; use /bot setup or migrate existing forums')
        slug = await bot.api_search_slug(session, 'naruto')
        info = await bot.api_fetch_anime(session, slug) if slug else None
        print(f'Yummy search and card: {"OK" if info else "FAIL"}')
        failed |= info is None
        info = await bot.jikan_fetch_anime(session, 20)
        print(f'Jikan card: {"OK" if info else "FAIL"}')
        failed |= info is None
    return int(failed)

if __name__ == '__main__':
    try:
        raise SystemExit(asyncio.run(main()))
    except (OSError, ValueError, RuntimeError, aiohttp.ClientError) as error:
        print(f'FAIL diagnostics: {type(error).__name__}')
        raise SystemExit(1)
