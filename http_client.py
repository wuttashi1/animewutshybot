"""Bounded, secret-safe JSON GETs with retries for transient failures only."""
import asyncio
import logging

import aiohttp

logger = logging.getLogger(__name__)


async def get_json(session, url, *, headers=None, params=None):
    for attempt in range(3):
        delay = 2 ** attempt
        try:
            async with session.get(url, headers=headers, params=params,
                                   timeout=aiohttp.ClientTimeout(total=20),
                                   allow_redirects=False) as response:
                status = response.status
                if status == 429 or 500 <= status < 600:
                    try:
                        delay = max(0, min(float(response.headers.get('Retry-After', delay)), 30))
                    except (TypeError, ValueError):
                        pass
                else:
                    if status != 200:
                        return None, status
                    try:
                        return await response.json(content_type=None), status
                    except (ValueError, aiohttp.ContentTypeError):
                        logger.warning('API returned invalid JSON (HTTP %s)', status)
                        return None, status
        except (aiohttp.ClientError, asyncio.TimeoutError):
            status = 0
        if attempt < 2:
            await asyncio.sleep(delay)
    return None, status
