"""Open archived threads for requested writes without overriding moderator locks."""
import discord


async def ensure_thread_writable(thread: discord.Thread) -> discord.Thread:
    if thread.archived and not thread.locked:
        return await thread.edit(archived=False, reason="Anime bot: requested list or panel update")
    return thread
