import { Client, ForumChannel } from "discord.js";
import { config } from "./config.js";
import { getAnimeTopic, getGuildConfig, loadState, saveAnimeTopic, upsertPersonalListItem } from "./storage.js";
import { filterYummyEntries, getYummyUserLists } from "./yummyApi.js";

async function runYummyBackgroundSync(client: Client): Promise<void> {
  const state = await loadState();
  const guild = client.guilds.cache.get(config.guildId);
  if (!guild) {
    return;
  }
  const cfg = await getGuildConfig(config.guildId);
  const forumId = cfg?.forumChannelId;
  if (!forumId) {
    return;
  }
  const channel = await guild.channels.fetch(forumId).catch(() => null);
  if (!(channel instanceof ForumChannel)) {
    return;
  }
  let totalCreated = 0;
  for (const binding of Object.values(state.yummyBindings)) {
    if (!binding.accessToken) {
      continue;
    }
    const entries = filterYummyEntries(
      await getYummyUserLists({
        accessToken: binding.accessToken,
        yummyUserId: binding.yummyUserId,
        appToken: config.yummyApplicationToken,
        userAgent: config.userAgent
      }),
      "all"
    ).slice(0, 8);
    for (const entry of entries) {
      const slug = (entry.anime_url || "").trim();
      if (!slug) continue;
      if (await getAnimeTopic(slug)) continue;
      const title = (entry.title || slug).trim();
      const pageUrl = `https://en.yummyani.me/catalog/item/${slug}`;
      const thread = await channel.threads
        .create({
          name: title.slice(0, 100),
          message: { content: `${title}\n${pageUrl}\nАвто-синк Yummy` }
        })
        .catch(() => null);
      if (!thread) continue;
      totalCreated += 1;
      await saveAnimeTopic(slug, {
        threadId: thread.id,
        title,
        pageUrl,
        yummySlug: slug,
        adders: [binding.userId]
      });
      await upsertPersonalListItem(binding.userId, {
        key: slug,
        title,
        url: pageUrl,
        addedAt: new Date().toISOString()
      });
    }
  }
  if (totalCreated > 0) {
    console.log(`[bg] Yummy sync created topics: ${totalCreated}`);
  }
}

export function startBackgroundLoops(client: Client): void {
  setInterval(() => {
    runYummyBackgroundSync(client).catch((err) => {
      console.error("[bg] yummy sync failed", err);
    });
  }, 10 * 60 * 1000);
}
