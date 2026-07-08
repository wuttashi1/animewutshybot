import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

export type YummyBinding = {
  userId: string;
  yummyUserId: number;
  nickname: string;
  accessToken: string;
  updatedAt: string;
};

export type GuildConfig = {
  categoryId?: string;
  forumChannelId?: string;
  listForumChannelId?: string;
  infoThreadId?: string;
  roasterEnabled: boolean;
  setupAt?: string;
};

export type AnimeTopic = {
  threadId: string;
  title: string;
  pageUrl: string;
  yummySlug: string;
  adders: string[];
};

export type PersonalListItem = {
  key: string;
  title: string;
  url: string;
  addedAt: string;
};

type State = {
  yummyBindings: Record<string, YummyBinding>;
  guilds: Record<string, GuildConfig>;
  animeTopics: Record<string, AnimeTopic>;
  personalLists: Record<string, PersonalListItem[]>;
  personalThreads: Record<string, string>;
  malBindings: Record<string, { username: string; listUrl: string; updatedAt: string }>;
  ratings: Record<string, Record<string, number>>;
};

const STATE_PATH = "data/v021beta-state.json";

function defaultState(): State {
  return {
    yummyBindings: {},
    guilds: {},
    animeTopics: {},
    personalLists: {},
    personalThreads: {},
    malBindings: {},
    ratings: {}
  };
}

async function ensureParent(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
}

export async function loadState(): Promise<State> {
  try {
    const raw = await readFile(STATE_PATH, "utf-8");
    const parsed = JSON.parse(raw) as Partial<State>;
    const base = defaultState();
    return {
      ...base,
      ...parsed,
      yummyBindings: parsed.yummyBindings ?? {},
      guilds: parsed.guilds ?? {},
      animeTopics: parsed.animeTopics ?? {},
      personalLists: parsed.personalLists ?? {},
      personalThreads: parsed.personalThreads ?? {},
      malBindings: parsed.malBindings ?? {},
      ratings: parsed.ratings ?? {}
    };
  } catch {
    return defaultState();
  }
}

export async function saveState(state: State): Promise<void> {
  await ensureParent(STATE_PATH);
  const legacyCompatible = {
    ...state,
    yummy_accounts: Object.fromEntries(
      Object.entries(state.yummyBindings).map(([uid, b]) => [
        uid,
        { token: b.accessToken, user_id: b.yummyUserId, nickname: b.nickname, updated_at: b.updatedAt }
      ])
    ),
    mal_accounts: state.malBindings,
    anime_topics: Object.fromEntries(
      Object.entries(state.animeTopics).map(([k, v]) => [
        k,
        {
          thread_id: Number(v.threadId),
          page_url: v.pageUrl,
          yummy_slug: v.yummySlug,
          adders: v.adders.map((x) => Number(x) || x),
          title: v.title
        }
      ])
    ),
    personal_lists: Object.fromEntries(
      Object.entries(state.personalLists).map(([uid, list]) => [
        uid,
        {
          order: list.map((x) => x.key),
          recent_keys: list.map((x) => x.key)
        }
      ])
    )
  };
  await writeFile(STATE_PATH, JSON.stringify(legacyCompatible, null, 2), "utf-8");
}

export async function saveYummyBinding(binding: YummyBinding): Promise<void> {
  const state = await loadState();
  state.yummyBindings[binding.userId] = binding;
  await saveState(state);
}

export async function getYummyBinding(userId: string): Promise<YummyBinding | null> {
  const state = await loadState();
  return state.yummyBindings[userId] ?? null;
}

export async function deleteYummyBinding(userId: string): Promise<void> {
  const state = await loadState();
  delete state.yummyBindings[userId];
  await saveState(state);
}

export async function saveGuildConfig(guildId: string, cfg: GuildConfig): Promise<void> {
  const state = await loadState();
  state.guilds[guildId] = cfg;
  await saveState(state);
}

export async function getGuildConfig(guildId: string): Promise<GuildConfig | null> {
  const state = await loadState();
  return state.guilds[guildId] ?? null;
}

export async function saveAnimeTopic(slug: string, topic: AnimeTopic): Promise<void> {
  const state = await loadState();
  state.animeTopics[slug] = topic;
  await saveState(state);
}

export async function getAnimeTopic(slug: string): Promise<AnimeTopic | null> {
  const state = await loadState();
  return state.animeTopics[slug] ?? null;
}

export async function upsertPersonalListItem(userId: string, item: PersonalListItem): Promise<void> {
  const state = await loadState();
  const current = state.personalLists[userId] ?? [];
  const next = current.filter((x) => x.key !== item.key);
  next.unshift(item);
  state.personalLists[userId] = next.slice(0, 300);
  await saveState(state);
}

export async function getPersonalList(userId: string): Promise<PersonalListItem[]> {
  const state = await loadState();
  return state.personalLists[userId] ?? [];
}

export async function saveMalBinding(
  userId: string,
  payload: { username: string; listUrl: string }
): Promise<void> {
  const state = await loadState();
  state.malBindings[userId] = {
    username: payload.username,
    listUrl: payload.listUrl,
    updatedAt: new Date().toISOString()
  };
  await saveState(state);
}

function personalThreadKey(guildId: string, userId: string): string {
  return `${guildId}:${userId}`;
}

export async function getPersonalThreadId(guildId: string, userId: string): Promise<string | null> {
  const state = await loadState();
  return state.personalThreads[personalThreadKey(guildId, userId)] ?? null;
}

export async function savePersonalThreadId(
  guildId: string,
  userId: string,
  threadId: string
): Promise<void> {
  const state = await loadState();
  state.personalThreads[personalThreadKey(guildId, userId)] = threadId;
  await saveState(state);
}

export async function getMalBinding(
  userId: string
): Promise<{ username: string; listUrl: string; updatedAt: string } | null> {
  const state = await loadState();
  return state.malBindings[userId] ?? null;
}

export async function rateAnimeTopic(threadId: string, userId: string, score: number): Promise<void> {
  const state = await loadState();
  const byUser = state.ratings[threadId] ?? {};
  byUser[userId] = score;
  state.ratings[threadId] = byUser;
  await saveState(state);
}

export async function getAnimeTopicRatings(
  threadId: string
): Promise<{ count: number; average: number | null; byUser: Record<string, number> }> {
  const state = await loadState();
  const byUser = state.ratings[threadId] ?? {};
  const values = Object.values(byUser);
  if (!values.length) {
    return { count: 0, average: null, byUser };
  }
  const sum = values.reduce((acc, x) => acc + x, 0);
  return { count: values.length, average: sum / values.length, byUser };
}

export async function syncPersonalListFromTopics(userId: string): Promise<number> {
  const state = await loadState();
  const items = Object.entries(state.animeTopics).map(([slug, topic]) => ({
    key: slug,
    title: topic.title,
    url: topic.pageUrl,
    addedAt: new Date().toISOString()
  }));
  state.personalLists[userId] = items.slice(0, 300);
  await saveState(state);
  return state.personalLists[userId].length;
}
