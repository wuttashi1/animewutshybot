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
  malBindings: Record<string, { username: string; listUrl: string; updatedAt: string }>;
};

const STATE_PATH = "data/v021beta-state.json";

function defaultState(): State {
  return {
    yummyBindings: {},
    guilds: {},
    animeTopics: {},
    personalLists: {},
    malBindings: {}
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
      malBindings: parsed.malBindings ?? {}
    };
  } catch {
    return defaultState();
  }
}

export async function saveState(state: State): Promise<void> {
  await ensureParent(STATE_PATH);
  await writeFile(STATE_PATH, JSON.stringify(state, null, 2), "utf-8");
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
