import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

export type YummyBinding = {
  userId: string;
  yummyUserId: number;
  nickname: string;
  accessToken: string;
  updatedAt: string;
};

type State = {
  yummyBindings: Record<string, YummyBinding>;
};

const STATE_PATH = "data/v021beta-state.json";

async function ensureParent(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
}

async function loadState(): Promise<State> {
  try {
    const raw = await readFile(STATE_PATH, "utf-8");
    const parsed = JSON.parse(raw) as Partial<State>;
    return {
      yummyBindings: parsed.yummyBindings ?? {}
    };
  } catch {
    return { yummyBindings: {} };
  }
}

async function saveState(state: State): Promise<void> {
  await ensureParent(STATE_PATH);
  await writeFile(STATE_PATH, JSON.stringify(state, null, 2), "utf-8");
}

export async function saveYummyBinding(binding: YummyBinding): Promise<void> {
  const state = await loadState();
  state.yummyBindings[binding.userId] = binding;
  await saveState(state);
}
