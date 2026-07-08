export type MalEntry = {
  id: number;
  title: string;
};

const STATUS_MAP: Record<string, string> = {
  all: "all",
  watching: "watching",
  completed: "completed",
  on_hold: "on_hold",
  dropped: "dropped",
  plan_to_watch: "plan_to_watch"
};

type JikanResponse = {
  data?: Array<{
    node?: {
      id?: number;
      title?: string;
    };
    anime?: {
      mal_id?: number;
      title?: string;
    };
  }>;
  pagination?: {
    has_next_page?: boolean;
    current_page?: number;
  };
};

export async function fetchMalList(username: string, status: string): Promise<MalEntry[]> {
  const norm = STATUS_MAP[status] ?? "all";
  const out: MalEntry[] = [];
  let page = 1;
  let safety = 0;
  while (safety < 10) {
    safety += 1;
    const url = new URL(`https://api.jikan.moe/v4/users/${encodeURIComponent(username)}/animelist`);
    if (norm !== "all") {
      url.searchParams.set("status", norm);
    }
    url.searchParams.set("limit", "100");
    url.searchParams.set("page", String(page));
    const resp = await fetch(url, { method: "GET" });
    if (!resp.ok) {
      throw new Error(`MAL API error (HTTP ${resp.status}).`);
    }
    const data = (await resp.json()) as JikanResponse;
    const rows = data.data ?? [];
    for (const row of rows) {
      const id = Number(row.node?.id ?? row.anime?.mal_id ?? 0);
      const title = String(row.node?.title ?? row.anime?.title ?? "").trim();
      if (id > 0 && title) {
        out.push({ id, title });
      }
    }
    const hasNext = Boolean(data.pagination?.has_next_page);
    if (!hasNext) {
      break;
    }
    page += 1;
  }
  return out;
}
