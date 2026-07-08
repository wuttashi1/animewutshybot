const BASE = "https://en.yummyani.me";

type SearchResponse = {
  response?: Array<{
    anime_url?: string;
    title?: string;
  }>;
};

type AnimeResponse = {
  response?: {
    title?: string;
    anime_url?: string;
  };
};

export function slugFromText(input: string): string | null {
  const text = input.trim();
  const m = text.match(/(?:https?:\/\/)?(?:en\.)?yummyani\.me\/catalog\/item\/([^/?#\s]+)/i);
  if (m?.[1]) {
    return m[1].trim();
  }
  return null;
}

export async function searchYummySlug(query: string): Promise<string | null> {
  const url = new URL(`${BASE}/api/search`);
  url.searchParams.set("q", query.trim());
  const resp = await fetch(url, { method: "GET" });
  if (!resp.ok) {
    return null;
  }
  const data = (await resp.json()) as SearchResponse;
  const first = data.response?.[0];
  return first?.anime_url?.trim() ?? null;
}

export async function fetchAnimeCard(slug: string): Promise<{ title: string; slug: string; pageUrl: string } | null> {
  const resp = await fetch(`${BASE}/api/anime/${encodeURIComponent(slug)}`, { method: "GET" });
  if (!resp.ok) {
    return null;
  }
  const data = (await resp.json()) as AnimeResponse;
  const card = data.response;
  const title = card?.title?.trim();
  if (!title) {
    return null;
  }
  const key = (card?.anime_url?.trim() || slug).trim();
  return {
    title,
    slug: key,
    pageUrl: `${BASE}/catalog/item/${key}`
  };
}
