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
    description?: string;
    year?: number | string;
    rating?: {
      average?: number | string;
    };
    genres?: Array<{ title?: string }>;
    anime_status?: { title?: string };
    type?: { name?: string };
    episodes?: {
      aired?: number | string;
      all?: number | string;
    };
    poster?: {
      fullsize?: string;
      big?: string;
      huge?: string;
    };
  };
};

export type AnimeCard = {
  title: string;
  slug: string;
  pageUrl: string;
  description: string;
  posterUrl: string | null;
  genres: string[];
  year: string;
  ratingAvg: string;
  statusTitle: string;
  typeName: string;
  episodesLabel: string;
};

function absMedia(url: string | undefined): string | null {
  const value = (url || "").trim();
  if (!value) return null;
  if (value.startsWith("//")) return `https:${value}`;
  if (value.startsWith("http://") || value.startsWith("https://")) return value;
  return `https://en.yummyani.me${value.startsWith("/") ? "" : "/"}${value}`;
}

function toLabel(raw: unknown): string {
  if (raw === null || raw === undefined) return "—";
  const s = String(raw).trim();
  return s || "—";
}

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

export async function fetchAnimeCard(slug: string): Promise<AnimeCard | null> {
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
  const genres = (card?.genres || [])
    .map((g) => (g?.title || "").trim())
    .filter((x) => Boolean(x))
    .slice(0, 6);
  const avg =
    card?.rating?.average === undefined || card?.rating?.average === null
      ? "—"
      : Number(card.rating.average).toFixed(2);
  const aired = toLabel(card?.episodes?.aired);
  const all = toLabel(card?.episodes?.all);
  const episodesLabel = aired === "—" && all === "—" ? "—" : `${aired}/${all}`;
  return {
    title,
    slug: key,
    pageUrl: `${BASE}/catalog/item/${key}`,
    description: (card?.description || "").trim(),
    posterUrl: absMedia(card?.poster?.fullsize || card?.poster?.big || card?.poster?.huge),
    genres,
    year: toLabel(card?.year),
    ratingAvg: avg,
    statusTitle: toLabel(card?.anime_status?.title),
    typeName: toLabel(card?.type?.name),
    episodesLabel
  };
}
