export type YummyProfile = {
  id: number;
  nickname: string;
};

export type YummyListEntry = {
  anime_id?: number;
  anime_url?: string;
  title?: string;
  user?: {
    list?: {
      list?: {
        href?: string;
      };
    };
  };
};

function buildHeaders(appToken: string, userAgent: string, bearer?: string): HeadersInit {
  const headers: Record<string, string> = {
    "X-Application": appToken,
    "Accept": "application/json",
    "Lang": "en",
    "User-Agent": userAgent,
    "Content-Type": "application/json"
  };
  if (bearer) {
    headers.Authorization = `Bearer ${bearer}`;
  }
  return headers;
}

function extractToken(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }
  const asRec = payload as Record<string, unknown>;
  for (const key of ["token", "access_token", "accessToken", "jwt"]) {
    const value = asRec[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  for (const key of ["response", "data", "result", "user", "profile"]) {
    const nested = extractToken(asRec[key]);
    if (nested) {
      return nested;
    }
  }
  return null;
}

function extractTokenFromHeaders(headers: Headers): string | null {
  const rawAuth = headers.get("authorization") || headers.get("Authorization");
  if (rawAuth) {
    const s = rawAuth.trim();
    if (s.toLowerCase().startsWith("bearer ")) {
      return s.slice(7).trim() || null;
    }
    if (s) return s;
  }
  for (const key of ["x-token", "x-access-token", "set-authorization"]) {
    const v = headers.get(key);
    if (v && v.trim()) return v.trim();
  }
  return null;
}

export async function loginYummyByPassword(params: {
  login: string;
  password: string;
  appToken: string;
  userAgent: string;
}): Promise<string> {
  const tried: string[] = [];
  const response = await fetch("https://api.yani.tv/profile/login", {
    method: "POST",
    headers: buildHeaders(params.appToken, params.userAgent),
    body: JSON.stringify({
      need_json: true,
      login: params.login,
      password: params.password
    })
  });

  const rawBody = await response.text();
  let payload: unknown = null;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    payload = null;
  }

  if (response.status === 401 || response.status === 403 || response.status === 422) {
    throw new Error("Неверный логин или пароль.");
  }
  if (!response.ok) {
    throw new Error(`API YummyAnime login failed (HTTP ${response.status}).`);
  }

  const token =
    extractToken(payload) ||
    extractTokenFromHeaders(response.headers);
  if (!token) {
    const setCookie = response.headers.get("set-cookie");
    const cookieHeader = setCookie ? setCookie.split(";")[0] : "";
    if (cookieHeader) {
      // Fallback path: some backends return session cookie, token is resolved by secondary API call.
      for (const endpoint of ["https://api.yani.tv/users/token", "https://api.yani.tv/profile/token"]) {
        tried.push(endpoint);
        const fallbackResp = await fetch(endpoint, {
          method: "GET",
          headers: {
            ...buildHeaders(params.appToken, params.userAgent),
            Cookie: cookieHeader
          }
        });
        const fallbackText = await fallbackResp.text();
        let fallbackPayload: unknown = null;
        try {
          fallbackPayload = JSON.parse(fallbackText);
        } catch {
          fallbackPayload = null;
        }
        const fallbackToken =
          extractToken(fallbackPayload) ||
          extractTokenFromHeaders(fallbackResp.headers);
        if (fallbackToken) {
          return fallbackToken;
        }
      }
    }
    const triedText = tried.length ? ` (fallback: ${tried.join(", ")})` : "";
    throw new Error(`API вернул успешный ответ, но без токена${triedText}.`);
  }
  return token;
}

export async function getYummyProfile(params: {
  accessToken: string;
  appToken: string;
  userAgent: string;
}): Promise<YummyProfile> {
  const response = await fetch("https://api.yani.tv/profile", {
    method: "GET",
    headers: buildHeaders(params.appToken, params.userAgent, params.accessToken)
  });
  const data = (await response.json()) as { response?: { id?: number; nickname?: string } };
  const profile = data.response;
  if (!response.ok || !profile?.id) {
    throw new Error(`Профиль YummyAnime не получен (HTTP ${response.status}).`);
  }
  return {
    id: Number(profile.id),
    nickname: String(profile.nickname ?? "")
  };
}

export async function getYummyUserLists(params: {
  accessToken: string;
  yummyUserId: number;
  appToken: string;
  userAgent: string;
}): Promise<YummyListEntry[]> {
  const response = await fetch(`https://api.yani.tv/users/${params.yummyUserId}/lists`, {
    method: "GET",
    headers: buildHeaders(params.appToken, params.userAgent, params.accessToken)
  });
  const data = (await response.json()) as { response?: unknown };
  if (!response.ok) {
    throw new Error(`Не удалось получить списки YummyAnime (HTTP ${response.status}).`);
  }
  const items = data.response;
  if (!Array.isArray(items)) {
    return [];
  }
  return items as YummyListEntry[];
}

export function filterYummyEntries(entries: YummyListEntry[], mode: string): YummyListEntry[] {
  if (mode === "all") {
    return entries;
  }
  const map: Record<string, string> = {
    watching: "watch_now",
    plan_to_watch: "will",
    completed: "watched",
    on_hold: "postpone",
    dropped: "lost"
  };
  const want = map[mode];
  if (!want) {
    return entries;
  }
  return entries.filter((entry) => entry.user?.list?.list?.href === want);
}
