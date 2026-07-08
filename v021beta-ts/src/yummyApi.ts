export type YummyProfile = {
  id: number;
  nickname: string;
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

export async function loginYummyByPassword(params: {
  login: string;
  password: string;
  appToken: string;
  userAgent: string;
}): Promise<string> {
  const response = await fetch("https://api.yani.tv/profile/login", {
    method: "POST",
    headers: buildHeaders(params.appToken, params.userAgent),
    body: JSON.stringify({
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

  const token = extractToken(payload);
  if (!token) {
    throw new Error("API вернул успешный ответ, но без токена.");
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
