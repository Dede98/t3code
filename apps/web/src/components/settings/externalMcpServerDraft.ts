import type {
  ExternalMcpHeader,
  ExternalMcpServerConfig,
  ProviderInstanceId,
} from "@t3tools/contracts";

const SERVER_ID_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/;
const HEADER_NAME_PATTERN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

export interface HeaderDraft {
  readonly key: number;
  readonly name: string;
  readonly value: string;
  readonly sensitive: boolean;
  /** Name of the secret the server already stores for this header, which keys it. */
  readonly storedSecretName: string | null;
}

export interface ExternalMcpServerDraft {
  readonly id: string;
  readonly displayName: string;
  readonly url: string;
  readonly auth: "headers" | "oauth";
  readonly clientId: string;
  readonly scopes: string;
  readonly headers: ReadonlyArray<HeaderDraft>;
  /** Null attaches the server to every provider instance. */
  readonly providerInstances: ReadonlyArray<ProviderInstanceId> | null;
}

export function toDraft(
  id: string,
  server: ExternalMcpServerConfig | undefined,
): ExternalMcpServerDraft {
  const oauth = server?.auth?.type === "oauth" ? server.auth : undefined;
  return {
    id,
    displayName: server?.displayName ?? "",
    url: server?.url ?? "",
    // Most hosted MCP servers sign in with OAuth.
    auth: oauth || server === undefined ? "oauth" : "headers",
    clientId: oauth?.clientId ?? "",
    scopes: oauth?.scopes?.join(" ") ?? "",
    headers: (server?.headers ?? []).map((header, key) => ({
      key,
      name: header.name,
      value: header.valueRedacted ? "" : header.value,
      sensitive: header.sensitive,
      storedSecretName: header.valueRedacted ? header.name : null,
    })),
    providerInstances: server?.providerInstances ?? null,
  };
}

/** The server keys stored secrets by lowercased name and keeps one only while it stays secret. */
export function keepsStoredSecret(header: HeaderDraft): boolean {
  return (
    header.storedSecretName !== null &&
    header.sensitive &&
    !header.value &&
    header.storedSecretName.toLowerCase() === header.name.trim().toLowerCase()
  );
}

function urlProblem(value: string): string | null {
  if (!value) return "Enter the server's URL.";
  if (!URL.canParse(value)) return "Enter a full URL, such as https://example.com/mcp.";
  const url = new URL(value);
  if (url.username || url.password) return "Put credentials in a secret header, not the URL.";
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol === "https:" || (url.protocol === "http:" && loopback)) return null;
  return "Use HTTPS. Plain HTTP works only for localhost.";
}

const scopeList = (scopes: string) => scopes.split(/[\s,]+/u).filter(Boolean);

/** Mirrors the settings contract, so a save the server would refuse never leaves the dialog. */
export function validate(
  draft: ExternalMcpServerDraft,
  takenIds: ReadonlySet<string>,
  isNew: boolean,
): string | null {
  if (isNew) {
    if (!SERVER_ID_PATTERN.test(draft.id)) {
      return "The ID starts with a letter and uses up to 64 letters, digits, - or _.";
    }
    if (draft.id === "t3-code") return "t3-code is reserved for T3 Code's own server.";
    if (takenIds.has(draft.id)) return `A server named ${draft.id} already exists.`;
  }
  const problem = urlProblem(draft.url.trim());
  if (problem) return problem;
  if (draft.auth === "oauth") {
    if (draft.clientId.trim().length > 512) return "The client ID is too long.";
    if (scopeList(draft.scopes).some((scope) => scope.length > 256)) {
      return "A scope is too long.";
    }
  }
  const names = new Set<string>();
  for (const header of draft.headers) {
    const name = header.name.trim();
    if (!HEADER_NAME_PATTERN.test(name) || name.length > 128) {
      return `"${name || "(empty)"}" is not a header name.`;
    }
    if (names.has(name.toLowerCase())) return `The ${name} header is listed twice.`;
    names.add(name.toLowerCase());
    if (draft.auth === "oauth" && name.toLowerCase() === "authorization") {
      return "With OAuth, sign-in provides the Authorization header. Remove it.";
    }
    if (header.storedSecretName !== null && !keepsStoredSecret(header) && !header.value) {
      return `Enter the value of ${name} again, or remove the header.`;
    }
  }
  return null;
}

export function toConfig(draft: ExternalMcpServerDraft, enabled: boolean): ExternalMcpServerConfig {
  const scopes = scopeList(draft.scopes);
  return {
    url: draft.url.trim(),
    enabled,
    ...(draft.displayName.trim() ? { displayName: draft.displayName.trim() } : {}),
    headers: draft.headers.map((header): ExternalMcpHeader => {
      const name = header.name.trim();
      return keepsStoredSecret(header)
        ? { name, value: "", sensitive: true, valueRedacted: true }
        : { name, value: header.value, sensitive: header.sensitive };
    }),
    ...(draft.providerInstances === null ? {} : { providerInstances: draft.providerInstances }),
    ...(draft.auth === "oauth"
      ? {
          auth: {
            type: "oauth" as const,
            ...(draft.clientId.trim() ? { clientId: draft.clientId.trim() } : {}),
            ...(scopes.length > 0 ? { scopes } : {}),
          },
        }
      : {}),
  };
}

/** Saving these changes ends the sign-in the server holds. */
export function endsSignIn(
  draft: ExternalMcpServerDraft,
  server: ExternalMcpServerConfig | undefined,
): boolean {
  if (server?.auth?.type !== "oauth") return false;
  return (
    draft.auth !== "oauth" ||
    draft.url.trim() !== server.url ||
    (draft.clientId.trim() || undefined) !== server.auth.clientId
  );
}
