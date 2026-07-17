import type { SessionStore, SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

export const CLAUDE_SESSION_STORE_CONTINUATION_KEY = "claude:session-store:t3-local:v1";

/**
 * Persistent implementation of the Claude Agent SDK's transcript mirror.
 *
 * The service deliberately exposes the SDK's Promise-based contract directly
 * so adapters can pass it to `query()` without another bridge layer.
 */
export interface ClaudeSessionSubkeySnapshot {
  readonly subpath: string;
  readonly entries: ReadonlyArray<SessionStoreEntry>;
}

export interface ClaudeSessionSnapshot {
  readonly projectKey: string;
  readonly sessionId: string;
  readonly entries: ReadonlyArray<SessionStoreEntry>;
  readonly subkeys: ReadonlyArray<ClaudeSessionSubkeySnapshot>;
}

export interface ClaudeSessionStoreShape extends SessionStore {
  /** Load the main transcript and every subkey from one SQLite snapshot. */
  readonly loadSession: (key: {
    readonly projectKey: string;
    readonly sessionId: string;
  }) => Promise<ClaudeSessionSnapshot | null>;

  /** Atomically replace the main transcript and every subkey for a session. */
  readonly replaceSession: (snapshot: ClaudeSessionSnapshot) => Promise<void>;
}

export class ClaudeSessionStoreError extends Schema.TaggedErrorClass<ClaudeSessionStoreError>()(
  "ClaudeSessionStoreError",
  {
    operation: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Claude session store failed in ${this.operation}: ${this.detail}`;
  }
}

export class ClaudeSessionStore extends Context.Service<
  ClaudeSessionStore,
  ClaudeSessionStoreShape
>()("t3/provider/Services/ClaudeSessionStore") {}
