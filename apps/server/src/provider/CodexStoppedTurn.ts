import { ProviderDriverKind } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ProviderStoppedTurnInput, ProviderStoppedTurn } from "./Services/ProviderAdapter.ts";

const decodeCursor = Schema.decodeUnknownOption(Schema.Struct({ threadId: Schema.NonEmptyString }));
const decodeSnapshot = Schema.decodeUnknownOption(
  Schema.Struct({
    thread: Schema.Struct({
      id: Schema.String,
      cwd: Schema.String,
      turns: Schema.Array(
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          startedAt: Schema.NullOr(Schema.Number),
          completedAt: Schema.NullOr(Schema.Number),
        }),
      ),
    }),
  }),
);

export const codexStoppedTurnThreadId = (cursor: unknown) =>
  Option.map(decodeCursor(cursor), (value) => value.threadId);

/** Missing, running and successful turns are never reclassified as interrupted. */
export const readCodexStoppedTurnEvidence = (
  input: ProviderStoppedTurnInput,
  snapshot: unknown,
): ProviderStoppedTurn | undefined => {
  const cursor = codexStoppedTurnThreadId(input.resumeCursor);
  const decoded = decodeSnapshot(snapshot);
  if (Option.isNone(cursor) || Option.isNone(decoded)) return;
  const thread = decoded.value.thread;
  if (thread.id !== cursor.value || thread.cwd !== input.cwd) return;
  const matches = thread.turns.filter((turn) => turn.id === input.providerTurnId);
  if (matches.length !== 1) return;
  const turn = matches[0]!;
  if (
    (turn.status !== "interrupted" && turn.status !== "failed") ||
    turn.startedAt === null ||
    turn.completedAt === null ||
    !Number.isSafeInteger(turn.startedAt) ||
    !Number.isSafeInteger(turn.completedAt) ||
    turn.startedAt < 0 ||
    turn.completedAt < turn.startedAt
  )
    return;
  const at = DateTime.make(turn.completedAt * 1000);
  if (Option.isNone(at)) return;
  return {
    provider: ProviderDriverKind.make("codex"),
    providerTurnId: input.providerTurnId,
    state: turn.status,
    terminalAt: DateTime.formatIso(at.value),
  };
};
