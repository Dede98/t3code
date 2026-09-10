import {
  CommandId,
  ProviderRuntimeEvent,
  RuntimeMode,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export class ProviderTerminalSessionCommandError extends Schema.TaggedError<ProviderTerminalSessionCommandError>()(
  "ProviderTerminalSessionCommandError",
  { reason: Schema.Literals(["native-terminal-identity-missing", "native-terminal-time-invalid"]) },
) {}

const decodeSource = Schema.decodeUnknownEffect(
  Schema.Struct({ event: ProviderRuntimeEvent, runtimeMode: RuntimeMode }),
);

/** Project an actual native terminal, including receipt replay, without mutable session defaults. */
export const makeProviderTerminalSessionCommand = Effect.fn("makeProviderTerminalSessionCommand")(
  function* (event: unknown, runtimeMode: RuntimeMode) {
    const { event: source } = yield* decodeSource({ event, runtimeMode });
    if (
      (source.type !== "turn.completed" && source.type !== "turn.aborted") ||
      source.providerInstanceId === undefined ||
      source.turnId === undefined
    ) {
      return yield* new ProviderTerminalSessionCommandError({
        reason: "native-terminal-identity-missing",
      });
    }
    const terminalAt = DateTime.make(source.createdAt);
    if (Option.isNone(terminalAt) || DateTime.formatIso(terminalAt.value) !== source.createdAt) {
      return yield* new ProviderTerminalSessionCommandError({
        reason: "native-terminal-time-invalid",
      });
    }
    const failed = source.type === "turn.aborted" || source.payload.state === "failed";
    return {
      type: "thread.session.set",
      commandId: CommandId.make(`provider:${source.eventId}:native-terminal-session`),
      threadId: source.threadId,
      session: {
        threadId: source.threadId,
        status: failed ? "error" : "ready",
        providerName: source.provider,
        providerInstanceId: source.providerInstanceId,
        runtimeMode,
        activeTurnId: null,
        lastError:
          source.type === "turn.aborted"
            ? source.payload.reason
            : source.payload.state === "failed"
              ? (source.payload.errorMessage ?? "Turn failed")
              : null,
        updatedAt: source.createdAt,
      },
      providerRuntimeLifecycle:
        source.type === "turn.completed"
          ? {
              runtimeEventId: source.eventId,
              runtimeEventType: source.type,
              providerInstanceId: source.providerInstanceId,
              providerTurnId: source.turnId,
              providerState: source.payload.state,
            }
          : {
              runtimeEventId: source.eventId,
              runtimeEventType: source.type,
              providerInstanceId: source.providerInstanceId,
              providerTurnId: source.turnId,
            },
      createdAt: source.createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.session.set" }>;
  },
);
