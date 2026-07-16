import { DesktopNotificationInputSchema, type DesktopNotificationInput } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { makeComponentLogger } from "../../app/DesktopObservability.ts";
import * as ElectronNotification from "../../electron/ElectronNotification.ts";
import * as DesktopWindow from "../../window/DesktopWindow.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopIpc from "../DesktopIpc.ts";

const { logWarning: logNotificationWarning } = makeComponentLogger("desktop-notification");

const NOTIFICATION_TITLES: Record<DesktopNotificationInput["kind"], string> = {
  approval: "Approval needed",
  input: "Input needed",
  completion: "Chat finished",
  failure: "Chat failed",
};

export function formatDesktopNotification(input: DesktopNotificationInput): {
  readonly title: string;
  readonly subtitle: string;
  readonly body: string;
} {
  return {
    title: NOTIFICATION_TITLES[input.kind],
    subtitle: input.projectTitle,
    body: input.threadTitle,
  };
}

export const showDesktopNotification = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.SHOW_NOTIFICATION_CHANNEL,
  payload: DesktopNotificationInputSchema,
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.notifications.show")(function* (input) {
    const notifications = yield* ElectronNotification.ElectronNotification;
    const desktopWindow = yield* DesktopWindow.DesktopWindow;
    const context = yield* Effect.context<DesktopWindow.DesktopWindow>();
    const runPromise = Effect.runPromiseWith(context);
    const target = {
      environmentId: input.environmentId,
      threadId: input.threadId,
    };

    return yield* notifications.show({
      key: `${input.environmentId}\u0000${input.threadId}`,
      ...formatDesktopNotification(input),
      onClick: () => {
        void runPromise(
          desktopWindow.dispatchNotificationClick(target).pipe(
            Effect.catchCause((cause) =>
              logNotificationWarning("failed to open chat from notification", {
                environmentId: input.environmentId,
                threadId: input.threadId,
                cause,
              }),
            ),
          ),
        );
      },
      onFailed: (error) => {
        void runPromise(
          logNotificationWarning("native notification failed", {
            environmentId: input.environmentId,
            threadId: input.threadId,
            kind: input.kind,
            error,
          }),
        );
      },
    });
  }),
});
