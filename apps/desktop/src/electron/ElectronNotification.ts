import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Electron from "electron";

export interface ElectronNotificationShowInput {
  readonly key: string;
  readonly title: string;
  readonly subtitle?: string;
  readonly body: string;
  readonly onClick: () => void;
  readonly onFailed: (error: string) => void;
}

export class ElectronNotificationShowError extends Schema.TaggedErrorClass<ElectronNotificationShowError>()(
  "ElectronNotificationShowError",
  {
    key: Schema.String,
    titleLength: Schema.Number,
    subtitleLength: Schema.NullOr(Schema.Number),
    bodyLength: Schema.Number,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to show Electron notification ${JSON.stringify(this.key)}.`;
  }
}

export class ElectronNotification extends Context.Service<
  ElectronNotification,
  {
    readonly show: (
      input: ElectronNotificationShowInput,
    ) => Effect.Effect<boolean, ElectronNotificationShowError>;
  }
>()("@t3tools/desktop/electron/ElectronNotification") {}

export const make = Effect.sync(() => {
  const activeByKey = new Map<string, Electron.Notification>();

  const show: ElectronNotification["Service"]["show"] = (input) =>
    Effect.try({
      try: () => {
        if (!Electron.Notification.isSupported()) {
          return false;
        }

        activeByKey.get(input.key)?.close();

        const notification = new Electron.Notification({
          title: input.title,
          ...(input.subtitle === undefined ? {} : { subtitle: input.subtitle }),
          body: input.body,
        });
        const removeIfCurrent = () => {
          if (activeByKey.get(input.key) === notification) {
            activeByKey.delete(input.key);
          }
        };

        notification.once("click", () => {
          removeIfCurrent();
          input.onClick();
        });
        notification.once("close", removeIfCurrent);
        notification.once("failed", (_event, error) => {
          removeIfCurrent();
          input.onFailed(error);
        });

        activeByKey.set(input.key, notification);
        notification.show();
        return true;
      },
      catch: (cause) =>
        new ElectronNotificationShowError({
          key: input.key,
          titleLength: input.title.length,
          subtitleLength: input.subtitle?.length ?? null,
          bodyLength: input.body.length,
          cause,
        }),
    });

  return ElectronNotification.of({ show });
});

export const layer = Layer.effect(ElectronNotification, make);
