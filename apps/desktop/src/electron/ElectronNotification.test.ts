import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { beforeEach, vi } from "vite-plus/test";

const { isSupportedMock, notificationInstances, notificationMock } = vi.hoisted(() => {
  const instances: Array<{
    readonly options: unknown;
    readonly listeners: Map<string, (...args: Array<unknown>) => void>;
    readonly close: ReturnType<typeof vi.fn>;
    readonly show: ReturnType<typeof vi.fn>;
  }> = [];
  return {
    isSupportedMock: vi.fn(),
    notificationInstances: instances,
    notificationMock: vi.fn(function NotificationMock(options: unknown) {
      const listeners = new Map<string, (...args: Array<unknown>) => void>();
      const instance = {
        options,
        listeners,
        close: vi.fn(),
        show: vi.fn(),
        once: vi.fn((event: string, listener: (...args: Array<unknown>) => void) => {
          listeners.set(event, listener);
        }),
      };
      instances.push(instance);
      return instance;
    }),
  };
});

vi.mock("electron", () => ({
  Notification: Object.assign(notificationMock, { isSupported: isSupportedMock }),
}));

import * as ElectronNotification from "./ElectronNotification.ts";

describe("ElectronNotification", () => {
  beforeEach(() => {
    isSupportedMock.mockReset();
    notificationMock.mockClear();
    notificationInstances.length = 0;
  });

  it.effect("returns false without constructing a notification when unsupported", () =>
    Effect.gen(function* () {
      isSupportedMock.mockReturnValue(false);
      const notifications = yield* ElectronNotification.ElectronNotification;

      const shown = yield* notifications.show({
        key: "environment/thread",
        title: "Chat finished",
        body: "Refactor auth flow",
        onClick: vi.fn(),
        onFailed: vi.fn(),
      });

      assert.isFalse(shown);
      assert.equal(notificationMock.mock.calls.length, 0);
    }).pipe(Effect.provide(ElectronNotification.layer)),
  );

  it.effect("shows a native notification and forwards clicks", () =>
    Effect.gen(function* () {
      isSupportedMock.mockReturnValue(true);
      const onClick = vi.fn();
      const notifications = yield* ElectronNotification.ElectronNotification;

      const shown = yield* notifications.show({
        key: "environment/thread",
        title: "Input needed",
        subtitle: "T3 Code",
        body: "Notification support",
        onClick,
        onFailed: vi.fn(),
      });

      assert.isTrue(shown);
      assert.deepEqual(notificationInstances[0]?.options, {
        title: "Input needed",
        subtitle: "T3 Code",
        body: "Notification support",
      });
      assert.equal(notificationInstances[0]?.show.mock.calls.length, 1);

      notificationInstances[0]?.listeners.get("click")?.();
      assert.equal(onClick.mock.calls.length, 1);
    }).pipe(Effect.provide(ElectronNotification.layer)),
  );

  it.effect("replaces an older notification for the same thread", () =>
    Effect.gen(function* () {
      isSupportedMock.mockReturnValue(true);
      const notifications = yield* ElectronNotification.ElectronNotification;
      const base = {
        key: "environment/thread",
        subtitle: "T3 Code",
        body: "Notification support",
        onClick: vi.fn(),
        onFailed: vi.fn(),
      };

      yield* notifications.show({ ...base, title: "Input needed" });
      yield* notifications.show({ ...base, title: "Chat finished" });

      assert.equal(notificationInstances.length, 2);
      assert.equal(notificationInstances[0]?.close.mock.calls.length, 1);
      assert.equal(notificationInstances[1]?.show.mock.calls.length, 1);
    }).pipe(Effect.provide(ElectronNotification.layer)),
  );

  it.effect("forwards asynchronous native failures", () =>
    Effect.gen(function* () {
      isSupportedMock.mockReturnValue(true);
      const onFailed = vi.fn();
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show({
        key: "environment/thread",
        title: "Chat finished",
        body: "Notification support",
        onClick: vi.fn(),
        onFailed,
      });
      notificationInstances[0]?.listeners.get("failed")?.({}, "The app is not signed");

      assert.deepEqual(onFailed.mock.calls, [["The app is not signed"]]);
    }).pipe(Effect.provide(ElectronNotification.layer)),
  );
});
