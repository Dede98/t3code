import { TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  createDesktopNotificationObservation,
  desktopNotificationKindForTransition,
  type DesktopNotificationObservation,
} from "./DesktopNotificationCoordinator.logic";

const observation = (
  phase: DesktopNotificationObservation["phase"],
  activityIdentity = "turn-1",
): DesktopNotificationObservation => ({ phase, activityIdentity });

describe("desktop notification transitions", () => {
  it("uses the first observation only as a baseline", () => {
    expect(desktopNotificationKindForTransition(undefined, observation("completed"))).toBeNull();
    expect(
      desktopNotificationKindForTransition(undefined, observation("waiting_for_input")),
    ).toBeNull();
  });

  it("notifies for approval and input transitions", () => {
    expect(
      desktopNotificationKindForTransition(
        observation("running"),
        observation("waiting_for_approval"),
      ),
    ).toBe("approval");
    expect(
      desktopNotificationKindForTransition(
        observation("running"),
        observation("waiting_for_input"),
      ),
    ).toBe("input");
  });

  it("does not repeat an unresolved attention notification", () => {
    expect(
      desktopNotificationKindForTransition(
        observation("waiting_for_input"),
        observation("waiting_for_input", "turn-2"),
      ),
    ).toBeNull();
  });

  it("notifies when a run completes or fails", () => {
    expect(
      desktopNotificationKindForTransition(observation("running"), observation("completed")),
    ).toBe("completion");
    expect(
      desktopNotificationKindForTransition(observation("running"), observation("failed")),
    ).toBe("failure");
  });

  it("recognizes a new completion even when React misses the intermediate running phase", () => {
    expect(
      desktopNotificationKindForTransition(
        observation("completed", "turn-1"),
        observation("completed", "turn-2"),
      ),
    ).toBe("completion");
    expect(
      desktopNotificationKindForTransition(
        observation("completed", "turn-2"),
        observation("completed", "turn-2"),
      ),
    ).toBeNull();
  });

  it("uses the latest user message as the stable activity identity", () => {
    expect(
      createDesktopNotificationObservation(
        {
          latestTurn: {
            turnId: TurnId.make("turn-2"),
            state: "completed",
            requestedAt: "2026-07-15T11:59:00.000Z",
            startedAt: null,
            completedAt: "2026-07-15T12:00:00.000Z",
            assistantMessageId: null,
          },
          latestUserMessageAt: "2026-07-15T11:59:00.000Z",
        },
        "completed",
      ),
    ).toEqual({
      phase: "completed",
      activityIdentity: "2026-07-15T11:59:00.000Z",
    });
  });

  it("does not repeat completion when final turn metadata arrives after the phase change", () => {
    const latestUserMessageAt = "2026-07-15T11:59:00.000Z";
    const projectedCompletion = createDesktopNotificationObservation(
      { latestTurn: null, latestUserMessageAt },
      "completed",
    );
    const settledCompletion = createDesktopNotificationObservation(
      {
        latestTurn: {
          turnId: TurnId.make("turn-2"),
          state: "completed",
          requestedAt: latestUserMessageAt,
          startedAt: null,
          completedAt: "2026-07-15T12:00:00.000Z",
          assistantMessageId: null,
        },
        latestUserMessageAt,
      },
      "completed",
    );

    expect(
      desktopNotificationKindForTransition(
        { ...projectedCompletion, phase: "running" },
        projectedCompletion,
      ),
    ).toBe("completion");
    expect(desktopNotificationKindForTransition(projectedCompletion, settledCompletion)).toBeNull();
  });
});
