import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";

import { formatDesktopNotification } from "./notifications.ts";

describe("desktop notification IPC", () => {
  it("formats a completion without exposing implementation details", () => {
    assert.deepEqual(
      formatDesktopNotification({
        environmentId: EnvironmentId.make("primary"),
        threadId: ThreadId.make("thread-1"),
        kind: "completion",
        projectTitle: "T3 Code",
        threadTitle: "Add desktop notifications",
      }),
      {
        title: "Chat finished",
        subtitle: "T3 Code",
        body: "Add desktop notifications",
      },
    );
  });

  it("uses distinct attention titles", () => {
    const input = {
      environmentId: EnvironmentId.make("primary"),
      threadId: ThreadId.make("thread-1"),
      projectTitle: "T3 Code",
      threadTitle: "Add desktop notifications",
    } as const;

    assert.equal(
      formatDesktopNotification({ ...input, kind: "approval" }).title,
      "Approval needed",
    );
    assert.equal(formatDesktopNotification({ ...input, kind: "input" }).title, "Input needed");
    assert.equal(formatDesktopNotification({ ...input, kind: "failure" }).title, "Chat failed");
  });
});
