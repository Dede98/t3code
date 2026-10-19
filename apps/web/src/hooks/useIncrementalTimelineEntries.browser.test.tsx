import { MessageId } from "@t3tools/contracts";
import { act, StrictMode, useLayoutEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import type { TimelineEntriesInput, TimelineEntry } from "../session-logic";
import { makeStreamingTimelineFixture } from "../test-fixtures";
import { useIncrementalTimelineEntries } from "./useIncrementalTimelineEntries";

let root: Root;
let entries: TimelineEntry[];

// A mock caller leaves allocations uncached, reproducing a compiler bailout in ChatView.
const TimelineProbe = vi.fn(
  ({
    threadKey,
    input,
    revision,
  }: {
    threadKey: string;
    input: TimelineEntriesInput;
    revision: number;
  }) => {
    const result = useIncrementalTimelineEntries(threadKey, {
      ...input,
      anchoredMessages: input.anchoredMessages?.map((message) => ({ ...message })) ?? [],
      attachmentUrlById: new Map(input.attachmentUrlById),
    });
    useLayoutEffect(() => {
      entries = result;
    });
    return <output>{revision}</output>;
  },
);

async function render(input: TimelineEntriesInput, revision = 0, threadKey = "draft") {
  await act(() =>
    root.render(
      <StrictMode>
        <TimelineProbe threadKey={threadKey} input={input} revision={revision} />
      </StrictMode>,
    ),
  );
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root = createRoot(document.createElement("div"));
});

afterEach(async () => {
  await act(() => root.unmount());
  vi.unstubAllGlobals();
});

describe("incremental timeline rendering", () => {
  it("can rerender an empty draft with fresh arrays and maps", async () => {
    const input = { visibleTurnItems: [], optimisticMessages: [] };
    await render(input);
    const previous = entries;
    await render(input, 1);
    expect(entries).toBe(previous);
    expect(entries).toEqual([]);
  });

  it("handles recreated feedback messages and still displays feedback edits", async () => {
    const input: TimelineEntriesInput = {
      visibleTurnItems: [],
      optimisticMessages: [],
      anchoredMessages: [
        {
          id: MessageId.make("feedback"),
          role: "user",
          text: "Original feedback",
          runId: null,
          streaming: false,
          createdAt: "2026-10-19T00:00:00.000Z",
          updatedAt: "2026-10-19T00:00:00.000Z",
        },
      ],
    };
    await render(input);
    const previous = entries;
    await render(input, 1);
    expect(entries).toBe(previous);
    await render(
      {
        ...input,
        anchoredMessages: input.anchoredMessages!.map((message) => ({
          ...message,
          text: "Updated feedback",
        })),
      },
      2,
    );
    expect(entries[0]).toMatchObject({ kind: "message", message: { text: "Updated feedback" } });
  });

  it("preserves history rows during streaming and clears them when switching to a draft", async () => {
    const fixture = makeStreamingTimelineFixture("First");
    const input: TimelineEntriesInput = {
      visibleTurnItems: fixture.visibleTurnItems,
      optimisticMessages: [],
    };
    await render(input, 0, "server");
    const history = entries[0];
    await render(
      {
        ...input,
        visibleTurnItems: fixture.visibleTurnItems.map((row) =>
          row.item.type === "assistant_message" && row.item.streaming
            ? { ...row, item: { ...row.item, text: "First, then more" } }
            : row,
        ),
      },
      1,
      "server",
    );
    expect(entries[0]).toBe(history);
    expect(entries.at(-1)).toMatchObject({
      kind: "message",
      message: { text: "First, then more" },
    });
    await render({ visibleTurnItems: [], optimisticMessages: [] }, 2, "draft");
    expect(entries).toEqual([]);
  });

  it("updates and removes attachment previews when URL map contents change", async () => {
    const fixture = makeStreamingTimelineFixture("Image");
    const attachment = {
      type: "image" as const,
      id: "image",
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 42,
    };
    const input: TimelineEntriesInput = {
      visibleTurnItems: fixture.visibleTurnItems.map((row) =>
        row.item.type === "assistant_message"
          ? { ...row, item: { ...row.item, attachments: [attachment] } }
          : row,
      ),
      optimisticMessages: [],
      attachmentUrlById: new Map([[attachment.id, "https://server.test/first"]]),
    };
    await render(input);
    expect(entries.at(-1)).toMatchObject({
      message: { attachments: [{ previewUrl: "https://server.test/first" }] },
    });
    await render(
      {
        ...input,
        attachmentUrlById: new Map([[attachment.id, "https://server.test/renewed"]]),
      },
      1,
    );
    expect(entries.at(-1)).toMatchObject({
      message: { attachments: [{ previewUrl: "https://server.test/renewed" }] },
    });
    await render({ ...input, attachmentUrlById: new Map() }, 2);
    const last = entries.at(-1);
    if (last?.kind !== "message") throw new Error("Expected assistant message");
    expect(last.message.attachments?.[0]).not.toHaveProperty("previewUrl");
  });
});
