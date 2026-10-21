import { MessageId } from "@t3tools/contracts";
import { StrictMode, useLayoutEffect } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import { render, type Screen } from "../../test/render";
import type { TimelineEntriesInput, TimelineEntry } from "../session-logic";
import { makeStreamingTimelineFixture } from "../test-fixtures";
import { useIncrementalTimelineEntries } from "./useIncrementalTimelineEntries";

// A mock caller leaves allocations uncached, reproducing a compiler bailout in ChatView.
const TimelineProbe = vi.fn(
  ({
    threadKey,
    input,
    revision,
    onEntries,
  }: {
    threadKey: string;
    input: TimelineEntriesInput;
    revision: number;
    onEntries: (entries: TimelineEntry[]) => void;
  }) => {
    const result = useIncrementalTimelineEntries(threadKey, {
      ...input,
      anchoredMessages: input.anchoredMessages?.map((message) => ({ ...message })) ?? [],
      attachmentUrlById: new Map(input.attachmentUrlById),
    });
    useLayoutEffect(() => {
      onEntries(result);
    });
    return <output>{revision}</output>;
  },
);

function mountTimeline() {
  let entries: TimelineEntry[] = [];
  let screen: Screen | undefined;
  const onEntries = (next: TimelineEntry[]) => {
    entries = next;
  };
  return {
    get entries() {
      return entries;
    },
    async render(input: TimelineEntriesInput, revision = 0, threadKey = "draft") {
      const ui = (
        <StrictMode>
          <TimelineProbe
            threadKey={threadKey}
            input={input}
            revision={revision}
            onEntries={onEntries}
          />
        </StrictMode>
      );
      if (screen) await screen.rerender(ui);
      else screen = await render(ui);
    },
    [Symbol.asyncDispose]: async () => {
      await screen?.[Symbol.asyncDispose]();
    },
  };
}

describe("incremental timeline rendering", () => {
  it("can rerender an empty draft with fresh arrays and maps", async () => {
    await using timeline = mountTimeline();
    const input = { visibleTurnItems: [], optimisticMessages: [] };
    await timeline.render(input);
    const previous = timeline.entries;
    await timeline.render(input, 1);
    expect(timeline.entries).toBe(previous);
    expect(timeline.entries).toEqual([]);
  });

  it("handles recreated feedback messages and still displays feedback edits", async () => {
    await using timeline = mountTimeline();
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
    await timeline.render(input);
    const previous = timeline.entries;
    await timeline.render(input, 1);
    expect(timeline.entries).toBe(previous);
    await timeline.render(
      {
        ...input,
        anchoredMessages: input.anchoredMessages!.map((message) => ({
          ...message,
          text: "Updated feedback",
        })),
      },
      2,
    );
    expect(timeline.entries[0]).toMatchObject({
      kind: "message",
      message: { text: "Updated feedback" },
    });
  });

  it("preserves history rows during streaming and clears them when switching to a draft", async () => {
    await using timeline = mountTimeline();
    const fixture = makeStreamingTimelineFixture("First");
    const input: TimelineEntriesInput = {
      visibleTurnItems: fixture.visibleTurnItems,
      optimisticMessages: [],
    };
    await timeline.render(input, 0, "server");
    const history = timeline.entries[0];
    await timeline.render(
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
    expect(timeline.entries[0]).toBe(history);
    expect(timeline.entries.at(-1)).toMatchObject({
      kind: "message",
      message: { text: "First, then more" },
    });
    await timeline.render({ visibleTurnItems: [], optimisticMessages: [] }, 2, "draft");
    expect(timeline.entries).toEqual([]);
  });

  it("updates and removes attachment previews when URL map contents change", async () => {
    await using timeline = mountTimeline();
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
    await timeline.render(input);
    expect(timeline.entries.at(-1)).toMatchObject({
      message: { attachments: [{ previewUrl: "https://server.test/first" }] },
    });
    await timeline.render(
      {
        ...input,
        attachmentUrlById: new Map([[attachment.id, "https://server.test/renewed"]]),
      },
      1,
    );
    expect(timeline.entries.at(-1)).toMatchObject({
      message: { attachments: [{ previewUrl: "https://server.test/renewed" }] },
    });
    await timeline.render({ ...input, attachmentUrlById: new Map() }, 2);
    const last = timeline.entries.at(-1);
    if (last?.kind !== "message") throw new Error("Expected assistant message");
    expect(last.message.attachments?.[0]).not.toHaveProperty("previewUrl");
  });
});
