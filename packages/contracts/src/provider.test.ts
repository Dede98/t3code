import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";
import {
  ProviderSendTurnInput,
  ProviderThreadContinuationSyncError,
  ProviderThreadContinuationSyncResult,
  ProviderUploadFeedbackError,
} from "./provider.ts";

const decodeProviderSendTurnInput = Schema.decodeUnknownSync(ProviderSendTurnInput);
const decodeContinuationSyncResult = Schema.decodeUnknownSync(ProviderThreadContinuationSyncResult);

describe("ProviderSendTurnInput", () => {
  it.each(["image", "file"])(
    "caps total image bytes for %s attachments without charging videos",
    (type) => {
      const image = {
        type,
        id: "image",
        name: "image.png",
        mimeType: "image/png",
        sizeBytes: 10 * 1024 * 1024,
      };
      const video = {
        type: "file",
        id: "video",
        name: "video.mp4",
        mimeType: "video/mp4",
        sizeBytes: 50 * 1024 * 1024,
      };
      expect(
        decodeProviderSendTurnInput({
          threadId: "thread-1",
          attachments: [...Array.from({ length: 8 }, () => image), video],
        }).attachments,
      ).toHaveLength(9);
      expect(() =>
        decodeProviderSendTurnInput({
          threadId: "thread-1",
          attachments: [...Array.from({ length: 8 }, () => image), { ...image, sizeBytes: 1 }],
        }),
      ).toThrow(/80 MiB/);
    },
  );
});

describe("ProviderThreadContinuationSync", () => {
  it("decodes a stable imported result", () => {
    expect(
      decodeContinuationSyncResult({
        threadId: "thread-1",
        providerInstanceId: "claude-work",
        state: "imported",
      }),
    ).toEqual({
      threadId: "thread-1",
      providerInstanceId: "claude-work",
      state: "imported",
    });
  });

  it("exposes a stable error code and message", () => {
    const error = new ProviderThreadContinuationSyncError({
      code: "turn-active",
      detail: "Wait for the active turn to finish.",
    });
    expect(error.code).toBe("turn-active");
    expect(error.message).toBe("Wait for the active turn to finish.");
  });
});

describe("provider feedback", () => {
  it("keeps the failed thread and original cause without exposing upstream text", () => {
    const cause = new Error("provider request secret");
    const error = new ProviderUploadFeedbackError({
      threadId: ThreadId.make("thread-1"),
      cause,
    });

    expect(error.threadId).toBe("thread-1");
    expect(error.cause).toBe(cause);
    expect(error.message).toBe("Failed to upload feedback for thread thread-1.");
    expect(error.message).not.toContain("provider request secret");
  });
});
