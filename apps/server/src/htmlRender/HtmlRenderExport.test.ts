import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  RunId,
  MessageId,
  type OrchestrationV2ConversationMessage,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { htmlRenderTheme, readHtmlRenderTheme } from "@t3tools/shared/htmlRender";
import {
  T3_CODE_DARK_THEME_COLORS,
  T3_CODE_LIGHT_THEME_COLORS,
} from "@t3tools/shared/themePalettes";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ServerConfig from "../config.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as HtmlRender from "./HtmlRender.ts";

const threadId = ThreadId.make("theme-export");
const activeRunId = RunId.make("active");
const queuedRunId = RunId.make("queued");
const theme = htmlRenderTheme(
  { ...T3_CODE_DARK_THEME_COLORS, canvas: "#213925", accent: "#c8ff85" },
  "dark",
);
const otherTheme = htmlRenderTheme(T3_CODE_LIGHT_THEME_COLORS, "light");
const layerTest = (captured = true) =>
  HtmlRender.layer.pipe(
    Layer.provide(McpProviderSessions.layer),
    Layer.provide(
      Layer.succeed(
        PreviewBrowser.PreviewBrowser,
        PreviewBrowser.PreviewBrowser.of({
          executable: Effect.die("No browser should launch in export tests"),
          installed: Effect.succeed(Option.none()),
        }),
      ),
    ),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed({ activeRunId } as OrchestrationV2ThreadShell),
        getThreadRecords: (_thread, _fields, filter) => {
          expect(_thread).toBe(threadId);
          expect(filter?.messageRunIds).toEqual([activeRunId]);
          const messages: ReadonlyArray<OrchestrationV2ConversationMessage> = [
            activeRunId,
            queuedRunId,
            activeRunId,
          ]
            .map((runId, index): OrchestrationV2ConversationMessage => ({
              id: MessageId.make(`message-${index}`),
              threadId,
              runId,
              nodeId: null,
              role: "user",
              text: "Export this page",
              createdBy: "user",
              creationSource: "web",
              attachments: [],
              streaming: false,
              createdAt: DateTime.makeUnsafe(0),
              updatedAt: DateTime.makeUnsafe(0),
              ...((captured || index === 1) && index < 2
                ? {
                    context: {
                      version: 1,
                      records: [],
                      htmlTheme: index === 0 ? theme : otherTheme,
                    },
                  }
                : {}),
            }))
            .filter(
              (message) => message.runId !== null && filter?.messageRunIds?.includes(message.runId),
            );
          return Effect.succeed({ messages } as OrchestrationV2ThreadProjection);
        },
      }),
    ),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-theme-export-" })),
    Layer.provideMerge(NodeServices.layer),
  );

describe("HTML export", () => {
  it.effect("rejects viewport heights in fitted pages while retaining standalone exports", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const fs = yield* FileSystem.FileSystem;
      const html = "<style>main{min-height:100dvh}</style><main>Page</main>";
      const published = yield* service
        .publish({ threadId, title: "Page", height: 400, html })
        .pipe(Effect.flip);
      expect(published._tag).toBe("HtmlRenderViewportHeightError");
      const previewed = yield* service.preview({ threadId, html }).pipe(Effect.flip);
      expect(previewed._tag).toBe("HtmlRenderViewportHeightError");

      const exported = yield* service.exportPage({ threadId, title: "Page", html });
      const saved = yield* fs.readFileString(exported.filePath);
      expect(saved).toContain("min-height:100dvh");
      expect(readHtmlRenderTheme(saved)).toEqual(theme);
      expect(saved).not.toContain("host-context-changed");

      const attachmentPreview = yield* service
        .preview({ threadId, attachmentId: exported.htmlRender.attachmentId })
        .pipe(Effect.flip);
      expect(attachmentPreview._tag).toBe("HtmlRenderViewportHeightError");
    }).pipe(Effect.provide(layerTest())),
  );

  it.effect("exports the active request's theme and inlines images without using a browser", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-theme-image-" });
      const file = `${directory}/image.svg`;
      yield* fs.writeFileString(file, '<svg xmlns="http://www.w3.org/2000/svg"/>');
      const output = yield* service.exportPage({
        threadId,
        title: "Custom page",
        html: `<img src="${file}"><p>Data</p>`,
      });
      const html = yield* fs.readFileString(output.filePath);
      expect(output.fileName).toBe("Custom page.html");
      expect(readHtmlRenderTheme(html)).toEqual(theme);
      expect(html).toContain("data:image/svg+xml;base64,");
      expect(html).not.toContain("host-context-changed");
      expect(html).not.toContain("prefers-color-scheme");
      expect(html).not.toContain(directory);
    }).pipe(Effect.provide(layerTest())),
  );

  it.effect("exports an existing attachment with an explicitly chosen replacement theme", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const fs = yield* FileSystem.FileSystem;
      const published = yield* service.publish({
        threadId,
        title: "Chart",
        height: 200,
        html: "<p>Chart</p>",
      });
      const output = yield* service.exportPage({
        threadId,
        title: "Light chart",
        attachmentId: published.attachmentId,
        theme: otherTheme,
      });
      const html = yield* fs.readFileString(output.filePath);
      expect(readHtmlRenderTheme(html)).toEqual(otherTheme);
      expect(html).toContain("<p>Chart</p>");
      expect(html).not.toContain("--background:#213925;");
    }).pipe(Effect.provide(layerTest())),
  );

  it.effect("refuses missing theme context instead of substituting standard colors", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const error = yield* service
        .exportPage({ threadId, title: "Chart", html: "<p>Chart</p>" })
        .pipe(Effect.flip);
      expect(error._tag).toBe("HtmlArtifactThemeMissingError");
    }).pipe(Effect.provide(layerTest(false))),
  );

  it.effect("retains a saved artifact's theme when exporting without a client snapshot", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const fs = yield* FileSystem.FileSystem;
      const published = yield* service.publish({
        threadId,
        title: "Chart",
        height: 200,
        html: "<p>Chart</p>",
        theme,
      });
      const output = yield* service.exportPage({
        threadId,
        title: "Chart",
        attachmentId: published.attachmentId,
      });
      expect(readHtmlRenderTheme(yield* fs.readFileString(output.filePath))).toEqual(theme);
    }).pipe(Effect.provide(layerTest(false))),
  );

  it.effect("rejects another thread's attachment and ambiguous sources", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      for (const source of [
        { html: "<p>x</p>", attachmentId: "other-html" },
        { attachmentId: "other-html" },
      ]) {
        const error = yield* service
          .exportPage({ threadId, title: "Chart", ...source })
          .pipe(Effect.flip);
        expect(error._tag).toBe("HtmlArtifactSourceError");
      }
    }).pipe(Effect.provide(layerTest())),
  );
});
