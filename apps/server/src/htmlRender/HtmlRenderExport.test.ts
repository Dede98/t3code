import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it, vi } from "@effect/vitest";
import {
  RunId,
  MessageId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import {
  exportHtmlRender,
  htmlRenderTheme,
  htmlRenderThemeFragment,
  readHtmlRenderTheme,
  type HtmlRenderTheme,
} from "@t3tools/shared/htmlRender";
import {
  T3_CODE_DARK_THEME_COLORS,
  T3_CODE_LIGHT_THEME_COLORS,
} from "@t3tools/shared/themePalettes";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as HeadlessChrome from "./headlessChrome.ts";
import * as HtmlRender from "./HtmlRender.ts";

const threadId = ThreadId.make("theme-export");
const earlierRunId = RunId.make("earlier");
const activeRunId = RunId.make("active");
const queuedRunId = RunId.make("queued");
const theme = htmlRenderTheme(
  { ...T3_CODE_DARK_THEME_COLORS, canvas: "#213925", accent: "#c8ff85" },
  "dark",
);
const earlierTheme = htmlRenderTheme({ ...T3_CODE_DARK_THEME_COLORS, canvas: "#101820" }, "dark");
const otherTheme = htmlRenderTheme(T3_CODE_LIGHT_THEME_COLORS, "light");
const queuedTheme = htmlRenderTheme({ ...T3_CODE_LIGHT_THEME_COLORS, canvas: "#fff8e7" }, "light");
const runs = [
  { id: earlierRunId, status: "completed", startedAt: DateTime.makeUnsafe(0) },
  { id: activeRunId, status: "running", startedAt: DateTime.makeUnsafe(1) },
  { id: queuedRunId, status: "queued", startedAt: null },
] as unknown as ReadonlyArray<OrchestrationV2Run>;
const message = (
  index: number,
  runId: RunId,
  htmlTheme?: HtmlRenderTheme,
): OrchestrationV2ConversationMessage => ({
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
  createdAt: DateTime.makeUnsafe(index),
  updatedAt: DateTime.makeUnsafe(index),
  ...(htmlTheme === undefined ? {} : { context: { version: 1, records: [], htmlTheme } }),
});
// The queued request always carries a theme: no test may ever see it.
const layerTest = (
  options: {
    readonly captured?: boolean;
    readonly earlier?: boolean;
    readonly activeRun?: boolean;
    readonly executable?: string;
  } = {},
) =>
  HtmlRender.layer.pipe(
    Layer.provide(McpProviderSessions.layer),
    Layer.provide(
      Layer.succeed(
        PreviewBrowser.PreviewBrowser,
        PreviewBrowser.PreviewBrowser.of({
          executable:
            options.executable === undefined
              ? Effect.die("No browser should launch in export tests")
              : Effect.succeed(options.executable),
          installed: Effect.succeedNone,
        }),
      ),
    ),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () =>
          Effect.succeed({
            activeRunId: options.activeRun === false ? null : activeRunId,
          } as OrchestrationV2ThreadShell),
        getThreadRecords: (thread, fields, filter) => {
          expect(thread).toBe(threadId);
          if ((fields as ReadonlyArray<string>).includes("runs")) {
            // A bounded read: long threads hold thousands of runs.
            expect(filter?.latestRunLimit).toBeGreaterThan(0);
            return Effect.succeed({ runs } as unknown as OrchestrationV2ThreadProjection);
          }
          expect(filter?.messageRoles).toEqual(["user"]);
          const messages: ReadonlyArray<OrchestrationV2ConversationMessage> = [
            message(0, earlierRunId, options.earlier === true ? earlierTheme : undefined),
            message(1, activeRunId, options.captured === false ? undefined : theme),
            message(2, queuedRunId, queuedTheme),
            message(3, activeRunId),
          ].filter((entry) => entry.runId !== null && filter?.messageRunIds?.includes(entry.runId));
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
    }).pipe(Effect.provide(layerTest({ captured: false }))),
  );

  it.effect.each([
    { when: "both captured one", captured: true, activeRun: true, expected: theme },
    { when: "only the earlier run did", captured: false, activeRun: true, expected: earlierTheme },
    { when: "no run is active", captured: false, activeRun: false, expected: earlierTheme },
    { when: "its own run is waiting", captured: true, activeRun: false, expected: theme },
  ])(
    "exports the newest theme a started run captured when $when",
    ({ captured, activeRun, expected }) =>
      Effect.gen(function* () {
        const service = yield* HtmlRender.HtmlRender;
        const fs = yield* FileSystem.FileSystem;
        const output = yield* service.exportPage({
          threadId,
          title: "Chart",
          html: "<p>Chart</p>",
        });
        expect(readHtmlRenderTheme(yield* fs.readFileString(output.filePath))).toEqual(expected);
      }).pipe(Effect.provide(layerTest({ captured, earlier: true, activeRun }))),
  );

  it.effect.each([
    {
      embedded: "an earlier run's theme",
      page: "a plain page",
      html: "<p>Chart</p>",
      expected: earlierTheme,
    },
    {
      embedded: "the page's own theme",
      page: "an exported page",
      html: exportHtmlRender("<p>Chart</p>", theme),
      expected: theme,
    },
  ])("embeds $embedded when an unthemed run renders $page inline", ({ html, expected }) =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const fs = yield* FileSystem.FileSystem;
      const config = yield* ServerConfig.ServerConfig;
      const published = yield* service.publish({ threadId, title: "Chart", height: 200, html });
      const stored = resolveAttachmentPathById({
        attachmentsDir: config.attachmentsDir,
        attachmentId: published.attachmentId,
      });
      expect(readHtmlRenderTheme(yield* fs.readFileString(stored ?? ""))).toEqual(expected);
    }).pipe(Effect.provide(layerTest({ captured: false, earlier: true }))),
  );

  it.effect("previews with an earlier run's theme rather than the standard palette", () =>
    Effect.gen(function* () {
      const service = yield* HtmlRender.HtmlRender;
      const capture = vi
        .spyOn(HeadlessChrome, "captureHtmlScreenshot")
        .mockImplementation((input) => {
          expect(input.urlFragment).toBe(htmlRenderThemeFragment(earlierTheme));
          return Effect.succeed({
            png: "",
            contentHeight: 80,
            capturedHeight: 80,
            consoleMessages: [],
          });
        });
      yield* service
        .preview({ threadId, html: "<p>Chart</p>" })
        .pipe(Effect.ensuring(Effect.sync(() => capture.mockRestore())));
    }).pipe(
      Effect.provide(layerTest({ captured: false, earlier: true, executable: "unused-browser" })),
    ),
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
    }).pipe(Effect.provide(layerTest({ captured: false, earlier: true }))),
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
