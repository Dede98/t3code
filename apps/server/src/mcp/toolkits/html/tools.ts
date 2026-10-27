import { HtmlThemeSnapshot, OrchestratorMcpFailure } from "@t3tools/contracts";
import {
  HTML_RENDER_COLUMN_WIDTH,
  HTML_RENDER_LAYOUT_GUIDE,
  HTML_RENDER_MAX_FIT_HEIGHT,
  HTML_RENDER_MAX_HEIGHT,
  HTML_RENDER_MAX_TITLE_LENGTH,
  HTML_RENDER_MIN_HEIGHT,
  HTML_RENDER_THEME_GUIDE,
  HTML_RENDER_TOOL_NAME,
} from "@t3tools/shared/htmlRender";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";

import * as HtmlRender from "../../../htmlRender/HtmlRender.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const Theme = Schema.optional(HtmlThemeSnapshot).annotate({
  description:
    "Normally omitted: use the requesting client's captured theme. Supply concrete variables only when the user explicitly chose another palette.",
});

const Html = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512_000)).annotate({
  description: "A complete, self-contained HTML document.",
});

const PAGE_RULES =
  "Inline <style> and <script>. Local images as absolute paths are inlined. Remote URLs load as-is.";

export const HtmlPreviewTool = Tool.make("html_preview", {
  description: `Check an HTML page (html or a thread attachmentId) before html_render. Returns PNG, contentHeight, and console output. If it says the browser is still installing, call again in a minute. ${PAGE_RULES} Theme and layout rules are in html_render.`,
  parameters: Schema.Struct({
    html: Schema.optional(Html),
    attachmentId: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
    theme: Theme,
    standalone: Schema.optional(Schema.Boolean).annotate({
      description:
        "Check exported HTML exactly as it stands, without injecting any theme or T3 bootstrap.",
    }),
    width: Schema.optional(
      Schema.Int.annotate({
        description: `CSS pixels, 240 to 1600. Default ${HTML_RENDER_COLUMN_WIDTH}. Use about 390 for phones.`,
      }),
    ),
    appearance: Schema.optional(
      Schema.Literals(["dark", "light"]).annotate({
        description: "Used only when no client theme was captured. Default dark.",
      }),
    ),
  }),
  success: Schema.Struct({
    width: Schema.Int,
    contentHeight: Schema.Int,
    capturedHeight: Schema.Int,
    consoleMessages: Schema.Array(
      Schema.Struct({
        level: Schema.Literals(["log", "info", "warning", "error"]),
        text: Schema.String,
      }),
    ),
    missingImages: Schema.optional(Schema.Array(Schema.String)),
    screenshot: Schema.Struct({
      mimeType: Schema.Literal("image/png"),
      data: Schema.String,
      width: Schema.Int,
      height: Schema.Int,
    }),
  }),
  failure: OrchestratorMcpFailure,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    HtmlRender.HtmlRender,
  ],
})
  .annotate(Tool.Title, "Preview HTML")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// This affects only the caller's thread, not its workspace, so read-only sandboxes and plan mode may use it.
const HtmlRenderTool = Tool.make(HTML_RENDER_TOOL_NAME, {
  description: `Show a finished HTML page (chart, diagram, mockup) inline in this thread, above your final reply. Do not announce or restate the page. Frame fits height to ${HTML_RENDER_MAX_FIT_HEIGHT}px. On mobile, content past the given height scrolls. ${HTML_RENDER_LAYOUT_GUIDE} ${HTML_RENDER_THEME_GUIDE}`,
  parameters: Schema.Struct({
    html: Html,
    theme: Theme,
    title: Schema.String.check(
      Schema.isMinLength(1),
      Schema.isMaxLength(HTML_RENDER_MAX_TITLE_LENGTH),
    ),
    height: Schema.Int.annotate({
      description: `Frame height in CSS pixels, ${HTML_RENDER_MIN_HEIGHT} to ${HTML_RENDER_MAX_HEIGHT}. Use the contentHeight from html_preview.`,
    }),
  }),
  success: Schema.Struct({
    htmlRender: Schema.Struct({
      attachmentId: Schema.String,
      title: Schema.String,
      height: Schema.Number,
      heights: Schema.optional(Schema.Array(Schema.Tuple([Schema.Int, Schema.Int]))),
    }),
    message: Schema.String,
  }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    HtmlRender.HtmlRender,
  ],
})
  .annotate(Tool.Title, "Render HTML")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

const HtmlExportTool = Tool.make("html_export", {
  description:
    "Export a T3 visualization as standalone HTML with the requesting client's captured theme embedded. Before uploading, publishing, or saving a visualization outside T3, call this tool and upload the returned filePath bytes. Supply either the original html or an attachmentId from this thread. The file works without T3 theme injection. A missing captured theme is an error; only pass theme when the user explicitly chose another palette. Preview the returned htmlRender.attachmentId with html_preview standalone:true. Remote scripts and fonts still require their own network access.",
  parameters: Schema.Struct({
    html: Schema.optional(Html),
    attachmentId: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
    title: Schema.String.check(
      Schema.isMinLength(1),
      Schema.isMaxLength(HTML_RENDER_MAX_TITLE_LENGTH),
    ),
    theme: Theme,
  }),
  success: Schema.Struct({
    htmlRender: Schema.Struct({
      attachmentId: Schema.String,
      title: Schema.String,
      height: Schema.Number,
    }),
    filePath: Schema.String,
    fileName: Schema.String,
  }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    HtmlRender.HtmlRender,
  ],
})
  .annotate(Tool.Title, "Export HTML")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false);

export const HtmlPreviewToolkit = Toolkit.make(HtmlPreviewTool);

export const HtmlRenderToolkit = Toolkit.make(HtmlRenderTool, HtmlExportTool);

export const HtmlToolkit = Toolkit.make(HtmlPreviewTool, HtmlRenderTool, HtmlExportTool);
