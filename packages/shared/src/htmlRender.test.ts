import { describe, expect, it } from "vite-plus/test";

import {
  exportHtmlRender,
  readHtmlRenderTheme,
  withHtmlTheme,
  htmlRenderTheme,
  injectHtmlRenderBootstrap,
  readHtmlRenderImageRequest,
  readHtmlRenderLinkRequest,
} from "./htmlRender.ts";
import { T3_CODE_DARK_THEME_COLORS, T3_CODE_LIGHT_THEME_COLORS } from "./themePalettes.ts";
import { htmlRenderFromToolItem } from "./toolOutput.ts";

const reference = { attachmentId: "thread-abc-123.html", title: "Chart", height: 420 };

describe("injectHtmlRenderBootstrap", () => {
  it("puts the theme ahead of the page's own head content", () => {
    const html =
      "<!doctype html><html><head><style>:root{--background:red}</style></head><body>x</body></html>";
    const injected = injectHtmlRenderBootstrap(html);
    const themeAt = injected.indexOf('<style id="t3-theme">');
    expect(themeAt).toBeGreaterThan(injected.indexOf("<head>"));
    expect(themeAt).toBeLessThan(injected.indexOf(":root{--background:red}"));
    expect(injected).toContain('<meta charset="utf-8">');
    expect(injected).toContain('name="viewport"');
  });

  it("wraps fragments without a head and keeps existing meta tags", () => {
    const fragment =
      '<meta charset="utf-8"><meta name="viewport" content="width=device-width"><p>hi</p>';
    const injected = injectHtmlRenderBootstrap(fragment);
    expect(injected.startsWith("<!doctype html><head>")).toBe(true);
    expect(injected.match(/charset/g)).toHaveLength(1);
    expect(injected.match(/name="viewport"/g)).toHaveLength(1);
    expect(injected.endsWith("<p>hi</p>")).toBe(true);
  });

  it.each(["textarea", "iframe", "plaintext"])("keeps the bootstrap outside %s content", (tag) => {
    const fragment = `<${tag}><head><meta name="viewport"></head></${tag}>`;
    const injected = injectHtmlRenderBootstrap(fragment);
    expect(injected.startsWith("<!doctype html><head>")).toBe(true);
    expect(injected.indexOf('<style id="t3-theme">')).toBeLessThan(injected.indexOf(`<${tag}>`));
    expect(injected).toContain('<meta name="viewport" content="width=device-width');
    expect(injected.endsWith(fragment)).toBe(true);
  });

  it.each([
    '<template><head><meta name="viewport"></head></template>',
    '<template><template>inner</template><head><meta name="viewport"></head></template>',
  ])("keeps the bootstrap outside inert template content: %s", (fragment) => {
    const injected = injectHtmlRenderBootstrap(fragment);
    expect(injected.startsWith("<!doctype html><head>")).toBe(true);
    expect(injected.indexOf('<style id="t3-theme">')).toBeLessThan(injected.indexOf("<template>"));
    expect(injected).toContain('<meta name="viewport" content="width=device-width');
    expect(injected.endsWith(fragment)).toBe(true);
  });

  it("ignores tags written inside comments and scripts", () => {
    const html =
      '<!-- copy <head> and <meta name="viewport"> here --><html><head>' +
      "<script>const tag = '<meta name=\"viewport\">';</script></head><body>x</body></html>";
    const injected = injectHtmlRenderBootstrap(html);
    expect(injected.indexOf('<style id="t3-theme">')).toBeGreaterThan(
      injected.indexOf("<html><head>"),
    );
    expect(injected).toContain('<meta name="viewport" content="width=device-width');
  });
});

describe("readHtmlRenderLinkRequest", () => {
  it("accepts only http(s) URLs in an MCP Apps ui/open-link request", () => {
    const link = (url: unknown, modifiers = {}) => ({
      jsonrpc: "2.0",
      id: 1,
      method: "ui/open-link",
      params: { url, ...modifiers },
    });
    expect(readHtmlRenderLinkRequest(link("https://example.com/a"))).toEqual({
      id: 1,
      url: "https://example.com/a",
      metaKey: false,
      ctrlKey: false,
    });
    expect(
      readHtmlRenderLinkRequest(link("https://example.com/a", { metaKey: true, ctrlKey: "yes" })),
    ).toEqual({ id: 1, url: "https://example.com/a", metaKey: true, ctrlKey: false });
    expect(readHtmlRenderLinkRequest(link("javascript:alert(1)"))).toBeUndefined();
    expect(readHtmlRenderLinkRequest(link("file:///etc/passwd"))).toBeUndefined();
    expect(
      readHtmlRenderLinkRequest({
        jsonrpc: "2.0",
        method: "ui/open-link",
        params: { url: "https://example.com" },
      }),
    ).toBeUndefined();
    expect(
      readHtmlRenderLinkRequest({ type: "t3-html-render-link", url: "https://example.com" }),
    ).toBeUndefined();
  });
});

describe("readHtmlRenderImageRequest", () => {
  it("accepts http(s) and inline image URLs in an open-image notification", () => {
    const image = (params: unknown) => ({
      jsonrpc: "2.0",
      method: "t3/notifications/open-image",
      params,
    });
    expect(
      readHtmlRenderImageRequest(image({ src: "data:image/png;base64,AA", name: " Chart " })),
    ).toEqual({
      src: "data:image/png;base64,AA",
      name: "Chart",
    });
    expect(readHtmlRenderImageRequest(image({ src: "https://example.com/a.png" }))).toEqual({
      src: "https://example.com/a.png",
      name: "Image",
    });
    expect(readHtmlRenderImageRequest(image({ src: "javascript:alert(1)" }))).toBeUndefined();
    expect(readHtmlRenderImageRequest(image({ src: "data:text/html,<p>" }))).toBeUndefined();
    expect(readHtmlRenderImageRequest(image({ src: "blob:null/1" }))).toBeUndefined();
  });
});

describe("htmlRenderFromToolItem", () => {
  const result = { htmlRender: reference, message: "Rendered above your reply." };

  it("reads the reference from each provider's result envelope", () => {
    for (const [toolName, output] of [
      ["mcp__t3-code__html_render", [{ type: "text", text: JSON.stringify(result) }]],
      ["t3-code.html_render", { structuredContent: result, content: [] }],
      ["t3-code-thread_1_html_render", JSON.stringify(result)],
      ["html_render", result],
    ] as const) {
      expect(htmlRenderFromToolItem({ toolName, output })).toEqual(reference);
    }
  });
});

describe("standalone theme export", () => {
  const theme = htmlRenderTheme(
    { ...T3_CODE_DARK_THEME_COLORS, canvas: "#213925", accent: "#c8ff85" },
    "dark",
  );
  it("embeds custom colors, fonts and appearance without a host bootstrap or OS fallback", () => {
    const body = "<main>Revenue</main><script>window.chartReady=true;</script>";
    const exported = exportHtmlRender(injectHtmlRenderBootstrap(body), theme);
    expect(exported).toContain("--background:#213925;");
    expect(exported).toContain("--accent:#c8ff85;");
    expect(exported).toContain("--chart-1:#c8ff85;");
    expect(exported).toContain("color-scheme:dark;");
    expect(exported).toContain(body);
    expect(exported).not.toContain("host-context-changed");
    expect(exported).not.toContain("prefers-color-scheme");
    expect(readHtmlRenderTheme(exported)).toEqual(theme);
    expect(exportHtmlRender(exported, theme)).toBe(exported);
  });
  it("replaces an old capture and ignores bootstrap-looking text in inert content", () => {
    const inert =
      '<template><style id="t3-theme">example</style></template><!-- <style id="t3-theme">comment</style> -->';
    const page = injectHtmlRenderBootstrap(
      `<html><head></head><body>${inert}</body></html>`,
      theme,
    );
    const light = htmlRenderTheme(T3_CODE_LIGHT_THEME_COLORS, "light");
    const exported = exportHtmlRender(page, light);
    expect(exported).toContain(inert);
    expect(exported).toContain(`--background:${light.variables["--background"]};`);
    expect(exported).not.toContain("--background:#213925;");
    expect(readHtmlRenderTheme(exported)).toEqual(light);
  });
  it("removes a previous release's host bootstrap when exporting or reopening a saved page", () => {
    const page = injectHtmlRenderBootstrap("<main>Saved report</main>", theme).replace(
      /(<script id="t3-html-bootstrap">)[\s\S]*?(<\/script>)/,
      "$1window.previousHostBootstrap=true;$2",
    );
    const exported = exportHtmlRender(page, theme);
    expect(exported).toContain("<main>Saved report</main>");
    expect(exported).not.toContain("previousHostBootstrap");
    expect(exported).not.toContain("t3-html-bootstrap");
    const reopened = injectHtmlRenderBootstrap(page, theme);
    expect(reopened).not.toContain("previousHostBootstrap");
    expect(reopened.match(/id="t3-html-bootstrap"/g)).toHaveLength(1);
  });
  it("captures a request without altering its composer records", () => {
    const context = { version: 1 as const, records: [] };
    expect(withHtmlTheme(context, theme)).toEqual({ ...context, htmlTheme: theme });
    expect(context).toEqual({ version: 1, records: [] });
  });
  it("refuses values that can break out of a stylesheet", () => {
    expect(() =>
      exportHtmlRender("<p>x</p>", {
        ...theme,
        variables: { ...theme.variables, "--accent": "red;</style><script>alert(1)</script>" },
      }),
    ).toThrow();
  });
});
