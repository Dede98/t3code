import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

let nextServerRequestId = 10_000;
let pendingSkillsListRequestId: number | string | null = null;
let pendingUserInputRequestId: number | null = null;
let activeThreadId = "mock-codex-thread-1";
let activeTurnId = "mock-codex-turn-1";
const requestLogPath = process.env.CODEX_APP_SERVER_REQUEST_LOG_PATH;
const terminalSignalPath = process.env.CODEX_APP_SERVER_TERMINAL_SIGNAL_PATH;

const writeMessage = (message: unknown) => {
  process.stdout.write(`${JSON.stringify(message)}\n`);
};

const respond = (id: number | string, result: unknown) => {
  writeMessage({ id, result });
};

const respondError = (id: number | string, code: number, message: string) => {
  writeMessage({
    id,
    error: {
      code,
      message,
    },
  });
};

const sendRequest = (method: string, params: unknown) => {
  const id = nextServerRequestId++;
  writeMessage({ id, method, params });
  return id;
};

const handleMethod = (message: Record<string, unknown>) => {
  const method = message.method;
  if (typeof method !== "string") {
    return;
  }

  switch (method) {
    case "initialize": {
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone mock peer process has no Effect runtime.
      const platform = NodeOS.platform();
      const stderrBytes = Number(process.env.CODEX_APP_SERVER_TEST_STDERR_BYTES ?? 0);
      if (Number.isFinite(stderrBytes) && stderrBytes > 0) {
        process.stderr.write("x".repeat(stderrBytes), () => {
          respond(message.id as number | string, {
            userAgent: "mock-codex-app-server",
            codexHome: process.cwd(),
            platformFamily: platform === "win32" ? "windows" : "unix",
            platformOs: platform === "darwin" ? "macos" : platform,
          });
        });
        return;
      }
      respond(message.id as number | string, {
        userAgent: "mock-codex-app-server",
        codexHome: process.cwd(),
        platformFamily: platform === "win32" ? "windows" : "unix",
        platformOs: platform === "darwin" ? "macos" : platform,
      });
      return;
    }
    case "initialized": {
      if (process.env.CODEX_APP_SERVER_EMIT_READY_DELTA === "0") {
        return;
      }
      writeMessage({
        method: "item/agentMessage/delta",
        params: {
          delta: "Mock server is ready.",
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
        },
      });
      return;
    }
    case "account/read": {
      respond(message.id as number | string, {
        account: {
          type: "chatgpt",
          email: "mock@example.com",
          planType: "plus",
        },
        requiresOpenaiAuth: false,
      });
      return;
    }
    case "thread/start": {
      const params =
        typeof message.params === "object" && message.params !== null
          ? (message.params as Record<string, unknown>)
          : {};
      const cwd = typeof params.cwd === "string" ? params.cwd : process.cwd();
      const model = typeof params.model === "string" ? params.model : "gpt-5.4";
      respond(message.id as number | string, {
        approvalPolicy: "untrusted",
        approvalsReviewer: "user",
        cwd,
        model,
        modelProvider: "openai",
        reasoningEffort: "high",
        sandbox: { type: "readOnly", networkAccess: false },
        serviceTier: params.serviceTier ?? null,
        thread: {
          id: activeThreadId,
          cliVersion: "mock-codex-app-server",
          createdAt: 1,
          cwd,
          ephemeral: false,
          modelProvider: "openai",
          preview: "",
          sessionId: "mock-codex-session-1",
          source: "appServer",
          status: { type: "idle" },
          turns: [],
          updatedAt: 1,
        },
      });
      return;
    }
    case "turn/start": {
      const params =
        typeof message.params === "object" && message.params !== null
          ? (message.params as Record<string, unknown>)
          : {};
      if (typeof params.threadId === "string") activeThreadId = params.threadId;
      respond(message.id as number | string, {
        turn: {
          id: activeTurnId,
          items: [],
          status: "inProgress",
        },
      });
      return;
    }
    case "skills/list": {
      pendingSkillsListRequestId = message.id as number | string;
      pendingUserInputRequestId = sendRequest("item/tool/requestUserInput", {
        itemId: "item-approval-1",
        threadId: "thread-1",
        turnId: "turn-1",
        questions: [
          {
            id: "approved",
            header: "Approve",
            question: "Continue with the mock skills request?",
            options: [
              {
                label: "yes",
                description: "Approve the request",
              },
            ],
          },
        ],
      });
      return;
    }
    default: {
      if (message.id !== undefined) {
        respondError(message.id as number | string, -32601, `Unhandled request: ${method}`);
      }
    }
  }
};

const handleResponse = (message: Record<string, unknown>) => {
  if (message.id !== pendingUserInputRequestId) {
    return;
  }

  pendingUserInputRequestId = null;

  respond(pendingSkillsListRequestId!, {
    data: [
      {
        cwd: process.cwd(),
        errors: [],
        skills: [],
      },
    ],
  });
  pendingSkillsListRequestId = null;
};

let remainder = "";

process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  remainder += chunk;
  const lines = remainder.split("\n");
  remainder = lines.pop() ?? "";

  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }

    const message = JSON.parse(trimmed) as Record<string, unknown>;
    if (requestLogPath) {
      NodeFS.appendFileSync(requestLogPath, `${JSON.stringify(message)}\n`, "utf8");
    }
    if ("method" in message) {
      handleMethod(message);
      continue;
    }
    if ("id" in message) {
      handleResponse(message);
    }
  }
});

if (terminalSignalPath) {
  const watcher = NodeFS.watch(NodePath.dirname(terminalSignalPath), (_eventType, filename) => {
    if (String(filename) !== NodePath.basename(terminalSignalPath)) return;
    watcher.close();
    writeMessage({
      method: "turn/completed",
      params: {
        threadId: activeThreadId,
        turn: {
          id: activeTurnId,
          items: [],
          status: "completed",
        },
      },
    });
  });
}

process.stdin.on("end", () => {
  process.exit(0);
});
