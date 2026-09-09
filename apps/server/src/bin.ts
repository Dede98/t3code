import { isEntrypoint } from "./entrypoint.ts";
import {
  SQLITE_NODE_RUNTIME_REQUIRED_CODE,
  nodeRuntimeRequiredError,
} from "./serverRuntimeGate.ts";

if (
  isEntrypoint({
    moduleUrl: import.meta.url,
    entryPath: process.argv[1],
    runtimeMain: import.meta.main,
  })
) {
  if (nodeRuntimeRequiredError() !== undefined) {
    process.stderr.write(`${SQLITE_NODE_RUNTIME_REQUIRED_CODE}\n`);
    process.exitCode = 1;
  } else {
    const { runCliMain } = await import("./cli/main.ts");
    runCliMain();
  }
}
