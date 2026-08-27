import {
  SQLITE_NODE_RUNTIME_REQUIRED_CODE,
  nodeRuntimeRequiredError,
} from "./serverRuntimeGate.ts";

if (import.meta.main) {
  if (nodeRuntimeRequiredError() !== undefined) {
    process.stderr.write(`${SQLITE_NODE_RUNTIME_REQUIRED_CODE}\n`);
    process.exitCode = 1;
  } else {
    const { runCliMain } = await import("./cli/main.ts");
    runCliMain();
  }
}
