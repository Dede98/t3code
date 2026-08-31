import * as NodeModule from "node:module";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

const require = NodeModule.createRequire(import.meta.url);
const NodeFS = require("node:fs");

const snapshotFilename = process.env.T3_RUN_ONCE_062_SNAPSHOT;
const acknowledgementSocket = process.env.T3_RUN_ONCE_062_ACK_SOCKET;
const originalRemove = NodeFS.rm;
let captured = false;

const acknowledgeSnapshot = () =>
  new Promise((resolve, reject) => {
    const socket = NodeNet.createConnection(acknowledgementSocket);
    let response = "";
    socket.setEncoding("utf8");
    socket.once("error", reject);
    socket.on("data", (chunk) => {
      response += chunk;
      if (!response.includes("\n")) return;
      if (response.trim() !== "ack") {
        reject(new Error(`unexpected production 062 snapshot acknowledgement: ${response.trim()}`));
        socket.destroy();
        return;
      }
      socket.end();
      resolve();
    });
    socket.write("snapshot-ready\n");
  });

const captureBeforeCleanup = async (directory) => {
  const databaseFilename = NodeFS.readdirSync(directory).find((name) => name.endsWith(".sqlite"));
  if (databaseFilename === undefined) {
    throw new Error(`production 062 database is missing in ${directory}`);
  }
  const sourceFilename = NodePath.join(directory, databaseFilename);
  const database = new NodeSqlite.DatabaseSync(sourceFilename, { readOnly: true });
  try {
    const authority = database
      .prepare(`SELECT
        (SELECT count(*) FROM main.effect_sql_migrations WHERE migration_id = 62) AS migration,
        (SELECT count(*)
         FROM main.agent_control_task_verification_finalization_evidence) AS evidence,
        (SELECT count(*)
         FROM main.agent_control_task_verification_finalization_receipts) AS receipts,
        (SELECT count(*)
         FROM main.agent_control_task_verification_finalization_markers) AS markers,
        (SELECT count(*) FROM main.agent_control_events
         WHERE event_type = 'agentControl.task.finalizedAfterVerification') AS events,
        (SELECT count(*) FROM main.agent_control_task_states
         WHERE status IN ('succeeded', 'failed', 'cancelled')) AS projections`)
      .get();
    if (
      authority.migration !== 1 ||
      authority.evidence !== 1 ||
      authority.receipts !== 1 ||
      authority.markers !== 1 ||
      authority.events !== 1 ||
      authority.projections !== 1
    ) {
      throw new Error(`production 062 authority is incomplete: ${JSON.stringify(authority)}`);
    }
    await NodeSqlite.backup(database, snapshotFilename);
  } finally {
    database.close();
  }
  await acknowledgeSnapshot();
};

if (snapshotFilename !== undefined && acknowledgementSocket !== undefined) {
  NodeFS.rm = (path, options, callback) => {
    const directory = String(path);
    if (
      captured ||
      !NodePath.basename(directory).startsWith("task-verification-finalizer-production-061-")
    ) {
      return originalRemove.call(NodeFS, path, options, callback);
    }
    captured = true;
    void captureBeforeCleanup(directory).then(
      () => originalRemove.call(NodeFS, path, options, callback),
      (cause) => callback(cause),
    );
  };
}
