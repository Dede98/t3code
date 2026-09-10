import * as NodeModule from "node:module";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

const require = NodeModule.createRequire(import.meta.url);
const NodeFS = require("node:fs");

const snapshotFilename = process.env.T3_TASK_FINALIZATION_061_SNAPSHOT;
const acknowledgementSocket = process.env.T3_TASK_FINALIZATION_061_ACK_SOCKET;
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
        reject(new Error(`unexpected production 061 snapshot acknowledgement: ${response.trim()}`));
        socket.destroy();
        return;
      }
      socket.end();
      resolve();
    });
    socket.write("snapshot-ready\n");
  });

const captureBeforeCleanup = async (directory) => {
  const sourceFilename = NodePath.join(directory, "state.sqlite");
  const database = new NodeSqlite.DatabaseSync(sourceFilename, { readOnly: true });
  try {
    const authority = database
      .prepare(`SELECT
        (SELECT count(*) FROM main.effect_sql_agent_control_migrations WHERE migration_id = 61) AS migration,
        (SELECT count(*) FROM main.agent_control_verification_finalization_markers) AS markers`)
      .get();
    if (authority.migration !== 1 || authority.markers !== 1) {
      return;
    }
    captured = true;
    await NodeSqlite.backup(database, snapshotFilename);
  } finally {
    database.close();
  }
  await acknowledgeSnapshot();
};

if (snapshotFilename !== undefined && acknowledgementSocket !== undefined) {
  NodeFS.rm = (path, options, callback) => {
    const directory = String(path);
    if (captured || !NodePath.basename(directory).startsWith("t3-initial-planning-finalizer-")) {
      return originalRemove.call(NodeFS, path, options, callback);
    }
    void captureBeforeCleanup(directory).then(
      () => originalRemove.call(NodeFS, path, options, callback),
      (cause) => callback(cause),
    );
  };
}
