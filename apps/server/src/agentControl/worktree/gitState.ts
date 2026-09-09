import * as Schema from "effect/Schema";

const OBJECT_ID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const MODE = /^(?:000000|100644|100755|120000|160000)$/;
const XY = /^[.MADRCUT]{2}$/;
const SUBMODULE = /^(?:N\.\.\.|S[C.][M.][U.])$/;

export interface AgentControlGitWorktreeRecord {
  readonly path: string;
  readonly head: string | null;
  readonly branch: string | null;
  readonly detached: boolean;
  readonly bare: boolean;
  readonly locked: boolean;
  readonly prunable: boolean;
}

export type AgentControlGitStatusRecord =
  | { readonly type: "ordinary"; readonly path: string }
  | { readonly type: "rename-copy"; readonly path: string; readonly originPath: string }
  | { readonly type: "unmerged"; readonly path: string }
  | { readonly type: "untracked"; readonly path: string }
  | { readonly type: "ignored"; readonly path: string };

export class AgentControlGitStateParseError extends Schema.TaggedError<AgentControlGitStateParseError>()(
  "AgentControlGitStateParseError",
  { reason: Schema.String },
) {}

const malformed = (reason: string): never => {
  throw new AgentControlGitStateParseError({ reason });
};

const fieldValue = (field: string, prefix: string) => {
  if (!field.startsWith(prefix) || field.length === prefix.length) {
    return malformed(`invalid ${prefix.trim()} field`);
  }
  return field.slice(prefix.length);
};

/**
 * Strict state-machine parser for `git worktree list --porcelain -z`.
 *
 * Every field and record delimiter is significant. Unknown fields are rejected
 * because a newer Git field can carry ownership or availability semantics that
 * this controller must not silently ignore.
 */
export const parseAgentControlWorktreeList = (
  stdout: string,
): ReadonlyArray<AgentControlGitWorktreeRecord> => {
  if (stdout.length === 0) return malformed("empty worktree list");
  if (stdout.startsWith("\0")) return malformed("leading empty record");
  if (!stdout.endsWith("\0\0")) return malformed("missing terminal record separator");

  const tokens = stdout.split("\0");
  if (tokens.at(-1) !== "" || tokens.at(-2) !== "") {
    return malformed("missing terminal record separator");
  }
  tokens.pop();
  if (tokens.at(-2) === "") return malformed("extra terminal record separator");

  const records: Array<AgentControlGitWorktreeRecord> = [];
  let index = 0;
  while (index < tokens.length) {
    const first = tokens[index++];
    if (first === undefined || first === "") return malformed("empty record");
    const worktreePath = fieldValue(first, "worktree ");
    let head: string | null = null;
    let branch: string | null = null;
    let detached = false;
    let bare = false;
    let locked = false;
    let prunable = false;
    let terminated = false;

    while (index < tokens.length) {
      const field = tokens[index++];
      if (field === "") {
        terminated = true;
        break;
      }
      if (field === undefined) return malformed("truncated record");
      if (field.startsWith("worktree ")) return malformed("missing record separator");
      if (field.startsWith("HEAD ")) {
        const value = fieldValue(field, "HEAD ");
        if (head !== null || !OBJECT_ID.test(value)) return malformed("invalid HEAD field");
        head = value;
        continue;
      }
      if (field.startsWith("branch ")) {
        const value = fieldValue(field, "branch ");
        if (branch !== null || !value.startsWith("refs/heads/")) {
          return malformed("invalid branch field");
        }
        branch = value.slice("refs/heads/".length);
        if (branch.length === 0) return malformed("empty branch");
        continue;
      }
      if (field === "detached") {
        if (detached) return malformed("duplicate detached field");
        detached = true;
        continue;
      }
      if (field === "bare") {
        if (bare) return malformed("duplicate bare field");
        bare = true;
        continue;
      }
      if (field === "locked" || field.startsWith("locked ")) {
        if (locked) return malformed("duplicate locked field");
        locked = true;
        continue;
      }
      if (field === "prunable" || field.startsWith("prunable ")) {
        if (prunable) return malformed("duplicate prunable field");
        prunable = true;
        continue;
      }
      return malformed(`unknown worktree field: ${field.split(" ", 1)[0] ?? ""}`);
    }
    if (!terminated && index === tokens.length) return malformed("truncated final record");
    if (bare) {
      if (head !== null || branch !== null || detached) {
        return malformed("contradictory bare record");
      }
    } else if (detached) {
      if (head === null || branch !== null) return malformed("invalid detached record");
    } else if (head === null || branch === null) {
      return malformed("missing normal worktree fields");
    }
    records.push({
      path: worktreePath,
      head,
      branch,
      detached,
      bare,
      locked,
      prunable,
    });
  }
  return records;
};

const ORDINARY = /^1 ([.MADRCUT]{2}) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ([\s\S]+)$/;
const RENAME_COPY = /^2 ([.MADRCUT]{2}) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ([\s\S]+)$/;
const UNMERGED = /^u ([.MADRCUT]{2}) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ([\s\S]+)$/;

const validateFields = (
  xy: string,
  submodule: string,
  modes: ReadonlyArray<string>,
  objectIds: ReadonlyArray<string>,
  path: string,
) => {
  if (!XY.test(xy)) return malformed("invalid XY field");
  if (!SUBMODULE.test(submodule)) return malformed("invalid submodule field");
  if (modes.some((mode) => !MODE.test(mode))) return malformed("invalid mode field");
  if (objectIds.some((oid) => !OBJECT_ID.test(oid))) {
    return malformed("invalid object id field");
  }
  if (path.length === 0) return malformed("empty status path");
};

/**
 * Strict NUL parser for
 * `git status --porcelain=v2 -z --untracked-files=all --ignore-submodules=none`.
 */
export const parseAgentControlPorcelainV2Status = (
  stdout: string,
): ReadonlyArray<AgentControlGitStatusRecord> => {
  if (stdout.length === 0) return [];
  if (!stdout.endsWith("\0")) return malformed("unterminated status record");
  const tokens = stdout.split("\0");
  tokens.pop();
  if (tokens.some((token) => token.length === 0)) return malformed("empty status record");

  const records: Array<AgentControlGitStatusRecord> = [];
  for (let index = 0; index < tokens.length; index += 1) {
    const record = tokens[index]!;
    if (record.startsWith("# ")) return malformed("unexpected status header");
    const type = record[0];
    if (type === "1") {
      const fields = ORDINARY.exec(record);
      if (fields === null) return malformed("invalid ordinary field count");
      validateFields(fields[1]!, fields[2]!, fields.slice(3, 6), fields.slice(6, 8), fields[8]!);
      records.push({ type: "ordinary", path: fields[8]! });
      continue;
    }
    if (type === "2") {
      const fields = RENAME_COPY.exec(record);
      if (fields === null) return malformed("invalid rename/copy field count");
      validateFields(fields[1]!, fields[2]!, fields.slice(3, 6), fields.slice(6, 8), fields[9]!);
      if (!/^[RC](?:100|[0-9]{1,2})$/.test(fields[8]!)) {
        return malformed("invalid rename/copy score");
      }
      const originPath = tokens[++index];
      if (originPath === undefined || originPath.length === 0) {
        return malformed("missing rename/copy origin path");
      }
      records.push({ type: "rename-copy", path: fields[9]!, originPath });
      continue;
    }
    if (type === "u") {
      const fields = UNMERGED.exec(record);
      if (fields === null) return malformed("invalid unmerged field count");
      validateFields(fields[1]!, fields[2]!, fields.slice(3, 7), fields.slice(7, 10), fields[10]!);
      records.push({ type: "unmerged", path: fields[10]! });
      continue;
    }
    if (type === "?" || type === "!") {
      if (record[1] !== " " || record.length === 2) return malformed("invalid short status record");
      records.push({
        type: type === "?" ? "untracked" : "ignored",
        path: record.slice(2),
      });
      continue;
    }
    return malformed(`unknown status record type: ${type ?? ""}`);
  }
  return records;
};
