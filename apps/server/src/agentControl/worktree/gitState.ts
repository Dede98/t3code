import * as Schema from "effect/Schema";

export interface AgentControlGitWorktreeRecord {
  readonly path: string;
  readonly head: string;
  readonly branch: string | null;
  readonly detached: boolean;
  readonly bare: boolean;
  readonly locked: boolean;
  readonly prunable: boolean;
}

export class AgentControlGitStateParseError extends Schema.TaggedErrorClass<AgentControlGitStateParseError>()(
  "AgentControlGitStateParseError",
  { reason: Schema.String },
) {}

const malformed = (reason: string): never => {
  throw new AgentControlGitStateParseError({ reason });
};

/**
 * Parses `git worktree list --porcelain -z`. Git terminates every field with
 * NUL and every record with an additional NUL, so path bytes such as spaces,
 * tabs, and newlines are data rather than separators.
 */
export const parseAgentControlWorktreeList = (
  stdout: string,
): ReadonlyArray<AgentControlGitWorktreeRecord> => {
  if (!stdout.endsWith("\0\0")) return malformed("missing terminal record separator");
  const records: Array<AgentControlGitWorktreeRecord> = [];
  let fields: Array<string> = [];
  const finish = () => {
    if (fields.length === 0) return malformed("empty record");
    let worktreePath: string | null = null;
    let head: string | null = null;
    let branch: string | null = null;
    let detached = false;
    let bare = false;
    let locked = false;
    let prunable = false;
    for (const field of fields) {
      if (field.startsWith("worktree ")) {
        if (worktreePath !== null || field.length === "worktree ".length) {
          return malformed("invalid worktree field");
        }
        worktreePath = field.slice("worktree ".length);
      } else if (field.startsWith("HEAD ")) {
        if (head !== null || !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(field.slice(5))) {
          return malformed("invalid HEAD field");
        }
        head = field.slice(5);
      } else if (field.startsWith("branch ")) {
        if (branch !== null || !field.startsWith("branch refs/heads/")) {
          return malformed("invalid branch field");
        }
        branch = field.slice("branch refs/heads/".length);
        if (branch.length === 0) return malformed("empty branch");
      } else if (field === "detached") {
        if (detached) return malformed("duplicate detached field");
        detached = true;
      } else if (field === "bare") {
        if (bare) return malformed("duplicate bare field");
        bare = true;
      } else if (field === "locked" || field.startsWith("locked ")) {
        if (locked) return malformed("duplicate locked field");
        locked = true;
      } else if (field === "prunable" || field.startsWith("prunable ")) {
        if (prunable) return malformed("duplicate prunable field");
        prunable = true;
      }
    }
    if (
      worktreePath === null ||
      head === null ||
      (branch === null && !detached && !bare) ||
      (branch !== null && (detached || bare)) ||
      (detached && bare)
    ) {
      return malformed("missing or contradictory required fields");
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
    fields = [];
  };
  for (const token of stdout.split("\0")) {
    if (token.length === 0) {
      if (fields.length > 0) finish();
      continue;
    }
    fields.push(token);
  }
  if (fields.length > 0 || records.length === 0) return malformed("unterminated or empty output");
  return records;
};
