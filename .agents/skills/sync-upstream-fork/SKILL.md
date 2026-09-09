---
name: sync-upstream-fork
description: Safely compare and merge upstream/main into this repository's fork main while preserving local work, selecting the best implementation when upstream overlaps fork features, resolving conflicts semantically, and running focused verification. Use for upstream fetches, upstream sync audits, merges from upstream/main, or investigations of duplicated upstream and fork functionality.
---

# Sync Upstream Fork

Integrate `upstream/main` into the fork's local `main` with a normal merge. Preserve required
behavior, but do not preserve a fork implementation merely because it came first.

Before inspecting or merging upstream, read
[`references/protected-fork-behavior.md`](references/protected-fork-behavior.md) completely. Treat
its inventory as required behavior to verify, not as a command to retain today's code shape.

## Establish the live state

1. Read the repository's current `AGENTS.md` completely.
2. Run `git status --short --branch`.
3. Record the current branch, `HEAD`, `origin/main`, and `upstream/main`.
4. Require the intended integration branch to be `main`. Stop and report an unexpected branch
   instead of silently switching it.
5. If the worktree is dirty, inspect all unstaged, staged, and untracked changes before doing
   anything else:
   - `git diff --stat` and `git diff`
   - `git diff --cached --stat` and `git diff --cached`
   - `git ls-files --others --exclude-standard`
6. Preserve pre-existing changes exactly. Never reset, restore, clean, checkout, or automatically
   stash them. If a safe merge cannot be proven, stop and ask for direction.

Do not push, open a pull request, write to live T3 data, launch a browser, or perform computer-use
verification unless the user explicitly requests it.

## Fetch and identify the integration range

1. Run `git fetch upstream --prune`.
2. Record the fetched `upstream/main` commit as the candidate tip.
3. Inspect both topology and commit contents:

   ```bash
   git rev-list --count main..upstream/main
   git rev-list --left-right --count main...upstream/main
   git log --reverse --oneline main..upstream/main
   git diff --stat main...upstream/main
   ```

4. If `main..upstream/main` is empty, do not create an empty merge. Report clearly that no new
   upstream commits exist, together with the live branch and divergence state.
5. If commits exist, summarize them by behavior and affected surface before merging. Inspect the
   actual diff and history; do not infer scope from commit subjects alone.

## Audit semantic overlap

Compare new upstream functionality against the protected fork behavior even when Git reports no
textual conflict. A clean merge can still leave duplicate routes, services, stores, RPCs, settings,
or UI entry points.

When upstream implements something already present in the fork:

1. Write down the observable behavior and invariants of both implementations.
2. Compare correctness, completeness, performance, remote readiness, supported clients and
   providers, persisted-state and wire compatibility, test coverage, simplicity, and ongoing
   maintenance cost.
3. Choose the better implementation as the base. Do not default to `ours`, `theirs`, or the fork.
4. If the better implementation already preserves all required functionality, use it and remove
   redundant parallel machinery where doing so is safe.
5. If the better implementation would lose required functionality, extend it minimally with the
   missing behavior. Prefer one coherent implementation over keeping two competing versions.
6. Preserve migrations, stored data, wire compatibility, and user-visible entry points unless a
   deliberate migration or behavior change is part of the decision.
7. Add or retain focused regression tests that prove both the chosen base and the required
   extensions.
8. Record the comparison, selected base, discarded duplication, and any extension in the final
   report.

Do not use line count or recency as a proxy for quality. If neither implementation is clearly
better or the choice requires a product-policy decision, stop and ask the user instead of guessing.

## Merge and resolve conflicts

Run:

```bash
git -c merge.renameLimit=20000 merge --no-edit upstream/main
```

For each conflict:

1. Inspect the merge base, local side, upstream side, surrounding callers, tests, and relevant
   history.
2. Resolve the behavior intentionally. Preserve upstream improvements and protected fork behavior
   through integration, replacement, or extension as established by the overlap audit.
3. Never resolve a conflicted file wholesale with `ours` or `theirs`.
4. Stage only files whose resolution has been reviewed.
5. If Git did not create the merge commit after all conflicts were resolved, finish it with the
   prepared merge message; do not invent an unrelated commit.

Do not rewrite either side's history and do not rebase this integration.

## Verify the integrated result

1. Review the final diff against both parents, including files that merged automatically.
2. Re-check every item in `references/protected-fork-behavior.md` and every newly integrated
   upstream feature that overlaps or touches it.
3. Run the smallest meaningful tests for changed or conflict-prone areas. Add targeted typechecks,
   lint, and formatting checks only for the affected scopes.
4. Do not run repository-wide checks or browser/computer-use verification without explicit
   permission.
5. Check for unresolved state and conflict markers:

   ```bash
   git diff --check
   git grep -n -E '^(<<<<<<< |=======|>>>>>>> )' -- . || true
   git status --short --branch
   ```

6. Fetch upstream again with `git fetch upstream --prune`. If `upstream/main` moved from the
   candidate tip, inspect and integrate the additional commits through the same workflow before
   claiming completion.
7. Require the final fetched upstream tip to be an ancestor of local `main`:

   ```bash
   git merge-base --is-ancestor upstream/main main
   ```

8. Document divergence with:

   ```bash
   git rev-list --left-right --count origin/main...main
   ```

9. If the worktree started clean, require it to finish clean. If it started dirty, prove and report
   that every pre-existing change remains intact and distinguish it from the merge result.

## Report the outcome

Include:

- the old and new upstream tips;
- every newly integrated upstream commit and a plain-language summary;
- the merge commit;
- conflicts and their semantic resolutions;
- all upstream-versus-fork overlap decisions;
- focused tests and results;
- an explicit assessment of protected fork behavior and affected upstream functionality;
- the final Git status and `origin/main` divergence;
- confirmation that the final `upstream/main` is an ancestor of `main`;
- confirmation that nothing was pushed and no pull request was created.
