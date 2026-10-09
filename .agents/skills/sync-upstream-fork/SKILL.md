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

## Standing toolchain approvals

The user explicitly approved the following for future local upstream syncs on 2026-10-09.
These approvals supersede older notes requiring a new confirmation each time:

- Use Node 24 from `/opt/homebrew/opt/node@24/bin` for focused validation, including when the
  ambient runtime is Node 26. Prepend that directory to the command's `PATH` and unset
  `ELECTRON_RUN_AS_NODE`; verify the resulting Node version. No renewed approval is needed.
- When integration requires refreshed dependencies, run `vp i --frozen-lockfile` with that
  toolchain. If `vp` is not on `PATH`, use `node_modules/.bin/vp i --frozen-lockfile`.
  No renewed approval is needed. This does not authorize an unfrozen install or dependency upgrades.

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

After every sync, provide a German inline visual summary by default, without waiting for a
separate request. Lead with the most important behavioral changes, ordered by impact:

- **Upstream highlights:** Explain the most significant new features, fixes, and architectural
  changes, what changes in practice, and which clients or providers benefit. Base these on the
  inspected diff and history, not commit titles alone.
- **Impact on fork changes:** Highlight the most important changes to our custom behavior and
  implementation. For each affected area, explain what existed before, what upstream introduced,
  what the integrated result does, and why that solution was chosen. Explicitly distinguish fork
  behavior retained, adapted, replaced by an upstream equivalent, or still unresolved. Include
  significant semantic overlaps even when Git reported no conflict; do not merely say
  "all fork changes preserved" or list conflicted filenames.
- **Validation and remaining issues:** Show focused check results, pre-existing failures,
  unverified behavior, and any necessary user action separately. Do not sum overlapping test runs
  or imply live-product validation from unit tests.

Keep these highlights visible in the initial view. Put the complete commit list, detailed conflict
resolutions, and full verification evidence in tabs or expandable sections so they remain
accessible without burying the summary. Include a compact Git status overview and label its
snapshot time if later edits changed the worktree.

When T3 Code's `html_preview` and `html_render` tools are available, build a self-contained,
responsive HTML report, inspect it with `html_preview`, and publish it inline with `html_render`
before the final reply. This report is requested by default; it does not require another approval
or authorize browser verification of the live app. If inline rendering is unavailable, provide
the same highlights and evidence directly in chat as Markdown. A file link alone is insufficient.

The full report must include:

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
