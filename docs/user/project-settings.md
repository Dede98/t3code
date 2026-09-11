# Project settings

Open **Settings → Projects**. The project and machine pickers start at **All projects** and
**All machines**.

Change the default model, workspace, automatic pull, agent browser access, or actions for projects that inherit those values.
Select an individual project to override a default. Reset its row to inherit again. Changing a
default preserves explicit project overrides. Workspace preferences in `t3.json` take precedence
over machine defaults when the project has no explicit workspace override.

Select a machine to limit edits to it. **All machines** writes defaults to connected machines;
offline machines keep their previous values. Mixed values are indicated when selected machines
or checkouts disagree. Browser access changes apply when an agent session next starts.

Project grouping has a client-wide default across machines, with individual checkout overrides.
Shared actions apply to inheriting projects; editing a project's actions creates an independent list.
Reset that list to use shared actions again. Existing project actions are preserved.

Project names, icons, removal, and importing actions from a checkout remain project-specific.
When there are several checkouts, the checkout picker selects which actions and grouping to edit.

## Run autonomous tasks

Open the project's **Autonomous task** section. Select the checkout on the environment where the work should run.
On mobile, open **New Task**, choose the project's **Autonomous tasks** entry, and select its environment.

Use **Project setup** to configure GitHub intake, provider routing, and verification checks. The
checkout determines the GitHub repository: the server prefers `upstream`, then `origin`, then the
first other fetch remote by name. Review the detected repository and the saved intake binding;
there is no separate repository picker. Set ready and pause labels, trusted GitHub accounts, and
the poll interval, then save GitHub settings. The environment's existing GitHub login needs access
to a repository with Issues enabled. Import or refresh issues to see the intake result.

Choose existing provider instances and models from that environment. Project fallbacks and role
routes can override inherited defaults; reset an override to inherit again. Full access, when
explicitly enabled, applies only to Implementation and Repair. Add at least one required
verification check: its program runs with a list of separate arguments and a directory relative
to the task worktree on the selected environment. Choose its timeout and result format to match
the command. Saving or passing provider preflight does not execute checks.

GitHub and policy settings save separately. Each result reports which settings were saved; a
failed second save does not undo the first. Save or discard local edits before starting work.
After a revision conflict or reconnect with unsaved edits, reload saved settings before editing
again. Setup changes are unavailable during an active run because they could affect later stages.
Saving configuration never enables Run once or Armed.

Enable task intake, select the next eligible task, review provider/model readiness and verification
checks, then choose **Run once**. Tasks follow issue-number order; selecting a later task explains
why it cannot start yet. Start blockers identify missing readiness or an existing run. Intake can
be disabled again when no run is active.
Saving setup, starting runs, and changing intake require an admin session (`access:write`) on the
selected environment. Importing issues requires `orchestration:operate`, which standard pairing
sessions include. Standard pairing and relay sessions can review saved runs; ask the environment
administrator for an admin pairing link to save setup or control runs.

Choose **Turn on automation** to enable automatic mode (Armed) for this project and environment.
The server starts eligible tasks one after another, including tasks that become eligible later.
Automatic mode stays on while a task runs and while waiting for more work. Readiness and recorded
blockers explain why work cannot start; **Run once** remains available when automatic mode is off.

**Turn off automation** prevents new tasks from starting automatically and leaves intake enabled.
Work already admitted can continue, including later stages of the current task. Turning automation
off does not interrupt provider turns or guarantee that the task will finish. Threads, changes and
verification evidence remain available.
After any active run releases its resources and blockers are resolved, you can turn automation on
again. Previously attempted tasks cannot be restarted as fresh runs; remove their ready label or
pause them in GitHub before offering new eligible tasks.
If the project was paused, choose **End paused mode** to return to manual control, then enable
intake again. This does not resume the previous autonomous run.

The latest saved run shows Planning, Implementation, Verification, and any Repair with its own
subsequent Verification. Expand check results to inspect their exit codes and captured output.
Missing evidence never counts as verified success. Reloading or reconnecting resumes the saved
progress without starting work again.

For a run blocked before its first provider turn by an unavailable default remote reference,
**End blocked run** returns the project to task intake and prevents further
automatic steps in that run. This requires an admin session and does not mark the task successful or retry
the rejected operation. Fix the reported cause, remove the old issue's ready label (or pause it)
in GitHub, and wait for intake to show it as ineligible. Then start a new eligible issue with
**Run once**. A task with saved execution history cannot be started again as a fresh run; its
original failure and evidence remain available.

Open a stage's thread or changes to review the result. Web and desktop offer the available editor
or file-manager actions for its worktree. Mobile can copy the worktree path; its files remain on
the connected environment.

## Project icons

Choose an icon, emoji, or image from the project to make it easier to recognize. The choice applies
to selected checkouts in the project group and appears on connected clients. Choose **Automatic** to
let T3 Code detect an icon again.

## Keep the default branch current

Enable **Automatically pull** to keep the default-branch checkout up to date with its configured
upstream.

T3 Code only pulls when it can fast-forward and the checkout has no changed files, untracked files,
or local commits. It skips checkouts on another branch or without an upstream. If a checkout has
local work, resolve it yourself before automatic pulls can resume.
