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

### Run an existing Epic

In **Autonomous tasks**, inspect an Epic by its GitHub issue number, review its tasks and start
blockers, then start the Epic. This version reads native GitHub Sub-Issues in their saved order and
native **blocked by** dependencies. It supports one level in the project's repository; nested or
cross-repository structures are blocked. Links and issue-description checklists do not define the
scope or grant execution approval. Each open task still needs the configured trusted ready label.
Already closed tasks are shown as externally closed, without a T3 verification claim.

Tasks run sequentially through the existing stages and bounded repair. Each next worktree starts
from the previous task's verified, accepted commit. The Epic succeeds only after the required
checks pass again on the combined result. Current mandatory-check execution requires a Codex
verification route; Planning and Implementation keep their configured routes and fallbacks.
Git file conversions must preserve the checked files when the accepted commit is checked out again.
Conversions that change those files block acceptance. Completion does not publish changes or
close or comment on GitHub issues.

Reloads and server restarts retain the same Epic run. Membership or dependency changes block it
instead of silently changing scope. Resolve a transient blocker and resume the saved run, or end
it and explicitly clear its target before returning to ordinary automation. A failed task's
exhausted repair requires inspection and ending that Epic; resuming does not erase or retry its
old execution. Turning automation off prevents additional task starts. Accepted commits, child
threads and checks remain available for review. A completed Epic never moves on to unrelated
issues automatically.

After successful common-result verification, choose **Review publication** in the Epic panel.
Review the repository, target branch and verified commit, then choose **Create Draft PR** to publish
that exact result for human review. This requires administrative access to the selected environment
and its GitHub credentials. Publication uses a branch dedicated to the Epic run and does not start
another model turn. Repository or target-branch changes can block publication until resolved.
If interrupted, retry the same handoff; T3 Code checks the existing branch and pull request first.
The saved PR remains available after reload and under previous Epics. A closed or merged PR is
never replaced automatically, and a publication failure retains the local result and its evidence.

### Queue approved Epics

Inspect each Epic and choose **Approve for queue**, then put waiting entries in the desired order.
The first approval keeps any already selected Epic at the front as active work. If ordinary task
automation is running, turn it off and finish that task before enabling the queue. Approval does not
turn Armed on. Queue edits and Armed changes require write access to the selected environment.

With Armed enabled, the server chooses the first eligible approved Epic. Each entry shows its
blockers; native GitHub dependencies and trusted task approvals still apply. A merge does not close
an issue or satisfy a GitHub dependency on an open issue. Waiting entries can be removed or moved;
an active entry cannot be replaced. If its membership or dependencies change, remove the waiting
entry and approve the current scope again.

After verification, explicitly publish the draft PR as above. The project waits for human review
and merge, checking GitHub about once a minute even with no connected client. A PR closed without
merge blocks continuation until it is reopened or its merge is confirmed. Before starting the next
Epic, T3 Code fetches the target branch and checks that it contains GitHub's merged result. GitHub,
fetch or repository-mapping errors retain the saved work and prevent starting on an old base.

Turning Armed off prevents further starts; turning it back on resumes from the saved queue.
An exhausted queue waits for more approvals instead of starting ordinary tasks. Completed runs,
verification evidence and PR links remain available. Pausing a queued Epic turns Armed off and preserves its execution for re-arm. A failed active Epic
holds its place for human inspection; queue editing does not discard or retry its execution.

To return to ordinary tasks, turn Armed off, wait for active work to finish, remove waiting entries,
and choose **Leave Epic queue**. This explicitly ends the selected Epic without claiming a merge;
its run history, checks and PR association remain available. You can later approve an Epic to begin
a new queue. During human review only the saved PR is polled; candidate eligibility is checked again
before starting the next Epic. If every waiting Epic is blocked, the server rechecks dependencies
every five minutes; editing the queue requests a fresh check.

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
