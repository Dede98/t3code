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

## Run one autonomous task

For projects with GitHub task intake and provider routing already configured, open the project's
**Autonomous task** section. Select the checkout on the environment where the work should run.
On mobile, open **New Task**, choose the project's **Autonomous tasks** entry, and select its environment.

Enable task intake, select the next eligible task, review provider/model readiness and verification
checks, then choose **Run once**. Tasks follow issue-number order; selecting a later task explains
why it cannot start yet. Start blockers identify missing readiness or an existing run. Intake can
be disabled again when no run is active. This view does not configure GitHub intake or provider routing.
Starting runs and changing intake require an admin session on the selected environment. Standard
pairing and relay sessions can review saved runs; ask the environment administrator for an admin
pairing link to enable these actions.

The latest saved run shows Planning, Implementation, Verification, and any Repair with its own
subsequent Verification. Expand check results to inspect their exit codes and captured output.
Missing evidence never counts as verified success. Reloading or reconnecting resumes the saved
progress without starting work again.

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
