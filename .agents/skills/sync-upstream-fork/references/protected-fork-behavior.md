# Protected Fork Behavior

Read this inventory before every upstream integration. Verify locations and implementation details
against the live tree and history because they can move over time.

## Required behavior

- A dedicated `/usage` route.
- A Usage button in the sidebar and the `mod+u` shortcut (`Command-U` on macOS).
- Provider-usage subscriptions and manual refresh.
- A download/update entry in the sidebar.
- Claude cross-account continuation and manual synchronization.
- A provider registry scoped per instance.
- Provider hydration that terminates only sessions affected by the changed provider state.
- The exact Claude Agent SDK pin `0.3.207`, unless a deliberate compatibility comparison proves a
  version change is safe and the resulting version remains intentionally pinned.
- Desktop notifications.
- The local macOS signing workflow.
- Worktree settings, naming, and hydration.
- Opus 5 support.

Treat this list as a behavioral contract, not ownership of particular files. When upstream ships an
overlapping feature, apply the comparison and selection rules in the parent skill. Retain the
better implementation and extend it when necessary to satisfy this contract.

## Historically high-risk integration seams

These are prior conflict outcomes, not permanent architecture. Re-evaluate them when upstream
changes the area:

- Desktop startup has retained both fork `ElectronNotification` behavior and upstream
  `ElectronPowerMonitor` behavior.
- Server settings hydration has retained the fork replay stream while also exposing a non-replaying
  PubSub for upstream `subscribeChanges`.
- RPC authorization is centralized in `apps/server/src/auth/RpcAuthorization.ts`. The fork RPC
  permissions have been:
  - `providerSyncThreadContinuation`: `operate`
  - `subscribeProviderUsage`: `read`
  - `refreshProviderUsage`: `operate`
- ChatView has retained fork Claude/session error handling alongside upstream shell-loading
  behavior.
- Settings has retained Claude and worktree functionality alongside upstream background-activity
  UI.
- Desktop packaging has combined local signing configuration with upstream packaging and resource
  monitoring.

If upstream replaces any of these mechanisms with a better coherent design, use that design as the
base and carry forward only the required behavior. Do not recreate a superseded mechanism merely to
match this historical file layout.
