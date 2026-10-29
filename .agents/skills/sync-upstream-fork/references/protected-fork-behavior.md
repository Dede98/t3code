# Protected Fork Behavior

Read this inventory before every upstream integration. Verify locations and implementation details
against the live tree and history because they can move over time.

## Required behavior

- A dedicated `/usage` route.
- A Usage button in the sidebar and the `mod+u` shortcut (`Command-U` on macOS).
- Provider-usage subscriptions and manual refresh.
- A download/update entry in the sidebar.
- Claude cross-account continuation and manual synchronization.
- The Labs alpha that moves a usage-limited thread to another same-driver account
  (`failoverLimitedThreads`, `apps/server/src/orchestration-v2/UsageLimitFailover.ts`, hooked into
  `UsageLimitRecoveryWorker`). Upstream's FAQ rejects an auto-switcher, so it never goes upstream.
  It also patches two upstream files: `ScheduledTaskService` asks `launchSelection` for the account
  of a new thread, and `followsCurrentConversation` in `Orchestrator.ts` calls `movedPastUsageLimit`
  (`UsageLimitQueueFollow.ts`). That second part applies regardless of the setting, so queued runs
  pinned to a limited account follow a manual Resume on another account too.
- Linked environments over Tailscale IPv4: `PeerLinks` accepts plain http to a 100.64.0.0/10
  address only while the local running Tailscale lists it (`readTailnetNodeAddresses` in
  `packages/tailscale`), re-checked before every token use. Server-config subscriptions re-send a
  snapshot when the machine's own addresses change (`DirectEndpoints.streamBoundEndpoints`,
  `withLateEditorConfig` in `ws.ts`). Drop it only if upstream ships an equivalent.
- Preview port discovery never connects to listeners it discovers; only configured preview URLs
  are probed (`PortScanner.ts`). This is a verbatim backport of upstream PR #26289 for issue
  #26279, because probes broke tests whose processes inherit `T3CODE_THREAD_ID`. Once upstream
  merges #26289 or another fix for #26279, take upstream's version.
- A provider registry scoped per instance.
- Provider hydration that terminates only sessions affected by the changed provider state.
- Claude Agent SDK compatibility with the native resume store: a null first main `load()` from a
  custom session store must still mean native resume. The fork follows upstream's SDK version
  (`0.3.276` as of 2026-09-21); before accepting a further bump, re-run the real-SDK server suites
  and the opt-in native memory test against the new version.
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
