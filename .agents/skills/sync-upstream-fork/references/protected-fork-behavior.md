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
  of a new thread, and `queuedRunFollowsCurrentConversation` in `Orchestrator.ts` adds
  `movedPastUsageLimit` (`UsageLimitQueueFollow.ts`) to upstream's `followsCurrentConversation`.
  That second part applies regardless of the setting, so queued runs pinned to a limited account
  follow a manual Resume on another account too. Upstream instead holds such a run until the
  limit resets (#22425); that hold still applies to runs queued on the limited account after the
  move. The failover resumes natively (upstream #36526) and says which account it moved to in
  the resume notification's `detail`.
- Sessions without a DPoP key stay signed in while they are used (`slides` and `renewIfDue` in
  `SessionStore.ts`): bearer pairings from clients that cannot renew, browser cookies, CLI-issued
  sessions of 30 days or more, and outside MCP clients. Peer links and DPoP pairing links use
  upstream's own renewal (#39399, #37980); do not add a second mechanism for those.
- Linked environments over Tailscale IPv4: `PeerLinks` accepts plain http to a 100.64.0.0/10
  address only while the local running Tailscale lists it (`readTailnetNodeAddresses` in
  `packages/tailscale`), re-checked before every token use. Server-config subscriptions re-send a
  snapshot when the machine's own addresses change (`DirectEndpoints.streamBoundEndpoints`,
  `withLateConfigDiscovery` in `ws.ts`). Drop it only if upstream ships an equivalent.
- Agent threads shelf: a thread an agent starts through `t3_thread_launch` or `create_threads`
  records its starter in `lineage.launchedByThreadId` (set by the decider from a server-only
  `thread.create` field that `withCreationProvenance` strips). It must stay out of
  `isListedThreadLineage`, `parentThreadId` and `delegatedFrom`, which change push, auto-settle
  and MCP output. Clients read it through `isAgentStartedThread` (client-runtime `threadShell.ts`):
  web shelves such threads in the sidebar's `agent` section, mobile in `v2-agent-shelf`. Pinned
  ones are exempt. The same change made the web Pinned section collapsible and its cards compact
  (`compact` on `SidebarThreadRow`).
- A provider registry scoped per instance.
- Provider hydration that terminates only sessions affected by the changed provider state.
- Claude Agent SDK compatibility with the native resume store: a null first main `load()` from a
  custom session store must still mean native resume. The fork follows upstream's SDK version
  (`0.3.321` as of 2026-11-10); before accepting a further bump, re-run the real-SDK server suites
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
- `ScheduledTaskService.patch` (MCP `updateScheduledTask` partial edits, compare-and-swap) is
  fork-only. It must apply the same refusals as upstream `upsert` and `setEnabled` (managed
  policy, one-time schedules). Run completion follows upstream's single write transaction.
- Desktop packaging has combined local signing configuration with upstream packaging and resource
  monitoring.
- Migration lineages (`Migrations.ts`): the fork's `ClaudeSessionStore` slot shifts upstream ids by
  one. Upstream migrations after 080 go in `laterUpstreamMigrationEntries` only (one line per
  migration); every lineage appends them shifted, and `upstreamHeadMigrationEntries` adopts official
  databases at any later head. Upstream's preflight (`skippedV2Migrations`, `hasPendingMigrations`,
  `servicePreflight.ts`) must check the selected lineage, never `migrationManifest`.
- The web sidebar's sections are enumerated in several places that must agree when upstream adds
  or reorders one: `partitionSidebarThreads` and the list items in `Sidebar.tsx`,
  `sectionAtSidebarSlot` and `resolveSidebarDropTarget` in `Sidebar.logic.ts`, the projection in
  `Sidebar.drag.ts` (which also sizes pinned rows separately from cards), and mobile's
  `buildThreadListV2Items` with its header-index slicing.
- Upstream requires every web component to compile under the React Compiler
  (`reactCompilerCoverage.test.ts`), every UI string to be wrapped for translation
  (`t3code/no-untranslated-copy`, `vp run i18n:generate`, `i18n:check`), and every MCP tool that
  acts as its calling thread to be hidden from outside clients (`McpToolAccess.routed`). Fork
  components, copy, and tools (`html_export`) must follow.

If upstream replaces any of these mechanisms with a better coherent design, use that design as the
base and carry forward only the required behavior. Do not recreate a superseded mechanism merely to
match this historical file layout.
