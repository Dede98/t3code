import * as Migrator from "effect/sql/Migrator";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  DATABASE_MIGRATION_PROGRESS_MESSAGE,
  DATABASE_MIGRATIONS_FINISHED_MESSAGE,
  DATABASE_MIGRATIONS_STARTED_MESSAGE,
} from "@t3tools/shared/databaseMigrationLog";

import Migration0001 from "./Migrations/001_OrchestrationEvents.ts";
import Migration0002 from "./Migrations/002_OrchestrationCommandReceipts.ts";
import Migration0003 from "./Migrations/003_CheckpointDiffBlobs.ts";
import Migration0004 from "./Migrations/004_ProviderSessionRuntime.ts";
import Migration0005 from "./Migrations/005_Projections.ts";
import Migration0006 from "./Migrations/006_ProjectionThreadSessionRuntimeModeColumns.ts";
import Migration0007 from "./Migrations/007_ProjectionThreadMessageAttachments.ts";
import Migration0008 from "./Migrations/008_ProjectionThreadActivitySequence.ts";
import Migration0009 from "./Migrations/009_ProviderSessionRuntimeMode.ts";
import Migration0010 from "./Migrations/010_ProjectionThreadsRuntimeMode.ts";
import Migration0011 from "./Migrations/011_OrchestrationThreadCreatedRuntimeMode.ts";
import Migration0012 from "./Migrations/012_ProjectionThreadsInteractionMode.ts";
import Migration0013 from "./Migrations/013_ProjectionThreadProposedPlans.ts";
import Migration0014 from "./Migrations/014_ProjectionThreadProposedPlanImplementation.ts";
import Migration0015 from "./Migrations/015_ProjectionTurnsSourceProposedPlan.ts";
import Migration0016 from "./Migrations/016_CanonicalizeModelSelections.ts";
import Migration0017 from "./Migrations/017_ProjectionThreadsArchivedAt.ts";
import Migration0018 from "./Migrations/018_ProjectionThreadsArchivedAtIndex.ts";
import Migration0019 from "./Migrations/019_ProjectionSnapshotLookupIndexes.ts";
import Migration0020 from "./Migrations/020_AuthAccessManagement.ts";
import Migration0021 from "./Migrations/021_AuthSessionClientMetadata.ts";
import Migration0022 from "./Migrations/022_AuthSessionLastConnectedAt.ts";
import Migration0023 from "./Migrations/023_ProjectionThreadShellSummary.ts";
import Migration0024 from "./Migrations/024_BackfillProjectionThreadShellSummary.ts";
import Migration0025 from "./Migrations/025_CleanupInvalidProjectionPendingApprovals.ts";
import Migration0026 from "./Migrations/026_CanonicalizeModelSelectionOptions.ts";
import Migration0027 from "./Migrations/027_ProviderSessionRuntimeInstanceId.ts";
import Migration0028 from "./Migrations/028_ProjectionThreadSessionInstanceId.ts";
import Migration0029 from "./Migrations/029_ProjectionThreadDetailOrderingIndexes.ts";
import Migration0030 from "./Migrations/030_ProjectionThreadShellArchiveIndexes.ts";
import Migration0031 from "./Migrations/031_AuthAuthorizationScopes.ts";
import Migration0032 from "./Migrations/032_AuthPairingProofKeyThumbprint.ts";
import Migration0033 from "./Migrations/034_ProjectionThreadsSettled.ts";
import Migration0034 from "./Migrations/035_ProjectionThreadsSnoozed.ts";
import Migration0035 from "./Migrations/036_ProjectionThreadTitleRegeneration.ts";
import Migration0036 from "./Migrations/037_ProjectionThreadsPinned.ts";
import Migration0037 from "./Migrations/038_ProjectionTurnsKeysetIndex.ts";
import Migration0038 from "./Migrations/039_ProjectionThreadsPinOrderKey.ts";
import Migration0039 from "./Migrations/040_ProjectionProjectsDefaultThreadEnvMode.ts";
import Migration0040 from "./Migrations/041_ProjectionProjectFaviconPath.ts";
import Migration0041 from "./Migrations/042_AuthSessionClientConnection.ts";
import Migration0042 from "./Migrations/043_ProjectionThreadLinkedPullRequest.ts";
import Migration0043 from "./Migrations/044_ProjectionThreadsUnsettledAt.ts";
import Migration0044 from "./Migrations/045_ClearAutomaticProjectModelDefaults.ts";
import Migration0045 from "./Migrations/046_ProjectionProjectsAutoPull.ts";
import Migration0046 from "./Migrations/047_RepairAutomaticSettlementTimestamps.ts";
import Migration0047 from "./Migrations/048_ProjectionProjectIcon.ts";
import Migration0048 from "./Migrations/049_ProjectionThreadBranchPullRequest.ts";
import Migration0049 from "./Migrations/050_ProjectionThreadsActiveOrderKey.ts";
import Migration0050 from "./Migrations/051_ProjectionThreadPullRequests.ts";
import Migration0051 from "./Migrations/052_ProjectionThreadMessageContext.ts";
import Migration0052 from "./Migrations/053_ProjectionThreadTitleState.ts";
import Migration0053 from "./Migrations/054_PullRequestFilesViewed.ts";
import ClaudeSessionStore from "./Migrations/033_ClaudeSessionStore.ts";
import Migration0054 from "./Migrations/055_ProjectionThreadsAutoSettleDisabledAt.ts";
import Migration0055 from "./Migrations/055_OrchestrationV2.ts";
import Migration0056 from "./Migrations/056_RemoveRedundantProjectionIndexes.ts";
import Migration0057 from "./Migrations/057_ScheduledTaskWebhooks.ts";
import Migration0058 from "./Migrations/058_WebhookRelayDeliveries.ts";
import Migration0059 from "./Migrations/059_McpAppModelContext.ts";
import Migration0060 from "./Migrations/060_ThreadSnapshotWindowIndexes.ts";
import Migration0061 from "./Migrations/061_PullRequestReviewGuides.ts";
import Migration0062 from "./Migrations/062_ThreadFailureLookupIndex.ts";
import Migration0063 from "./Migrations/063_DropUnusedV1Projections.ts";
import Migration0064 from "./Migrations/064_CanonicalizeThreadPullRequestLinks.ts";
import Migration0065 from "./Migrations/065_CodexUsageNativeTurnIndex.ts";
import Migration0066 from "./Migrations/066_RepairClientSettlementTimestamps.ts";
import Migration0067 from "./Migrations/067_WebhookDedupe.ts";
import Migration0068 from "./Migrations/068_ShellStopRequestIndex.ts";
import Migration0069 from "./Migrations/069_ThreadMessageSearch.ts";
import Migration0070 from "./Migrations/070_PeerLinks.ts";
import Migration0071 from "./Migrations/071_PeerLinkFence.ts";
import Migration0072 from "./Migrations/072_WaitingLinkRequestIndex.ts";
import Migration0073 from "./Migrations/073_ActiveTurnItemIndexes.ts";
import Migration0074 from "./Migrations/074_ExcludeAttachmentPlaceholdersFromSearch.ts";
import Migration0075 from "./Migrations/075_SubagentDeliveryState.ts";
import Migration0076 from "./Migrations/076_ProjectionProjectsPinArchive.ts";
import Migration0077 from "./Migrations/077_RepairBooleanSettledOverride.ts";
import Migration0078 from "./Migrations/078_DropOrchestrationEventCorrelationIndex.ts";
import Migration0079 from "./Migrations/079_DelegatedRecoveryIndexes.ts";
import Migration0080 from "./Migrations/080_ScheduledTaskThreadMode.ts";
import Migration0081 from "./Migrations/081_ProjectionMessageSearchText.ts";
import Migration0082 from "./Migrations/082_TurnItemConversationIndex.ts";
import Migration0083 from "./Migrations/083_CommandReadIndexes.ts";
import Migration0084 from "./Migrations/084_LastExecutedRunIndex.ts";
import Migration0085 from "./Migrations/085_TurnItemCounts.ts";
import Migration0086 from "./Migrations/086_QueuedTurnInputIndex.ts";
import Migration0087 from "./Migrations/087_ProjectionProjectsCustomTitle.ts";
import Migration0088 from "./Migrations/088_TurnItemTypeStatusIndex.ts";
import Migration0089 from "./Migrations/089_ShellThreadReadIndexes.ts";
import Migration0090 from "./Migrations/090_AuthConnectDevices.ts";
import Migration0091 from "./Migrations/091_AuthConnectDeniedRequests.ts";
import Migration0092 from "./Migrations/092_QueuedTurnInputIndexJsonValid.ts";
import Migration0093 from "./Migrations/093_ProviderTurnAttemptIndex.ts";
import Migration0094 from "./Migrations/094_TurnItemMessageOrdinalIndex.ts";
import Migration0095 from "./Migrations/095_TurnItemInlineWorkOutput.ts";
import Migration0096 from "./Migrations/096_RepairStaleTitleRegeneration.ts";
import Migration0097 from "./Migrations/097_IssueTrackerBindings.ts";
import Migration0098 from "./Migrations/098_ActiveShellIndexIndexes.ts";
import Migration0099 from "./Migrations/099_BackgroundRecoveryIndexes.ts";
import Migration0100 from "./Migrations/100_TurnStartReadIndexes.ts";
import Migration0101 from "./Migrations/101_DelegatedTaskReadIndexes.ts";
import Migration0102 from "./Migrations/102_ThreadItemCounts.ts";
import Migration0103 from "./Migrations/103_IssueTrackerAdditionalScopes.ts";
import Migration0104 from "./Migrations/104_ProjectionProjectsTrackedFolders.ts";
import Migration0105 from "./Migrations/105_TaskFolders.ts";
import Migration0106 from "./Migrations/106_WorktreePathKeyIndex.ts";
import Migration0107 from "./Migrations/107_DeferredSlashCommandIndex.ts";

// Upstream migrations after 080, at upstream ids. A sync adds new ones here only: every
// lineage below appends them one id later, because ClaudeSessionStore took a slot before them.
const laterUpstreamMigrationEntries = [
  [81, "ProjectionMessageSearchText", Migration0081],
  [82, "TurnItemConversationIndex", Migration0082],
  [83, "CommandReadIndexes", Migration0083],
  [84, "LastExecutedRunIndex", Migration0084],
  [85, "TurnItemCounts", Migration0085],
  [86, "QueuedTurnInputIndex", Migration0086],
  [87, "ProjectionProjectsCustomTitle", Migration0087],
  [88, "TurnItemTypeStatusIndex", Migration0088],
  [89, "ShellThreadReadIndexes", Migration0089],
  [90, "AuthConnectDevices", Migration0090],
  [91, "AuthConnectDeniedRequests", Migration0091],
  [92, "QueuedTurnInputIndexJsonValid", Migration0092],
  [93, "ProviderTurnAttemptIndex", Migration0093],
  [94, "TurnItemMessageOrdinalIndex", Migration0094],
  [95, "TurnItemInlineWorkOutput", Migration0095],
  [96, "RepairStaleTitleRegeneration", Migration0096],
  [97, "IssueTrackerBindings", Migration0097],
  [98, "ActiveShellIndexIndexes", Migration0098],
  [99, "BackgroundRecoveryIndexes", Migration0099],
  [100, "TurnStartReadIndexes", Migration0100],
  [101, "DelegatedTaskReadIndexes", Migration0101],
  [102, "ThreadItemCounts", Migration0102],
  [103, "IssueTrackerAdditionalScopes", Migration0103],
  [104, "ProjectionProjectsTrackedFolders", Migration0104],
  [105, "TaskFolders", Migration0105],
  [106, "WorktreePathKeyIndex", Migration0106],
  [107, "DeferredSlashCommandIndex", Migration0107],
] as const;

const shiftedLaterMigrationEntries = laterUpstreamMigrationEntries.map(
  ([id, name, migration]) => [id + 1, name, migration] as const,
);

export const migrationEntries = [
  [1, "OrchestrationEvents", Migration0001],
  [2, "OrchestrationCommandReceipts", Migration0002],
  [3, "CheckpointDiffBlobs", Migration0003],
  [4, "ProviderSessionRuntime", Migration0004],
  [5, "Projections", Migration0005],
  [6, "ProjectionThreadSessionRuntimeModeColumns", Migration0006],
  [7, "ProjectionThreadMessageAttachments", Migration0007],
  [8, "ProjectionThreadActivitySequence", Migration0008],
  [9, "ProviderSessionRuntimeMode", Migration0009],
  [10, "ProjectionThreadsRuntimeMode", Migration0010],
  [11, "OrchestrationThreadCreatedRuntimeMode", Migration0011],
  [12, "ProjectionThreadsInteractionMode", Migration0012],
  [13, "ProjectionThreadProposedPlans", Migration0013],
  [14, "ProjectionThreadProposedPlanImplementation", Migration0014],
  [15, "ProjectionTurnsSourceProposedPlan", Migration0015],
  [16, "CanonicalizeModelSelections", Migration0016],
  [17, "ProjectionThreadsArchivedAt", Migration0017],
  [18, "ProjectionThreadsArchivedAtIndex", Migration0018],
  [19, "ProjectionSnapshotLookupIndexes", Migration0019],
  [20, "AuthAccessManagement", Migration0020],
  [21, "AuthSessionClientMetadata", Migration0021],
  [22, "AuthSessionLastConnectedAt", Migration0022],
  [23, "ProjectionThreadShellSummary", Migration0023],
  [24, "BackfillProjectionThreadShellSummary", Migration0024],
  [25, "CleanupInvalidProjectionPendingApprovals", Migration0025],
  [26, "CanonicalizeModelSelectionOptions", Migration0026],
  [27, "ProviderSessionRuntimeInstanceId", Migration0027],
  [28, "ProjectionThreadSessionInstanceId", Migration0028],
  [29, "ProjectionThreadDetailOrderingIndexes", Migration0029],
  [30, "ProjectionThreadShellArchiveIndexes", Migration0030],
  [31, "AuthAuthorizationScopes", Migration0031],
  [32, "AuthPairingProofKeyThumbprint", Migration0032],
  [33, "ProjectionThreadsSettled", Migration0033],
  [34, "ProjectionThreadsSnoozed", Migration0034],
  [35, "ProjectionThreadTitleRegeneration", Migration0035],
  [36, "ProjectionThreadsPinned", Migration0036],
  [37, "ProjectionTurnsKeysetIndex", Migration0037],
  [38, "ProjectionThreadsPinOrderKey", Migration0038],
  [39, "ProjectionProjectsDefaultThreadEnvMode", Migration0039],
  [40, "ProjectionProjectFaviconPath", Migration0040],
  [41, "AuthSessionClientConnection", Migration0041],
  [42, "ProjectionThreadLinkedPullRequest", Migration0042],
  [43, "ProjectionThreadsUnsettledAt", Migration0043],
  [44, "ClearAutomaticProjectModelDefaults", Migration0044],
  [45, "ProjectionProjectsAutoPull", Migration0045],
  [46, "RepairAutomaticSettlementTimestamps", Migration0046],
  [47, "ProjectionProjectIcon", Migration0047],
  [48, "ProjectionThreadBranchPullRequest", Migration0048],
  [49, "ProjectionThreadsActiveOrderKey", Migration0049],
  [50, "ProjectionThreadPullRequests", Migration0050],
  [51, "ProjectionThreadMessageContext", Migration0051],
  [52, "ProjectionThreadTitleState", Migration0052],
  [53, "PullRequestFilesViewed", Migration0053],
  [54, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  // Preserve this migration's schema. Future V2 schema changes need new migrations.
  [55, "OrchestrationV2", Migration0055],
  [56, "RemoveRedundantProjectionIndexes", Migration0056],
  [57, "ClaudeSessionStore", ClaudeSessionStore],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// These are ledger lineages, not aliases for migration numbers. Once applied,
// an ID/name pair never changes. Each lineage appends only its missing work.
const forkV1Entries = [
  ...migrationEntries.filter(([id]) => id < 33),
  [33, "ClaudeSessionStore", ClaudeSessionStore],
  ...migrationEntries
    .filter(([id]) => id >= 33 && id <= 53)
    .map(([id, name, migration]) => [id + 1, name, migration] as const),
] as const;

export const forkMigrationEntries = [
  ...forkV1Entries,
  [55, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  [56, "ForkOrchestrationV2", Migration0055],
  [57, "RemoveRedundantProjectionIndexes", Migration0056],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

export const forkIntegrationMigrationEntries = [
  ...forkV1Entries,
  [55, "ForkOrchestrationV2", Migration0055],
  [56, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  [57, "RemoveRedundantProjectionIndexes", Migration0056],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamIntegrationMigrationEntries = [
  ...migrationEntries.filter(([id]) => id <= 53),
  [54, "OrchestrationV2", Migration0055],
  [55, "ClaudeSessionStore", ClaudeSessionStore],
  [56, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  [57, "RemoveRedundantProjectionIndexes", Migration0056],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const preview54MigrationEntries = [
  ...migrationEntries.filter(([id]) => id <= 53),
  [54, "OrchestrationV2", Migration0055],
  [55, "RemoveRedundantProjectionIndexes", Migration0056],
  [56, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  [57, "ClaudeSessionStore", ClaudeSessionStore],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const preview53MigrationEntries = [
  ...migrationEntries.filter(([id]) => id <= 52),
  [53, "OrchestrationV2", Migration0055],
  [54, "PullRequestFilesViewed", Migration0053],
  [55, "RemoveRedundantProjectionIndexes", Migration0056],
  [56, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  [57, "ClaudeSessionStore", ClaudeSessionStore],
  [58, "ScheduledTaskWebhooks", Migration0057],
  [59, "WebhookRelayDeliveries", Migration0058],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamWebhookMigrationEntries = [
  ...migrationEntries.filter(([id]) => id <= 56),
  [57, "ScheduledTaskWebhooks", Migration0057],
  [58, "WebhookRelayDeliveries", Migration0058],
  [59, "ClaudeSessionStore", ClaudeSessionStore],
  [60, "McpAppModelContext", Migration0059],
  [61, "ThreadSnapshotWindowIndexes", Migration0060],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have the app migrations at 59/60.
// Keep those records and append the fork's session store after them.
const upstreamMcpAppMigrationEntries = [
  ...upstreamWebhookMigrationEntries.filter(([id]) => id <= 58),
  [59, "McpAppModelContext", Migration0059],
  [60, "ThreadSnapshotWindowIndexes", Migration0060],
  [61, "ClaudeSessionStore", ClaudeSessionStore],
  [62, "PullRequestReviewGuides", Migration0061],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Upstream databases that already applied review guides keep that id/name pair.
const upstreamReviewGuideMigrationEntries = [
  ...upstreamMcpAppMigrationEntries.filter(([id]) => id <= 60),
  [61, "PullRequestReviewGuides", Migration0061],
  [62, "ClaudeSessionStore", ClaudeSessionStore],
  [63, "ThreadFailureLookupIndex", Migration0062],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have the failure index at 62.
// Preserve it and append only the missing native Claude session store.
const upstreamFailureIndexMigrationEntries = [
  ...upstreamReviewGuideMigrationEntries.filter(([id]) => id <= 61),
  [62, "ThreadFailureLookupIndex", Migration0062],
  [63, "ClaudeSessionStore", ClaudeSessionStore],
  [64, "DropUnusedV1Projections", Migration0063],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official databases may have used 63/64 before ever opening in the fork.
// Append the Claude store without changing an already recorded id/name pair.
const upstreamDropProjectionsMigrationEntries = [
  ...upstreamFailureIndexMigrationEntries.filter(([id]) => id <= 62),
  [63, "DropUnusedV1Projections", Migration0063],
  [64, "ClaudeSessionStore", ClaudeSessionStore],
  [65, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamCanonicalLinksMigrationEntries = [
  ...upstreamDropProjectionsMigrationEntries.filter(([id]) => id <= 63),
  [64, "CanonicalizeThreadPullRequestLinks", Migration0064],
  [65, "ClaudeSessionStore", ClaudeSessionStore],
  [66, "CodexUsageNativeTurnIndex", Migration0065],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamUsageIndexMigrationEntries = [
  ...upstreamCanonicalLinksMigrationEntries.filter(([id]) => id <= 64),
  [65, "CodexUsageNativeTurnIndex", Migration0065],
  [66, "ClaudeSessionStore", ClaudeSessionStore],
  [67, "RepairClientSettlementTimestamps", Migration0066],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamSettlementMigrationEntries = [
  ...upstreamUsageIndexMigrationEntries.filter(([id]) => id <= 65),
  [66, "RepairClientSettlementTimestamps", Migration0066],
  [67, "ClaudeSessionStore", ClaudeSessionStore],
  [68, "WebhookDedupe", Migration0067],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have webhook dedupe at 67.
const upstreamWebhookDedupeMigrationEntries = [
  ...upstreamSettlementMigrationEntries.filter(([id]) => id <= 66),
  [67, "WebhookDedupe", Migration0067],
  [68, "ClaudeSessionStore", ClaudeSessionStore],
  [69, "ShellStopRequestIndex", Migration0068],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have the shell stop index at 68.
const upstreamShellStopIndexMigrationEntries = [
  ...upstreamWebhookDedupeMigrationEntries.filter(([id]) => id <= 67),
  [68, "ShellStopRequestIndex", Migration0068],
  [69, "ClaudeSessionStore", ClaudeSessionStore],
  [70, "ThreadMessageSearch", Migration0069],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have message search at 69.
const upstreamThreadMessageSearchMigrationEntries = [
  ...upstreamShellStopIndexMigrationEntries.filter(([id]) => id <= 68),
  [69, "ThreadMessageSearch", Migration0069],
  [70, "ClaudeSessionStore", ClaudeSessionStore],
  [71, "PeerLinks", Migration0070],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Keep official peer-link ledger entries and append the missing Claude store.
const upstreamPeerLinksMigrationEntries = [
  ...upstreamThreadMessageSearchMigrationEntries.filter(([id]) => id <= 69),
  [70, "PeerLinks", Migration0070],
  [71, "ClaudeSessionStore", ClaudeSessionStore],
  [72, "PeerLinkFence", Migration0071],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamPeerLinkFenceMigrationEntries = [
  ...upstreamPeerLinksMigrationEntries.filter(([id]) => id <= 70),
  [71, "PeerLinkFence", Migration0071],
  [72, "ClaudeSessionStore", ClaudeSessionStore],
  [73, "WaitingLinkRequestIndex", Migration0072],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamWaitingLinkRequestMigrationEntries = [
  ...upstreamPeerLinkFenceMigrationEntries.filter(([id]) => id <= 71),
  [72, "WaitingLinkRequestIndex", Migration0072],
  [73, "ClaudeSessionStore", ClaudeSessionStore],
  [74, "ActiveTurnItemIndexes", Migration0073],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamActiveTurnItemIndexesMigrationEntries = [
  ...upstreamWaitingLinkRequestMigrationEntries.filter(([id]) => id <= 72),
  [73, "ActiveTurnItemIndexes", Migration0073],
  [74, "ClaudeSessionStore", ClaudeSessionStore],
  [75, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have 074-077 before opening in the fork.
const upstreamAttachmentSearchMigrationEntries = [
  ...upstreamActiveTurnItemIndexesMigrationEntries.filter(([id]) => id <= 73),
  [74, "ExcludeAttachmentPlaceholdersFromSearch", Migration0074],
  [75, "ClaudeSessionStore", ClaudeSessionStore],
  [76, "SubagentDeliveryState", Migration0075],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamSubagentDeliveryMigrationEntries = [
  ...upstreamAttachmentSearchMigrationEntries.filter(([id]) => id <= 74),
  [75, "SubagentDeliveryState", Migration0075],
  [76, "ClaudeSessionStore", ClaudeSessionStore],
  [77, "ProjectionProjectsPinArchive", Migration0076],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamProjectPinArchiveMigrationEntries = [
  ...upstreamSubagentDeliveryMigrationEntries.filter(([id]) => id <= 75),
  [76, "ProjectionProjectsPinArchive", Migration0076],
  [77, "ClaudeSessionStore", ClaudeSessionStore],
  [78, "RepairBooleanSettledOverride", Migration0077],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamSettledOverrideRepairMigrationEntries = [
  ...upstreamProjectPinArchiveMigrationEntries.filter(([id]) => id <= 76),
  [77, "RepairBooleanSettledOverride", Migration0077],
  [78, "ClaudeSessionStore", ClaudeSessionStore],
  [79, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

const upstreamDropCorrelationIndexMigrationEntries = [
  ...upstreamSettledOverrideRepairMigrationEntries.filter(([id]) => id <= 77),
  [78, "DropOrchestrationEventCorrelationIndex", Migration0078],
  [79, "ClaudeSessionStore", ClaudeSessionStore],
  [80, "DelegatedRecoveryIndexes", Migration0079],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have delegated recovery indexes at 79.
const upstreamDelegatedRecoveryIndexesMigrationEntries = [
  ...upstreamDropCorrelationIndexMigrationEntries.filter(([id]) => id <= 78),
  [79, "DelegatedRecoveryIndexes", Migration0079],
  [80, "ClaudeSessionStore", ClaudeSessionStore],
  [81, "ScheduledTaskThreadMode", Migration0080],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases may already have the scheduled task thread mode at 80.
const upstreamScheduledTaskThreadModeMigrationEntries = [
  ...upstreamDelegatedRecoveryIndexesMigrationEntries.filter(([id]) => id <= 79),
  [80, "ScheduledTaskThreadMode", Migration0080],
  [81, "ClaudeSessionStore", ClaudeSessionStore],
  ...shiftedLaterMigrationEntries,
] as const;

// Official upstream databases at head 81 or later keep their ids up to that head. The Claude
// store follows it, and the rest of the later migrations run one id after their upstream id.
const upstreamHeadMigrationEntries = (head: number) => [
  ...upstreamScheduledTaskThreadModeMigrationEntries.filter(([id]) => id <= 80),
  ...laterUpstreamMigrationEntries.filter(([id]) => id <= head),
  [head + 1, "ClaudeSessionStore", ClaudeSessionStore] as const,
  ...shiftedLaterMigrationEntries.filter(([id]) => id > head + 1),
];

export const migrationManifest = migrationEntries.map(([id, name]) => [id, name] as const);

type MigrationEntry = readonly [number, string, (typeof migrationEntries)[number][2]];

const makeMigrationLoader = (entries: ReadonlyArray<MigrationEntry>, throughId?: number) =>
  Migrator.fromRecord(
    Object.fromEntries(
      entries
        .filter(([id]) => throughId === undefined || id <= throughId)
        .map(([id, name, migration]) => [`${id}_${name}`, migration]),
    ),
  );

// V2's schema migrations start at 55 after the legacy schema migrations. Fork
// and preview lineages record other names there, but every lineage needs each
// of its ids from 55 on.
const FIRST_V2_MIGRATION_ID = 55;

type RecordedMigration = { readonly id: number; readonly name: string };

/**
 * The selected lineage's migrations from V2 on that the migrator would skip: it skips every id
 * up to the highest recorded one, whatever name was recorded. Earlier ids only get the warning
 * in runMigrations; databases with a site-local migration there still work.
 */
export function skippedV2Migrations(
  recorded: ReadonlyArray<RecordedMigration>,
  throughId?: number,
): string[] {
  const latestRecordedId = Math.max(0, ...recorded.map((row) => row.id));
  const recordedNames = new Map(recorded.map((row) => [row.id, row.name]));
  return selectMigrationEntries(recorded).flatMap(([id, name]) => {
    if (
      id < FIRST_V2_MIGRATION_ID ||
      id > latestRecordedId ||
      (throughId !== undefined && id > throughId)
    )
      return [];
    const recordedName = recordedNames.get(id);
    return recordedName === name ? [] : [`${id}_${name} (recorded: ${recordedName ?? "nothing"})`];
  });
}

// Migrators skip ids up to the highest recorded; reject collisions that would skip V2 schema.
const assertV2MigrationsCanRun = Effect.fn("assertV2MigrationsCanRun")(function* (
  recorded: ReadonlyArray<RecordedMigration>,
  throughId?: number,
) {
  const skipped = skippedV2Migrations(recorded, throughId);
  if (skipped.length === 0) return;
  const latestRecordedId = Math.max(0, ...recorded.map((row) => row.id));
  return yield* new Migrator.MigrationError({
    kind: "BadState",
    message: `This database's migration history would skip required schema migrations: ${skipped.join(", ")}. The migrator skips every id up to the highest recorded id (${latestRecordedId}), regardless of name. It was probably written by a different build. Start the build that wrote it, or report this migration history so an upgrade path can be added.`,
  });
});

const latestMigrationId = Math.max(...migrationEntries.map(([id]) => id));

// Mirrors the Migrator, which runs only ids above the highest recorded. Every lineage ends at
// the same id, and preview ledgers are selected as lineages instead of being rewritten.
export const hasPendingMigrations = Effect.fn("hasPendingMigrations")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
  `;
  if (tables.length === 0) return true;
  const rows = yield* sql<{ readonly latestId: number | null }>`
    SELECT MAX(migration_id) AS latestId FROM effect_sql_migrations
  `;
  return (rows[0]?.latestId ?? 0) < latestMigrationId;
});

const run = Migrator.make({});

export interface RunMigrationsOptions {
  readonly toMigrationInclusive?: number | undefined;
}

// An official ledger past 80 matches the later upstream ids up to its head, where the fork's
// Claude store then follows. Ids there are contiguous from 81, so the first miss gives the head.
function selectLaterUpstreamMigrationEntries(has: (id: number, name: string) => boolean) {
  const missing = laterUpstreamMigrationEntries.findIndex(([id, name]) => !has(id, name));
  const head = 80 + (missing === -1 ? laterUpstreamMigrationEntries.length : missing);
  return head === 80
    ? upstreamScheduledTaskThreadModeMigrationEntries
    : upstreamHeadMigrationEntries(head);
}

function selectMigrationEntries(
  recorded: ReadonlyArray<RecordedMigration>,
): ReadonlyArray<MigrationEntry> {
  const has = (id: number, name: string) =>
    recorded.some((row) => row.id === id && row.name === name);
  return has(33, "ClaudeSessionStore")
    ? has(55, "ForkOrchestrationV2")
      ? forkIntegrationMigrationEntries
      : forkMigrationEntries
    : has(55, "ClaudeSessionStore")
      ? upstreamIntegrationMigrationEntries
      : has(53, "OrchestrationV2")
        ? preview53MigrationEntries
        : has(54, "OrchestrationV2")
          ? preview54MigrationEntries
          : has(59, "McpAppModelContext")
            ? has(61, "PullRequestReviewGuides")
              ? has(62, "ThreadFailureLookupIndex")
                ? has(63, "DropUnusedV1Projections")
                  ? has(64, "CanonicalizeThreadPullRequestLinks")
                    ? has(65, "CodexUsageNativeTurnIndex")
                      ? has(66, "RepairClientSettlementTimestamps")
                        ? has(67, "WebhookDedupe")
                          ? has(68, "ShellStopRequestIndex")
                            ? has(69, "ThreadMessageSearch")
                              ? has(70, "PeerLinks")
                                ? has(71, "PeerLinkFence")
                                  ? has(72, "WaitingLinkRequestIndex")
                                    ? has(73, "ActiveTurnItemIndexes")
                                      ? has(74, "ExcludeAttachmentPlaceholdersFromSearch")
                                        ? has(75, "SubagentDeliveryState")
                                          ? has(76, "ProjectionProjectsPinArchive")
                                            ? has(77, "RepairBooleanSettledOverride")
                                              ? has(78, "DropOrchestrationEventCorrelationIndex")
                                                ? has(79, "DelegatedRecoveryIndexes")
                                                  ? has(80, "ScheduledTaskThreadMode")
                                                    ? selectLaterUpstreamMigrationEntries(has)
                                                    : upstreamDelegatedRecoveryIndexesMigrationEntries
                                                  : upstreamDropCorrelationIndexMigrationEntries
                                                : upstreamSettledOverrideRepairMigrationEntries
                                              : upstreamProjectPinArchiveMigrationEntries
                                            : upstreamSubagentDeliveryMigrationEntries
                                          : upstreamAttachmentSearchMigrationEntries
                                        : upstreamActiveTurnItemIndexesMigrationEntries
                                      : upstreamWaitingLinkRequestMigrationEntries
                                    : upstreamPeerLinkFenceMigrationEntries
                                  : upstreamPeerLinksMigrationEntries
                                : upstreamThreadMessageSearchMigrationEntries
                              : upstreamShellStopIndexMigrationEntries
                            : upstreamWebhookDedupeMigrationEntries
                          : upstreamSettlementMigrationEntries
                        : upstreamUsageIndexMigrationEntries
                      : upstreamCanonicalLinksMigrationEntries
                    : upstreamDropProjectionsMigrationEntries
                  : upstreamFailureIndexMigrationEntries
                : upstreamReviewGuideMigrationEntries
              : upstreamMcpAppMigrationEntries
            : has(57, "ScheduledTaskWebhooks")
              ? upstreamWebhookMigrationEntries
              : migrationEntries;
}

export const runMigrations = Effect.fn("runMigrations")(function* ({
  toMigrationInclusive,
}: RunMigrationsOptions = {}) {
  const sql = yield* SqlClient.SqlClient;
  const ledger = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
  `;
  const recorded =
    ledger.length === 0
      ? []
      : yield* sql<RecordedMigration>`
          SELECT migration_id AS id, name FROM effect_sql_migrations ORDER BY migration_id
        `;
  const entries = selectMigrationEntries(recorded);
  yield* assertV2MigrationsCanRun(recorded, toMigrationInclusive);
  const expected = new Map<number, string>(entries.map(([id, name]) => [id, name]));
  // Select known fork/preview lineages without rewriting their ledger. Preserve
  // upstream's diagnostic behavior for site-local migrations sharing an id.
  const divergent = recorded.flatMap((row) => {
    const name = expected.get(row.id);
    return name === row.name
      ? []
      : [
          `${row.id}:${row.name} (${name === undefined ? "unknown to this build" : `this build: ${name}`})`,
        ];
  });
  if (divergent.length > 0) {
    yield* Effect.logWarning(
      "Database migration history diverges from this build; recorded migration ids are skipped, not reconciled by name.",
    ).pipe(Effect.annotateLogs({ divergent }));
  }
  const latestRecordedId = recorded.at(-1)?.id ?? 0;
  const pending = entries.filter(
    ([id]) =>
      id > latestRecordedId && (toMigrationInclusive === undefined || id <= toMigrationInclusive),
  );
  // The desktop waits past its readiness budget until the finished line follows this one.
  if (pending.length > 0) yield* Effect.log(DATABASE_MIGRATIONS_STARTED_MESSAGE);
  // One transaction per migration: an index build on a large database can take minutes, and a
  // restart then resumes after the last finished migration instead of rolling all of them back.
  const executedMigrations: Array<readonly [id: number, name: string]> = [];
  for (const [index, [id, name]] of pending.entries()) {
    if (latestRecordedId > 0) {
      yield* Effect.log(DATABASE_MIGRATION_PROGRESS_MESSAGE).pipe(
        Effect.annotateLogs({
          migration: `${id}_${name}`,
          progress: `${index + 1} of ${pending.length}`,
        }),
      );
    }
    const executed = yield* run({ loader: makeMigrationLoader(entries, id) });
    // Effect's migrator treats every ledger constraint failure as a concurrent runner and
    // returns []. A competing runner has committed this row; a rejected insert has not, and
    // must not let the server start against an unupgraded schema.
    if (executed.length === 0) {
      const completed = yield* sql`
        SELECT 1 FROM effect_sql_migrations WHERE migration_id = ${id} AND name = ${name}
      `;
      if (completed.length === 0) {
        return yield* new Migrator.MigrationError({
          kind: "Failed",
          message: `Database migration did not complete: ${id}_${name}`,
        });
      }
    }
    executedMigrations.push(...executed);
  }
  const migrations = executedMigrations.map(([id, name]) => `${id}_${name}`);
  yield* migrations.length === 0
    ? Effect.logDebug("Database schema is current")
    : Effect.log(DATABASE_MIGRATIONS_FINISHED_MESSAGE).pipe(Effect.annotateLogs({ migrations }));
  return executedMigrations;
});
