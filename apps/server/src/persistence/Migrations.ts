/**
 * Migration runner with an inline loader.
 *
 * Uses Migrator.make with fromRecord to define migrations inline.
 * All migrations are statically imported - no dynamic file system loading.
 *
 * `runMigrations` is called by the SQLite persistence layer at startup, so the
 * schema is always up to date before the application starts.
 */

import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

// Import all migrations statically
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
import Migration0033 from "./Migrations/033_ClaudeSessionStore.ts";
import Migration0034 from "./Migrations/034_ProjectionThreadsSettled.ts";
import Migration0035 from "./Migrations/035_ProjectionThreadsSnoozed.ts";
import Migration0036 from "./Migrations/036_ProjectionThreadTitleRegeneration.ts";
import Migration0037 from "./Migrations/037_ProjectionThreadsPinned.ts";
import Migration0038 from "./Migrations/038_ProjectionTurnsKeysetIndex.ts";
import Migration0039 from "./Migrations/039_ProjectionThreadsPinOrderKey.ts";
import Migration0040 from "./Migrations/040_ProjectionProjectsDefaultThreadEnvMode.ts";
import Migration0041 from "./Migrations/041_ProjectionProjectFaviconPath.ts";
import Migration0042 from "./Migrations/042_AuthSessionClientConnection.ts";
import Migration0043 from "./Migrations/043_ProjectionThreadLinkedPullRequest.ts";
import Migration0044 from "./Migrations/044_ProjectionThreadsUnsettledAt.ts";
import Migration0045 from "./Migrations/045_ClearAutomaticProjectModelDefaults.ts";
import Migration0046 from "./Migrations/046_ProjectionProjectsAutoPull.ts";
import Migration0047 from "./Migrations/047_RepairAutomaticSettlementTimestamps.ts";
import Migration0048 from "./Migrations/048_ProjectionProjectIcon.ts";
import Migration0049 from "./Migrations/049_ProjectionThreadBranchPullRequest.ts";
import Migration0050 from "./Migrations/050_ProjectionThreadsActiveOrderKey.ts";

/**
 * Migration loader with all migrations defined inline.
 *
 * Key format: "{id}_{name}" where:
 * - id: numeric migration ID (determines execution order)
 * - name: descriptive name for the migration
 *
 * Uses Migrator.fromRecord which parses the key format and
 * returns migrations sorted by ID.
 */
import AgentMigration0034 from "./Migrations/034_OrchestrationCommandAuthority.ts";
import AgentMigration0035 from "./Migrations/035_AgentControlThreadBinding.ts";
import AgentMigration0036 from "./Migrations/036_AgentControlProjectPolicies.ts";
import AgentMigration0037 from "./Migrations/037_DetachAgentControlProjectPolicies.ts";
import AgentMigration0038 from "./Migrations/038_AgentControlCqrsFoundation.ts";
import AgentMigration0039 from "./Migrations/039_AgentControlGithubObserveFoundation.ts";
import AgentMigration0040 from "./Migrations/040_AgentControlGithubObserveReactor.ts";
import AgentMigration0041 from "./Migrations/041_AgentControlGithubObserveRecoveryCas.ts";
import AgentMigration0042 from "./Migrations/042_AgentControlTaskIntakeCqrs.ts";
import AgentMigration0043 from "./Migrations/043_AgentControlTaskIntakeHardening.ts";
import AgentMigration0044 from "./Migrations/044_AgentControlStageRunCqrs.ts";
import AgentMigration0045 from "./Migrations/045_AgentControlStageRunLeaseFoundation.ts";
import AgentMigration0046 from "./Migrations/046_AgentControlWorktreeReservationFoundation.ts";
import AgentMigration0047 from "./Migrations/047_AgentControlControlledThreadReservationFoundation.ts";
import AgentMigration0048 from "./Migrations/048_AgentControlControlledThreadMaterializationBoundary.ts";
import AgentMigration0049 from "./Migrations/049_AgentControlControlledThreadMaterializationCoordinator.ts";
import AgentMigration0050 from "./Migrations/050_AgentControlControlledThreadPrepareFinalization.ts";
import AgentMigration0051 from "./Migrations/051_AgentControlInitialPlanningHandoff.ts";
import AgentMigration0052 from "./Migrations/052_AgentControlInitialPlanningStageFinalization.ts";
import AgentMigration0053 from "./Migrations/053_AgentControlInitialPlanningStageFinalizationHardening.ts";
import AgentMigration0054 from "./Migrations/054_AgentControlImplementationAdmission.ts";
import AgentMigration0055 from "./Migrations/055_AgentControlImplementationTurnStart.ts";
import AgentMigration0056 from "./Migrations/056_AgentControlImplementationStageFinalization.ts";
import AgentMigration0057 from "./Migrations/057_AgentControlVerificationAdmission.ts";
import AgentMigration0058 from "./Migrations/058_AgentControlVerificationTurnStart.ts";
import AgentMigration0059 from "./Migrations/059_AgentControlVerificationTurnTerminalObservation.ts";
import AgentMigration0060 from "./Migrations/060_AgentControlVerificationEvaluation.ts";
import AgentMigration0061 from "./Migrations/061_AgentControlVerificationStageFinalization.ts";
import AgentMigration0062 from "./Migrations/062_AgentControlTaskVerificationFinalization.ts";
import AgentMigration0063 from "./Migrations/063_AgentControlRunOnceActivation.ts";
import AgentMigration0064 from "./Migrations/064_AgentControlArmedSingleFlight.ts";
import AgentMigration0065 from "./Migrations/065_AgentControlProviderCapacityAdmission.ts";

import AgentMigration0066 from "./Migrations/066_AgentControlMainLifecycleEvents.ts";
import AgentMigration0067 from "./Migrations/067_AgentControlLeaseRuntimeOwnership.ts";
import AgentMigration0068 from "./Migrations/068_AgentControlProviderPreInvokeRecovery.ts";
import AgentMigration0069 from "./Migrations/069_AgentControlVerificationLeaseRenewal.ts";

import AgentMigration0070 from "./Migrations/070_AgentControlPreparedSessionRecovery.ts";
import AgentMigration0071 from "./Migrations/071_AgentControlNativeTerminalReceipts.ts";
import AgentMigration0073 from "./Migrations/073_AgentControlRunOnceRepair.ts";
import AgentMigration0072 from "./Migrations/072_AgentControlArchivedPreparationCapacityRecovery.ts";

const agentMigrationEntries = [
  [34, "OrchestrationCommandAuthority", AgentMigration0034],
  [35, "AgentControlThreadBinding", AgentMigration0035],
  [36, "AgentControlProjectPolicies", AgentMigration0036],
  [37, "DetachAgentControlProjectPolicies", AgentMigration0037],
  [38, "AgentControlCqrsFoundation", AgentMigration0038],
  [39, "AgentControlGithubObserveFoundation", AgentMigration0039],
  [40, "AgentControlGithubObserveReactor", AgentMigration0040],
  [41, "AgentControlGithubObserveRecoveryCas", AgentMigration0041],
  [42, "AgentControlTaskIntakeCqrs", AgentMigration0042],
  [43, "AgentControlTaskIntakeHardening", AgentMigration0043],
  [44, "AgentControlStageRunCqrs", AgentMigration0044],
  [45, "AgentControlStageRunLeaseFoundation", AgentMigration0045],
  [46, "AgentControlWorktreeReservationFoundation", AgentMigration0046],
  [47, "AgentControlControlledThreadReservationFoundation", AgentMigration0047],
  [48, "AgentControlControlledThreadMaterializationBoundary", AgentMigration0048],
  [49, "AgentControlControlledThreadMaterializationCoordinator", AgentMigration0049],
  [50, "AgentControlControlledThreadPrepareFinalization", AgentMigration0050],
  [51, "AgentControlInitialPlanningHandoff", AgentMigration0051],
  [52, "AgentControlInitialPlanningStageFinalization", AgentMigration0052],
  [53, "AgentControlInitialPlanningStageFinalizationHardening", AgentMigration0053],
  [54, "AgentControlImplementationAdmission", AgentMigration0054],
  [55, "AgentControlImplementationTurnStart", AgentMigration0055],
  [56, "AgentControlImplementationStageFinalization", AgentMigration0056],
  [57, "AgentControlVerificationAdmission", AgentMigration0057],
  [58, "AgentControlVerificationTurnStart", AgentMigration0058],
  [59, "AgentControlVerificationTurnTerminalObservation", AgentMigration0059],
  [60, "AgentControlVerificationEvaluation", AgentMigration0060],
  [61, "AgentControlVerificationStageFinalization", AgentMigration0061],
  [62, "AgentControlTaskVerificationFinalization", AgentMigration0062],
  [63, "AgentControlRunOnceActivation", AgentMigration0063],
  [64, "AgentControlArmedSingleFlight", AgentMigration0064],
  [65, "AgentControlProviderCapacityAdmission", AgentMigration0065],
  [66, "AgentControlMainLifecycleEvents", AgentMigration0066],
  [67, "AgentControlLeaseRuntimeOwnership", AgentMigration0067],
  [68, "AgentControlProviderPreInvokeRecovery", AgentMigration0068],
  [69, "AgentControlVerificationLeaseRenewal", AgentMigration0069],
  [70, "AgentControlPreparedSessionRecovery", AgentMigration0070],
  [71, "AgentControlNativeTerminalReceipts", AgentMigration0071],
  [72, "AgentControlArchivedPreparationCapacityRecovery", AgentMigration0072],
  [73, "AgentControlRunOnceRepair", AgentMigration0073],
] as const;

const migrationEntries = [
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
  [33, "ClaudeSessionStore", Migration0033],
  [34, "ProjectionThreadsSettled", Migration0034],
  [35, "ProjectionThreadsSnoozed", Migration0035],
  [36, "ProjectionThreadTitleRegeneration", Migration0036],
  [37, "ProjectionThreadsPinned", Migration0037],
  [38, "ProjectionTurnsKeysetIndex", Migration0038],
  [39, "ProjectionThreadsPinOrderKey", Migration0039],
  [40, "ProjectionProjectsDefaultThreadEnvMode", Migration0040],
  [41, "ProjectionProjectFaviconPath", Migration0041],
  [42, "AuthSessionClientConnection", Migration0042],
  [43, "ProjectionThreadLinkedPullRequest", Migration0043],
  [44, "ProjectionThreadsUnsettledAt", Migration0044],
  [45, "ClearAutomaticProjectModelDefaults", Migration0045],
  [46, "ProjectionProjectsAutoPull", Migration0046],
  [47, "RepairAutomaticSettlementTimestamps", Migration0047],
  [48, "ProjectionProjectIcon", Migration0048],
  [49, "ProjectionThreadBranchPullRequest", Migration0049],
  [50, "ProjectionThreadsActiveOrderKey", Migration0050],
] as const;

export const migrationManifest = migrationEntries.map(([id, name]) => [id, name] as const);

export const makeMigrationLoader = (throughId?: number) =>
  Migrator.fromRecord(
    Object.fromEntries(
      migrationEntries
        .filter(([id]) => throughId === undefined || id <= throughId)
        .map(([id, name, migration]) => [`${id}_${name}`, migration]),
    ),
  );

export const makeAgentControlMigrationLoader = (throughId?: number) =>
  Migrator.fromRecord(
    Object.fromEntries(
      agentMigrationEntries
        .filter(([id]) => throughId === undefined || id <= throughId)
        .map(([id, name, migration]) => [`${id}_${name}`, migration]),
    ),
  );

/**
 * Migrator run function - no schema dumping needed
 * Uses the base Migrator.make without platform dependencies
 */
const run = Migrator.make({});

export interface RunMigrationsOptions {
  readonly toMigrationInclusive?: number | undefined;
}

/**
 * Run all pending migrations.
 *
 * Creates the migrations tracking table (effect_sql_migrations) if it doesn't exist,
 * then runs any migrations with ID greater than the latest recorded migration.
 *
 * Returns array of [id, name] tuples for migrations that were run.
 *
 * @returns Effect containing array of executed migrations
 */
export const runMigrations = Effect.fn("runMigrations")(function* ({
  toMigrationInclusive,
}: RunMigrationsOptions = {}) {
  const sql = yield* SqlClient.SqlClient;
  // The two branches allocated IDs independently. Keep the released main
  // history intact and track Agent Control migrations in a separate journal.
  // Move only exact known legacy entries, preserving their original timestamps.
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`CREATE TABLE IF NOT EXISTS main.effect_sql_migrations (
      migration_id integer PRIMARY KEY NOT NULL,
      created_at datetime NOT NULL DEFAULT current_timestamp,
      name VARCHAR(255) NOT NULL
    )`;
      yield* sql`CREATE TABLE IF NOT EXISTS main.effect_sql_agent_control_migrations (
      migration_id integer PRIMARY KEY NOT NULL,
      created_at datetime NOT NULL DEFAULT current_timestamp,
      name VARCHAR(255) NOT NULL
    )`;
      for (const [id, name] of agentMigrationEntries) {
        yield* sql`INSERT OR IGNORE INTO main.effect_sql_agent_control_migrations
        SELECT * FROM main.effect_sql_migrations WHERE migration_id = ${id} AND name = ${name}`;
        yield* sql`DELETE FROM main.effect_sql_migrations WHERE migration_id = ${id} AND name = ${name}`;
      }
    }),
  );
  const coreMigrations = yield* run({ loader: makeMigrationLoader(toMigrationInclusive) });
  const agentMigrations = yield* run({
    table: "effect_sql_agent_control_migrations",
    loader: makeAgentControlMigrationLoader(toMigrationInclusive),
  });
  const executedMigrations = [...coreMigrations, ...agentMigrations];
  const migrations = executedMigrations.map(([id, name]) => `${id}_${name}`);
  yield* migrations.length === 0
    ? Effect.logDebug("Database schema is current")
    : Effect.log("Migrations ran successfully").pipe(Effect.annotateLogs({ migrations }));
  return executedMigrations;
});
