import { CommandId, EventId } from "@t3tools/contracts";

import { sha256AgentControlIdentity } from "../controlledThreadReservation/identity.ts";

const derive = (domain: string, handoffId: string, handoffFingerprint: string) =>
  sha256AgentControlIdentity([domain, handoffId, handoffFingerprint]);

export const deriveInitialPlanningStageStartCommandId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  CommandId.make(
    `initial-planning-stage-start-${derive(
      "agent-control-initial-planning-stage-start-command-v1",
      handoffId,
      handoffFingerprint,
    )}`,
  );

export const deriveInitialPlanningStageStartedEventId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  EventId.make(
    `initial-planning-stage-started-${derive(
      "agent-control-initial-planning-stage-started-event-v1",
      handoffId,
      handoffFingerprint,
    )}`,
  );

export const deriveInitialPlanningFinalizationCommandId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  CommandId.make(
    `initial-planning-finalize-${derive(
      "agent-control-initial-planning-finalization-command-v1",
      handoffId,
      handoffFingerprint,
    )}`,
  );

export const deriveInitialPlanningResultEvidenceId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  `initial-planning-result-${derive(
    "agent-control-initial-planning-result-evidence-v1",
    handoffId,
    handoffFingerprint,
  )}`;

export const deriveInitialPlanningTerminalStageEventId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  EventId.make(
    `initial-planning-stage-terminal-${derive(
      "agent-control-initial-planning-terminal-stage-event-v1",
      handoffId,
      handoffFingerprint,
    )}`,
  );

export const deriveInitialPlanningLeaseReleaseEventId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  EventId.make(
    `initial-planning-lease-release-${derive(
      "agent-control-initial-planning-lease-release-event-v1",
      handoffId,
      handoffFingerprint,
    )}`,
  );

export const deriveInitialPlanningFinalizationMarkerId = (
  handoffId: string,
  handoffFingerprint: string,
) =>
  `initial-planning-finalization-marker-${derive(
    "agent-control-initial-planning-finalization-marker-v1",
    handoffId,
    handoffFingerprint,
  )}`;

export const fingerprintInitialPlanningFinalization = (
  domain: "start" | "result" | "marker",
  parts: ReadonlyArray<string>,
) =>
  sha256AgentControlIdentity([`agent-control-initial-planning-${domain}-fingerprint-v1`, ...parts]);
