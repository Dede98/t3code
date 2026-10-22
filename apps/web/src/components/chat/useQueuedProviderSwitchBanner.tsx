import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ProviderInstanceId,
  ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import { ArrowRightLeftIcon } from "lucide-react";
import { useState } from "react";

import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  buildClaudeContinuationSyncIdentity,
  describeClaudeContinuationSyncError,
  getQueuedProviderSwitchDescription,
  shouldOfferClaudeContinuationSync,
} from "../ChatView.logic";
import { Button } from "../ui/button";
import { stackedThreadToast, toastManager } from "../ui/toast";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

interface QueuedProviderSwitchBannerInput {
  readonly thread: {
    readonly id: ThreadId;
    readonly environmentId: EnvironmentId;
    readonly updatedAt: string;
  } | null;
  readonly activeInstanceId: ProviderInstanceId | undefined;
  readonly selectedInstanceId: ProviderInstanceId | null;
  readonly providerStatuses: ReadonlyArray<ServerProvider>;
  readonly crossAccountContinuationEnabled: boolean;
}

/**
 * A provider picked for a running thread applies on the next turn. Between two
 * Claude accounts the banner also offers to sync the history ahead of time.
 */
export function useQueuedProviderSwitchBanner(
  input: QueuedProviderSwitchBannerInput,
): ComposerBannerStackItem | null {
  const { thread, activeInstanceId, selectedInstanceId, providerStatuses } = input;
  const syncThreadContinuation = useAtomCommand(threadEnvironment.syncContinuation, {
    reportFailure: false,
  });
  const [syncingContinuationIdentity, setSyncingContinuationIdentity] = useState<string | null>(
    null,
  );
  const [syncedContinuationIdentityByThreadId, setSyncedContinuationIdentityByThreadId] = useState<
    ReadonlyMap<ThreadId, string>
  >(() => new Map());

  const queuedProviderSwitch = (() => {
    if (!activeInstanceId || !selectedInstanceId || selectedInstanceId === activeInstanceId) {
      return null;
    }
    const target = providerStatuses.find((provider) => provider.instanceId === selectedInstanceId);
    return {
      instanceId: selectedInstanceId,
      label: target?.displayName ?? selectedInstanceId,
      driver: target?.driver,
    };
  })();
  const canSyncQueuedClaudeSwitch = shouldOfferClaudeContinuationSync({
    enabled: input.crossAccountContinuationEnabled,
    sourceDriver: providerStatuses.find((provider) => provider.instanceId === activeInstanceId)
      ?.driver,
    targetDriver: queuedProviderSwitch?.driver,
    isQueuedSwitch: queuedProviderSwitch !== null,
  });
  const continuationSyncIdentity =
    thread && canSyncQueuedClaudeSwitch
      ? buildClaudeContinuationSyncIdentity({
          threadId: thread.id,
          threadUpdatedAt: thread.updatedAt,
          sourceInstanceId: activeInstanceId,
          sourceSessionUpdatedAt: thread.updatedAt,
          targetInstanceId: queuedProviderSwitch?.instanceId,
        })
      : null;
  const isSyncingContinuation =
    continuationSyncIdentity !== null && syncingContinuationIdentity === continuationSyncIdentity;
  const hasSyncedContinuation =
    thread !== null &&
    continuationSyncIdentity !== null &&
    syncedContinuationIdentityByThreadId.get(thread.id) === continuationSyncIdentity;
  const handleSyncThreadContinuation = async () => {
    if (!thread || continuationSyncIdentity === null || isSyncingContinuation) return;
    setSyncingContinuationIdentity(continuationSyncIdentity);
    const threadRef = scopeThreadRef(thread.environmentId, thread.id);
    // A bare try/finally makes React Compiler skip the whole component.
    await syncThreadContinuation({
      environmentId: thread.environmentId,
      input: { threadId: thread.id },
    })
      .then((result) => {
        if (result._tag === "Failure") {
          const error = squashAtomCommandFailure(result);
          toastManager.add(
            stackedThreadToast({
              type: "error",
              title: "Could not sync Claude history",
              data: { threadRef },
              description: describeClaudeContinuationSyncError(error),
            }),
          );
          return;
        }
        setSyncedContinuationIdentityByThreadId((current) => {
          const next = new Map(current);
          next.set(thread.id, continuationSyncIdentity);
          return next;
        });
        toastManager.add(
          stackedThreadToast({
            type: "success",
            title: "Claude history synced",
            data: { threadRef },
            description: "The next turn can start on the selected Claude account.",
          }),
        );
      })
      .finally(() => {
        setSyncingContinuationIdentity((current) =>
          current === continuationSyncIdentity ? null : current,
        );
      });
  };

  if (!queuedProviderSwitch) return null;
  return {
    id: `provider-switch-queued:${queuedProviderSwitch.instanceId}`,
    variant: "info",
    icon: <ArrowRightLeftIcon />,
    title: `Continue with ${queuedProviderSwitch.label}`,
    description: getQueuedProviderSwitchDescription({
      canSyncClaudeContinuation: canSyncQueuedClaudeSwitch,
    }),
    ...(canSyncQueuedClaudeSwitch
      ? {
          actions: (
            <Button
              size="xs"
              variant="outline"
              disabled={isSyncingContinuation || hasSyncedContinuation}
              onClick={() => void handleSyncThreadContinuation()}
            >
              {isSyncingContinuation
                ? "Syncing…"
                : hasSyncedContinuation
                  ? "History synced"
                  : "Sync history now"}
            </Button>
          ),
        }
      : {}),
  };
}
