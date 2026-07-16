import type { EnvironmentId, ProviderUsageSnapshot, ServerProvider } from "@t3tools/contracts";
import { GaugeIcon, RefreshCwIcon } from "lucide-react";
import { useState } from "react";

import { cn } from "~/lib/utils";
import { serverEnvironment } from "../state/server";
import { useAtomCommand } from "../state/use-atom-command";
import { Tooltip, TooltipPopup, TooltipTrigger } from "./ui/tooltip";
import { useEnvironments, type EnvironmentPresentation } from "../state/environments";
import { useProviderUsage } from "../state/server";
import { formatProviderUsageResetAt } from "./providerUsageFormatting";

function providerName(provider: ServerProvider): string {
  return provider.displayName ?? (provider.driver === "claudeAgent" ? "Claude" : "Codex");
}

function UsageRow(props: { provider: ServerProvider; usage: ProviderUsageSnapshot | undefined }) {
  const windows = props.usage?.windows ?? [];
  return (
    <div className="grid min-h-14 gap-x-5 gap-y-3 border-border/55 border-t px-1 py-3 first:border-t-0 sm:grid-cols-[minmax(9rem,1fr)_minmax(16rem,2fr)] sm:items-start">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-foreground">
          {providerName(props.provider)}
        </div>
        <div className="truncate text-[11px] text-muted-foreground/55">{props.provider.driver}</div>
      </div>
      {windows.length > 0 ? (
        <div className="flex min-w-0 flex-col gap-2.5">
          {windows.map((window) => (
            <div
              key={window.id}
              className="grid min-w-0 grid-cols-[minmax(5.5rem,auto)_minmax(4rem,1fr)_2.5rem] items-center gap-x-3 gap-y-0.5"
            >
              <span className="truncate text-xs text-muted-foreground">{window.label}</span>
              <div className="h-1.5 min-w-16 overflow-hidden rounded-full bg-muted/60">
                <div
                  className={cn(
                    "h-full rounded-full",
                    window.usedPercent >= 90
                      ? "bg-red-500"
                      : window.usedPercent >= 70
                        ? "bg-amber-500"
                        : "bg-blue-500",
                  )}
                  style={{ width: `${window.usedPercent}%` }}
                />
              </div>
              <span className="text-right text-xs tabular-nums text-muted-foreground/75">
                {Math.round(window.usedPercent)}%
              </span>
              <span className="col-span-2 col-start-2 text-[10px] text-muted-foreground/55">
                {formatProviderUsageResetAt(window.resetsAt)}
              </span>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-xs text-muted-foreground/50 sm:pt-1">Usage unavailable</div>
      )}
    </div>
  );
}

function EnvironmentUsageSection(props: {
  environmentId: EnvironmentId;
  environment: EnvironmentPresentation;
}) {
  const usage = useProviderUsage(props.environmentId);
  const refreshUsage = useAtomCommand(serverEnvironment.refreshProviderUsage, {
    reportFailure: false,
  });
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const providers =
    props.environment.serverConfig?.providers.filter(
      (provider) =>
        provider.enabled && (provider.driver === "claudeAgent" || provider.driver === "codex"),
    ) ?? [];

  if (providers.length === 0) return null;
  const handleRefresh = async () => {
    if (isRefreshing) return;
    setIsRefreshing(true);
    setRefreshError(null);
    const result = await refreshUsage({
      environmentId: props.environmentId,
      input: { providerInstanceIds: providers.map((provider) => provider.instanceId) },
    });
    if (result._tag === "Failure") {
      setRefreshError("Refresh failed");
    } else if (result.value.failures.length > 0) {
      setRefreshError(
        result.value.failures.length === providers.length
          ? (result.value.failures[0]?.message ?? "Refresh failed")
          : `${result.value.failures.length} provider refresh failed`,
      );
    }
    setIsRefreshing(false);
  };
  return (
    <section className="w-full">
      <div className="mb-2 flex items-center justify-between gap-3">
        <h2 className="text-xs font-medium text-muted-foreground">{props.environment.label}</h2>
        <div className="flex min-w-0 items-center gap-2">
          {usage.error || refreshError ? (
            <span className="max-w-64 truncate text-[10px] text-red-500">
              {refreshError ?? "Usage unavailable"}
            </span>
          ) : null}
          <Tooltip>
            <TooltipTrigger
              render={
                <button
                  type="button"
                  aria-label="Refresh usage"
                  className="inline-flex size-7 shrink-0 items-center justify-center rounded-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:cursor-wait disabled:opacity-60"
                  disabled={isRefreshing}
                  onClick={handleRefresh}
                />
              }
            >
              <RefreshCwIcon className={cn("size-3.5", isRefreshing && "animate-spin")} />
            </TooltipTrigger>
            <TooltipPopup>Refresh usage</TooltipPopup>
          </Tooltip>
        </div>
      </div>
      <div className="border-border/60 border-y">
        {providers.map((provider) => (
          <UsageRow
            key={provider.instanceId}
            provider={provider}
            usage={usage.data?.get(provider.instanceId)}
          />
        ))}
      </div>
    </section>
  );
}

export function ProviderUsageOverview() {
  const { environments } = useEnvironments();
  const supportedEnvironments = environments.filter((environment) =>
    environment.serverConfig?.providers.some(
      (provider) => provider.driver === "claudeAgent" || provider.driver === "codex",
    ),
  );

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-8 px-5 py-8 sm:px-8 sm:py-12">
      <div className="flex items-center gap-3">
        <GaugeIcon className="size-5 text-muted-foreground" />
        <div>
          <h1 className="text-lg font-medium text-foreground">Usage</h1>
          <p className="text-xs text-muted-foreground/60">Limits by provider instance</p>
        </div>
      </div>
      {supportedEnvironments.length > 0 ? (
        supportedEnvironments.map((environment) => (
          <EnvironmentUsageSection
            key={environment.environmentId}
            environmentId={environment.environmentId}
            environment={environment}
          />
        ))
      ) : (
        <div className="py-10 text-sm text-muted-foreground/55">
          No Claude or Codex providers are configured.
        </div>
      )}
    </div>
  );
}
