import type { EnvironmentId, ServerProvider } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { limitsNotice, remainingPercent } from "@t3tools/shared/usageLimits";
import { GaugeIcon } from "lucide-react";
import { useState } from "react";

import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { LimitWindows, ResetCredits } from "../usage/UsageLimits";
import { readUsagePagePreferences, saveUsagePagePreferences } from "../usage/usagePagePreferences";
import { Button } from "../ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { selectPrimaryUsageWindow } from "./providerUsageAvailability";

export function ProviderUsageMeter(props: {
  provider: ServerProvider;
  environmentId: EnvironmentId | null;
  providerDisplayName: string;
}) {
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
  });
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const limits = props.provider.usageLimits;
  const [openedAt, setOpenedAt] = useState(Date.now);
  const now = Math.max(openedAt, limits ? Date.parse(limits.checkedAt) : openedAt);
  const primary = selectPrimaryUsageWindow(limits, now);
  const notice = limits ? limitsNotice(limits) : "Waiting for provider limits.";
  const percent = primary ? remainingPercent(primary) : null;
  const label = primary
    ? `${primary.usedPercent > 90 ? `${primary.label} ` : ""}${percent}% left`
    : "Limits —";

  // Drivers without subscription limits (including API-key accounts) have no quota meter.
  if (
    !props.provider.enabled ||
    limits?.unavailable?.reason === "unsupported" ||
    (!limits && props.provider.driver !== "codex" && props.provider.driver !== "claudeAgent")
  )
    return null;

  const refresh = async () => {
    if (!props.environmentId || refreshing) return;
    setRefreshing(true);
    setRefreshError(null);
    try {
      const result = await refreshProviders({
        environmentId: props.environmentId,
        input: { instanceId: props.provider.instanceId },
      });
      if (result._tag === "Failure") setRefreshError("Could not refresh limits.");
    } finally {
      setOpenedAt(Date.now());
      setRefreshing(false);
    }
  };

  return (
    <Popover
      onOpenChange={(open) => {
        if (open) setOpenedAt(Date.now());
      }}
    >
      <PopoverTrigger
        render={
          <button
            type="button"
            className="inline-flex h-6 shrink-0 cursor-pointer items-center gap-1 rounded-md px-1.5 text-[11px] tabular-nums text-muted-foreground outline-none hover:bg-accent hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
            aria-label={`${props.providerDisplayName} subscription limits: ${label}`}
          >
            <GaugeIcon
              className="size-3.5"
              style={
                primary && primary.usedPercent > 90
                  ? {
                      color:
                        primary.usedPercent >= 100
                          ? "var(--color-red-500)"
                          : "var(--color-amber-500)",
                    }
                  : undefined
              }
            />
            <span>{label}</span>
          </button>
        }
      />
      <PopoverPopup
        side="top"
        align="end"
        className="w-[min(26rem,calc(100vw-2rem))] max-w-none p-3"
      >
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-xs font-medium">{props.providerDisplayName}</div>
              <div className="text-[10px] text-muted-foreground">Subscription limits</div>
            </div>
            <Button
              size="xs"
              variant="outline"
              disabled={refreshing || !props.environmentId}
              onClick={() => void refresh()}
            >
              {refreshing ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
          {notice ? (
            <p className="text-xs text-muted-foreground">{notice}</p>
          ) : limits ? (
            <LimitWindows
              driver={props.provider.driver}
              windows={limits.windows}
              now={now}
              compact
            />
          ) : null}
          {!notice && limits && !primary ? (
            <p className="text-xs text-muted-foreground">
              Reset time passed. Refresh to check the current limits.
            </p>
          ) : null}
          {refreshError ? (
            <p role="status" className="text-xs text-destructive">
              {refreshError}
            </p>
          ) : null}
          {limits?.resetCredits && props.environmentId ? (
            <ResetCredits
              environmentId={props.environmentId}
              input={{ instanceId: props.provider.instanceId }}
              credits={limits.resetCredits}
              now={now}
            />
          ) : null}
          <div className="flex items-center justify-between gap-3 border-t border-border/60 pt-2 text-[10px] text-muted-foreground">
            <span>
              {limits
                ? `Updated ${new Date(limits.checkedAt).toLocaleTimeString()}`
                : "No limits received"}
            </span>
            <Link
              to="/usage"
              onClick={() =>
                saveUsagePagePreferences({ ...readUsagePagePreferences(), metric: "limits" })
              }
              className="hover:text-foreground"
            >
              View all limits
            </Link>
          </div>
        </div>
      </PopoverPopup>
    </Popover>
  );
}
