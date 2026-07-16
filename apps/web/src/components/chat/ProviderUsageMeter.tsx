import type { ProviderUsageSnapshot, ProviderUsageWindow } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { GaugeIcon } from "lucide-react";

import { cn } from "~/lib/utils";
import { formatProviderUsageResetAt } from "../providerUsageFormatting";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";

export function selectPrimaryUsageWindow(usage: ProviderUsageSnapshot): ProviderUsageWindow | null {
  const warningWindow = usage.windows
    .filter((window) => window.usedPercent > 90)
    .toSorted((left, right) => right.usedPercent - left.usedPercent)[0];
  return (
    warningWindow ??
    usage.windows.find((window) => window.id === "five_hour") ??
    usage.windows.find(
      (window) =>
        window.id === "primary" &&
        (window.durationMinutes === undefined || window.durationMinutes < 1_440),
    ) ??
    usage.windows.find((window) => window.id === "seven_day") ??
    usage.windows.find((window) => window.id === "primary") ??
    usage.windows.find((window) => window.id === "secondary") ??
    null
  );
}

function usageColor(percent: number): string {
  if (percent >= 100) return "var(--color-red-500)";
  if (percent >= 70) return "var(--color-amber-500)";
  return "var(--color-blue-500)";
}

function UsageProgress(props: { window: ProviderUsageWindow }) {
  const percent = Math.round(props.window.usedPercent);
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between gap-3 text-[11px]">
        <span className="font-medium text-muted-foreground">{props.window.label}</span>
        <span className="tabular-nums text-muted-foreground/75">{percent}% used</span>
      </div>
      <div
        className="h-1.5 overflow-hidden rounded-full bg-muted/60"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={percent}
        aria-label={`${props.window.label} usage`}
      >
        <div
          className="h-full rounded-full transition-[width,background-color] duration-500 motion-reduce:transition-none"
          style={{ width: `${props.window.usedPercent}%`, backgroundColor: usageColor(percent) }}
        />
      </div>
      <span className="text-[10px] text-muted-foreground/55">
        {formatProviderUsageResetAt(props.window.resetsAt)}
      </span>
    </div>
  );
}

export function ProviderUsageMeter(props: {
  usage: ProviderUsageSnapshot | null;
  providerDisplayName: string;
}) {
  const primary = props.usage ? selectPrimaryUsageWindow(props.usage) : null;
  const percent = primary ? Math.round(primary.usedPercent) : null;
  const showWindowLabel = primary !== null && primary.usedPercent > 90;

  return (
    <Popover>
      <PopoverTrigger
        render={
          <button
            type="button"
            className={cn(
              "inline-flex h-6 shrink-0 cursor-pointer items-center gap-1 rounded-md border border-transparent px-1.5 text-[11px] tabular-nums text-muted-foreground outline-none transition-colors",
              "hover:bg-accent hover:text-foreground data-[pressed]:bg-accent",
              "focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-background",
            )}
            aria-label={
              percent === null
                ? `${props.providerDisplayName} session usage unavailable`
                : `${props.providerDisplayName} ${primary?.label ?? "session"} usage ${percent}% used`
            }
          >
            <GaugeIcon
              className="size-3.5"
              style={percent === null ? undefined : { color: usageColor(percent) }}
            />
            <span>
              {percent === null ? "--" : `${showWindowLabel ? `${primary.label} ` : ""}${percent}%`}
            </span>
          </button>
        }
      />
      <PopoverPopup side="top" align="end" className="w-72 max-w-none p-0">
        <div className="flex flex-col gap-3 p-3">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-xs font-medium text-foreground">{props.providerDisplayName}</div>
              <div className="text-[10px] text-muted-foreground/55">Provider usage</div>
            </div>
            {props.usage ? (
              <span
                className={cn(
                  "text-[10px] font-medium capitalize",
                  props.usage.status === "rejected"
                    ? "text-red-500"
                    : props.usage.status === "warning"
                      ? "text-amber-500"
                      : "text-muted-foreground/60",
                )}
              >
                {props.usage.status}
              </span>
            ) : null}
          </div>
          {props.usage ? (
            props.usage.windows.map((window) => <UsageProgress key={window.id} window={window} />)
          ) : (
            <div className="py-2 text-xs text-muted-foreground/60">
              Waiting for usage data from the provider.
            </div>
          )}
          {props.usage?.overageStatus ? (
            <div className="flex items-center justify-between border-border/60 border-t pt-2 text-[11px]">
              <span className="text-muted-foreground/60">Overage</span>
              <span className="capitalize text-muted-foreground/80">
                {props.usage.isUsingOverage ? "In use" : props.usage.overageStatus}
              </span>
            </div>
          ) : null}
          <div className="flex items-center justify-between border-border/60 border-t pt-2 text-[10px] text-muted-foreground/50">
            <span>
              {props.usage
                ? `Updated ${new Date(props.usage.observedAt).toLocaleTimeString()}`
                : "No usage event received"}
            </span>
            <Link to="/" className="text-muted-foreground/75 hover:text-foreground">
              View all usage
            </Link>
          </div>
        </div>
      </PopoverPopup>
    </Popover>
  );
}
