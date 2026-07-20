import { ArrowLeftIcon } from "lucide-react";

import { Button } from "./ui/button";
import { SidebarInset } from "./ui/sidebar";
import { ProviderUsageOverview } from "./ProviderUsageOverview";
import { isElectron } from "../env";
import { cn } from "~/lib/utils";
import { COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS } from "~/workspaceTitlebar";

export function NoActiveThreadState({ onBack }: { readonly onBack?: () => void }) {
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none bg-background text-foreground">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-x-hidden bg-background">
        <header
          className={cn(
            "border-b border-border px-3 transition-[padding-left] duration-200 ease-linear motion-reduce:transition-none sm:px-5",
            isElectron ? "workspace-topbar drag-region" : "workspace-topbar",
            COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS,
          )}
        >
          <div className="flex items-center gap-2">
            {onBack ? (
              <Button
                aria-label="Back to previous page"
                className="[-webkit-app-region:no-drag]"
                onClick={onBack}
                size="xs"
                variant="ghost"
              >
                <ArrowLeftIcon />
                Back
              </Button>
            ) : null}
            {isElectron ? (
              <span className="text-xs text-muted-foreground/50 wco:pr-[var(--workspace-native-controls-inset)]">
                Usage
              </span>
            ) : (
              <span className="text-sm font-medium text-foreground md:text-muted-foreground/60">
                Usage
              </span>
            )}
          </div>
        </header>

        <div className="min-h-0 flex-1 overflow-y-auto">
          <ProviderUsageOverview />
        </div>
      </div>
    </SidebarInset>
  );
}
