import { createFileRoute, useCanGoBack, useNavigate } from "@tanstack/react-router";
import { useCallback, useEffect } from "react";

import { isCommandPaletteOpen } from "../commandPaletteBus";
import { NoActiveThreadState } from "../components/NoActiveThreadState";

function UsageRouteView() {
  const canGoBack = useCanGoBack();
  const navigate = useNavigate();
  const navigateBackWithinApp = useCallback(() => {
    if (canGoBack) {
      window.history.back();
      return;
    }
    void navigate({ to: "/" });
  }, [canGoBack, navigate]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || isCommandPaletteOpen()) return;
      if (event.key !== "Escape") return;

      event.preventDefault();
      navigateBackWithinApp();
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [navigateBackWithinApp]);

  return <NoActiveThreadState onBack={navigateBackWithinApp} />;
}

export const Route = createFileRoute("/_chat/usage")({
  component: UsageRouteView,
});
