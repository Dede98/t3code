import { createFileRoute } from "@tanstack/react-router";

import { NoActiveThreadState } from "../components/NoActiveThreadState";

function UsageRouteView() {
  return <NoActiveThreadState />;
}

export const Route = createFileRoute("/_chat/usage")({
  component: UsageRouteView,
});
