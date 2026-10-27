import { useAtomMount } from "@effect/atom-react";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { createThreadVisitTracker } from "@t3tools/client-runtime/state/thread-visits";
import { type ScopedThreadRef } from "@t3tools/contracts";
import { useParams } from "@tanstack/react-router";
import { useSyncExternalStore } from "react";
import { getDocumentActivity, subscribeDocumentActivity } from "~/lib/documentActivity";

import { useUiStateStore } from "../../uiStateStore";
import { environmentThreadShells, threadEnvironment } from "../../state/threads";
import { resolveThreadRouteRef } from "../../threadRoutes";
import { isViewingThread } from "../ChatView.logic";

const threadVisitTracker = createThreadVisitTracker({
  threadShellAtom: environmentThreadShells.threadShellAtom,
  visit: threadEnvironment.visit,
  markLocalVisit: (ref, visitedAt) =>
    useUiStateStore.getState().markThreadVisited(scopedThreadKey(ref), visitedAt),
});

// The visit watermark is the one part of the chat view that follows the
// shell's updatedAt, which ticks while a run streams. The tracker subscribes to
// it outside React, so ChatView keeps reading the quiet shell.
export function ThreadVisitWatermark(props: { readonly threadRef: ScopedThreadRef }) {
  const activeRouteThreadRef = useParams({
    strict: false,
    select: (params) => resolveThreadRouteRef(params),
  });
  const documentActivity = useSyncExternalStore(subscribeDocumentActivity, getDocumentActivity);
  // A mounted tracker counts as viewing, so background threads and unfocused windows stay unread.
  const viewing = isViewingThread({
    viewedThreadRef: props.threadRef,
    activeRouteThreadRef,
    isDocumentVisible: documentActivity.visible,
    isDocumentFocused: documentActivity.focused,
  });
  useAtomMount(threadVisitTracker(viewing ? props.threadRef : null));
  return null;
}
