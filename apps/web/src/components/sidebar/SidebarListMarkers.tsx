import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { msg, useMessages } from "gt-react";
import { type ReactNode, useLayoutEffect } from "react";
import { cn } from "~/lib/utils";
import { useUiStateStore } from "../../uiStateStore";
import {
  animateSidebarLayoutChanges,
  resolveSidebarShelfAttention,
  resolveThreadLastVisitedAt,
  sidebarMarkerId,
  type SidebarListMarker,
} from "../Sidebar.logic";
import { SIDEBAR_SORTABLE_DRAG_DISABLED } from "./SidebarThreadRow";

function SortableSidebarMarker(props: {
  marker: SidebarListMarker;
  className?: string;
  children?: ReactNode;
  revealOverflow?: boolean;
  "data-testid"?: string;
}) {
  const { node, setNodeRef, transform, transition } = useSortable({
    id: sidebarMarkerId(props.marker),
    disabled: SIDEBAR_SORTABLE_DRAG_DISABLED,
    animateLayoutChanges: animateSidebarLayoutChanges,
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the projected drop box inside the scroll viewport as drag transforms move it
  useLayoutEffect(() => {
    if (props.revealOverflow && transform?.y !== undefined) {
      node.current?.firstElementChild?.scrollIntoView({ block: "nearest", inline: "nearest" });
    }
  }, [node, props.revealOverflow, transform?.y]);
  return (
    <li
      ref={setNodeRef}
      data-thread-selection-safe
      data-testid={props["data-testid"]}
      className={cn("list-none", props.className)}
      style={{
        transform: CSS.Translate.toString(transform),
        transition: props.marker.endsWith("-placeholder") ? "none" : transition,
        visibility: transform?.scaleY === 0 ? "hidden" : undefined,
      }}
    >
      {props.children}
    </li>
  );
}

export function SidebarSectionPlaceholder(props: {
  marker: "active-placeholder" | "settled-placeholder";
  label: string;
  showHint: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="relative mx-0.5 -mb-px h-0"
      // Revealing on every drag scrolls the list out from under the pointer.
      revealOverflow={
        props.marker === "settled-placeholder" && props.showHint && props.isDropTarget
      }
    >
      {props.showHint ? (
        <div
          className={cn(
            "absolute inset-x-0 top-0 flex h-9 items-center justify-center rounded-md border border-dashed border-sidebar-foreground/25 text-xs text-sidebar-foreground/80",
            props.isDropTarget && "border-primary/40 bg-primary/5 text-primary",
          )}
        >
          {props.label}
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

export function SidebarDragBoundary(props: {
  marker: "pinned-header" | "pinned-divider";
  label: string;
  visible: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="pointer-events-none relative mx-0.5 -mb-px h-0"
    >
      {props.visible ? (
        <div className="sidebar-drag-boundary-label absolute inset-x-2 top-1 flex h-4 items-center gap-2">
          <span
            className={cn(
              "shrink-0 text-xs font-medium",
              props.isDropTarget ? "text-primary" : "text-sidebar-foreground/80",
            )}
          >
            {props.label}
          </span>
          <span
            aria-hidden
            className={cn(
              "h-px flex-1",
              props.isDropTarget ? "bg-primary/50" : "bg-sidebar-foreground/25",
            )}
          />
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

const SHELF_BY_HEADER = {
  "pinned-header": "pinned",
  "working-header": "working",
  "agent-header": "agent",
  "snoozed-header": "snoozed",
  "settled-header": "settled",
} as const;

const SHELF_ATTENTION_CLASS_NAME = { attention: "bg-warning", failed: "bg-error" } as const;

const SHELF_ATTENTION_LABEL = {
  attention: msg(", one needs you", {
    $context: "sidebar section status, read after its name and thread count",
  }),
  failed: msg(", one failed", {
    $context: "sidebar section status, read after its name and thread count",
  }),
} as const;

// A collapsed section would otherwise hide that one of its threads waits on the user.
function ShelfCountWithAttention(props: {
  count: number;
  hiddenThreads: readonly EnvironmentThreadShell[];
}) {
  const m = useMessages();
  const localLastVisitedAtById = useUiStateStore((state) => state.threadLastVisitedAtById);
  const attention = resolveSidebarShelfAttention(
    props.hiddenThreads.map((thread) => ({
      ...thread,
      lastVisitedAt: resolveThreadLastVisitedAt(
        thread.lastVisitedAt,
        localLastVisitedAtById[scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id))],
      ),
    })),
  );
  return (
    <>
      {attention === null ? null : (
        <span
          aria-hidden
          data-testid="sidebar-shelf-attention"
          className={cn("size-1.5 shrink-0 rounded-full", SHELF_ATTENTION_CLASS_NAME[attention])}
        />
      )}
      <span className="font-normal tabular-nums">{props.count}</span>
      {attention === null ? null : (
        <span className="sr-only">{m(SHELF_ATTENTION_LABEL[attention])}</span>
      )}
    </>
  );
}

export function SidebarSectionHeader(props: {
  marker: keyof typeof SHELF_BY_HEADER;
  label: string;
  /** A muted trailing count. */
  count: number;
  /** The threads the collapsed section hides, to flag one that needs the user. */
  hiddenThreads?: readonly EnvironmentThreadShell[] | undefined;
  className?: string;
  dragging?: boolean;
  isDropTarget?: boolean;
  toggle: { expanded: boolean; onToggle: () => void };
}) {
  const shelf = SHELF_BY_HEADER[props.marker];
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className={cn("relative mx-0.5 h-8", props.className)}
    >
      <CollapsibleSectionHeader
        data-sidebar-row={`shelf:${shelf}`}
        onClick={props.toggle.onToggle}
        expanded={props.toggle.expanded}
        tone={props.isDropTarget ? "accent" : props.dragging ? "emphasized" : "muted"}
        data-testid={`sidebar-${shelf}-shelf-toggle`}
        accessory={
          props.hiddenThreads?.length ? (
            <ShelfCountWithAttention count={props.count} hiddenThreads={props.hiddenThreads} />
          ) : (
            <span className="font-normal tabular-nums">{props.count}</span>
          )
        }
      >
        {props.label}
      </CollapsibleSectionHeader>
    </SortableSidebarMarker>
  );
}
