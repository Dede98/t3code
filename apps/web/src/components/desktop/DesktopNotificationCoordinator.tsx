import { safeErrorLogAttributes } from "@t3tools/client-runtime/errors";
import { scopedProjectKey, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { useNavigate, useParams } from "@tanstack/react-router";
import { projectThreadAwareness } from "@t3tools/shared/agentAwareness";
import { useEffect, useMemo, useRef } from "react";

import { setActiveEnvironmentId, useProjects, useThreadShells } from "../../state/entities";
import { resolveThreadRouteRef } from "../../threadRoutes";
import {
  createDesktopNotificationObservation,
  desktopNotificationKindForTransition,
  type DesktopNotificationObservation,
} from "./DesktopNotificationCoordinator.logic";

const NOTIFICATION_ERROR_SCOPE = "[DESKTOP_NOTIFICATION]";

export function DesktopNotificationCoordinator() {
  const navigate = useNavigate();
  const activeThreadRef = useParams({
    strict: false,
    select: (params) => resolveThreadRouteRef(params),
  });
  const projects = useProjects();
  const threads = useThreadShells();
  const previousByThreadRef = useRef(new Map<string, DesktopNotificationObservation>());
  const projectByKey = useMemo(
    () =>
      new Map(
        projects.map((project) => [
          scopedProjectKey({
            environmentId: project.environmentId,
            projectId: project.id,
          }),
          project,
        ]),
      ),
    [projects],
  );

  useEffect(() => {
    const bridge = window.desktopBridge;
    if (!bridge) {
      return;
    }

    const currentThreadKeys = new Set<string>();
    const activeThreadKey = activeThreadRef ? scopedThreadKey(activeThreadRef) : null;
    for (const thread of threads) {
      const threadKey = scopedThreadKey({
        environmentId: thread.environmentId,
        threadId: thread.id,
      });
      currentThreadKeys.add(threadKey);
      const project = projectByKey.get(
        scopedProjectKey({
          environmentId: thread.environmentId,
          projectId: thread.projectId,
        }),
      );
      if (!project) {
        continue;
      }

      const awareness = projectThreadAwareness({
        environmentId: thread.environmentId,
        project,
        thread,
      });
      if (!awareness) {
        continue;
      }

      const current = createDesktopNotificationObservation(thread, awareness.phase);
      const previous = previousByThreadRef.current.get(threadKey);
      previousByThreadRef.current.set(threadKey, current);
      const kind = desktopNotificationKindForTransition(previous, current);
      if (!kind) {
        continue;
      }

      const isVisibleActiveThread =
        activeThreadKey === threadKey &&
        document.visibilityState === "visible" &&
        document.hasFocus();
      if (isVisibleActiveThread) {
        continue;
      }

      void bridge
        .showDesktopNotification({
          environmentId: thread.environmentId,
          threadId: thread.id,
          kind,
          projectTitle: project.title,
          threadTitle: thread.title,
        })
        .catch((error) => {
          console.error(`${NOTIFICATION_ERROR_SCOPE} show failed`, {
            operation: "show",
            environmentId: thread.environmentId,
            threadId: thread.id,
            kind,
            ...safeErrorLogAttributes(error),
          });
        });
    }

    for (const threadKey of previousByThreadRef.current.keys()) {
      if (!currentThreadKeys.has(threadKey)) {
        previousByThreadRef.current.delete(threadKey);
      }
    }
  }, [activeThreadRef, projectByKey, threads]);

  useEffect(() => {
    const onDesktopNotificationClick = window.desktopBridge?.onDesktopNotificationClick;
    if (!onDesktopNotificationClick) {
      return;
    }

    return onDesktopNotificationClick((target) => {
      setActiveEnvironmentId(target.environmentId);
      void navigate({
        to: "/$environmentId/$threadId",
        params: target,
      });
    });
  }, [navigate]);

  return null;
}
