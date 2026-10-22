import { withHtmlTheme } from "@t3tools/shared/htmlRender";

import { useHtmlRenderTheme } from "../../hooks/useHtmlRenderTheme";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";

/**
 * Starts a turn with the sender's theme in the message context, so HTML the
 * agent renders or exports during that turn matches this client.
 */
export function useStartThreadTurn() {
  const htmlTheme = useHtmlRenderTheme();
  const startThreadTurn = useOrchestrationCommand(threadEnvironment.startTurn, {
    reportFailure: false,
  });
  return (request: Parameters<typeof startThreadTurn>[0]) =>
    startThreadTurn({
      ...request,
      input: {
        ...request.input,
        message: {
          ...request.input.message,
          context: withHtmlTheme(request.input.message.context, htmlTheme),
        },
      },
    });
}
