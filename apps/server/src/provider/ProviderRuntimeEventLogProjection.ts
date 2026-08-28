import type { ProviderRuntimeEvent } from "@t3tools/contracts";

const technicalEventFields = (event: ProviderRuntimeEvent) => ({
  eventId: event.eventId,
  provider: event.provider,
  ...(event.providerInstanceId === undefined
    ? {}
    : { providerInstanceId: event.providerInstanceId }),
  threadId: event.threadId,
  createdAt: event.createdAt,
  ...(event.turnId === undefined ? {} : { turnId: event.turnId }),
  ...(event.itemId === undefined ? {} : { itemId: event.itemId }),
  ...(event.requestId === undefined ? {} : { requestId: event.requestId }),
  ...(event.providerRefs === undefined
    ? {}
    : {
        providerRefs: {
          ...(event.providerRefs.providerTurnId === undefined
            ? {}
            : { providerTurnId: event.providerRefs.providerTurnId }),
          ...(event.providerRefs.providerItemId === undefined
            ? {}
            : { providerItemId: event.providerRefs.providerItemId }),
          ...(event.providerRefs.providerRequestId === undefined
            ? {}
            : { providerRequestId: event.providerRefs.providerRequestId }),
        },
      }),
});

/**
 * Build the observability-only projection written to the canonical provider
 * event log. Assistant output remains available on the original runtime event
 * for ingestion and evidence capture; this projection is never an authority or
 * replay source.
 */
export function projectProviderRuntimeEventForCanonicalLog(event: ProviderRuntimeEvent): unknown {
  if (
    (event.type === "item.started" ||
      event.type === "item.updated" ||
      event.type === "item.completed") &&
    event.payload.itemType === "assistant_message"
  ) {
    return {
      ...technicalEventFields(event),
      type: event.type,
      payload: {
        itemType: event.payload.itemType,
        ...(event.payload.status === undefined ? {} : { status: event.payload.status }),
      },
    };
  }

  if (event.type === "content.delta" && event.payload.streamKind === "assistant_text") {
    return {
      ...technicalEventFields(event),
      type: event.type,
      payload: {
        streamKind: event.payload.streamKind,
        ...(event.payload.contentIndex === undefined
          ? {}
          : { contentIndex: event.payload.contentIndex }),
        ...(event.payload.summaryIndex === undefined
          ? {}
          : { summaryIndex: event.payload.summaryIndex }),
      },
    };
  }

  return event;
}
