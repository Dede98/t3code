import type { ProviderInstanceId, ProviderRuntimeEvent } from "@t3tools/contracts";

export type ProviderRuntimeEventEmission<
  Event extends ProviderRuntimeEvent = ProviderRuntimeEvent,
> = Event extends ProviderRuntimeEvent ? Omit<Event, "providerInstanceId"> : never;

export type BoundProviderRuntimeEvent = ProviderRuntimeEvent & {
  readonly providerInstanceId: ProviderInstanceId;
};

export function bindProviderRuntimeEvent(
  providerInstanceId: ProviderInstanceId,
  event: ProviderRuntimeEventEmission,
): BoundProviderRuntimeEvent {
  return {
    ...event,
    providerInstanceId,
  } as BoundProviderRuntimeEvent;
}
