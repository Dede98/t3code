import type {
  ExternalMcpServerConfig,
  ProviderInstanceId,
  ServerSettings,
} from "@t3tools/contracts";

export interface ExternalMcpServer extends ExternalMcpServerConfig {
  readonly name: string;
}

export function resolveExternalMcpServers(
  settings: Pick<ServerSettings, "externalMcpServers">,
  instanceId: ProviderInstanceId,
): ReadonlyArray<ExternalMcpServer> {
  return Object.entries(settings.externalMcpServers).flatMap(([name, server]) => {
    if (!server.enabled) return [];
    if (server.providerInstances && !server.providerInstances.includes(instanceId)) return [];
    return [{ name, ...server }];
  });
}

export function externalMcpHeadersRecord(server: ExternalMcpServer): Record<string, string> {
  return Object.fromEntries(server.headers.map((header) => [header.name, header.value]));
}
