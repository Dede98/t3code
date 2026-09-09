import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Argument, Command } from "effect/unstable/cli";
import * as CliError from "effect/unstable/cli/CliError";

import * as NetService from "@t3tools/shared/Net";
import packageJson from "../../package.json" with { type: "json" };
import { authCommand } from "./auth.ts";
import { appCommand } from "./app.ts";
import { connectCommand } from "./connect.ts";
import { pairCommand } from "./pair.ts";
import { hasCloudPublicConfig } from "../cloud/publicConfig.ts";
import { sharedServerCommandFlags } from "./config.ts";
import { projectCommand } from "./project.ts";
import { runServerCommand, serveCommand, startCommand } from "./server.ts";
import { serviceCommand } from "./service.ts";
import { servicePreflightCommand } from "./servicePreflight.ts";
import { themeCommand } from "./theme.ts";
import { triageCommand } from "./triage.ts";

const CliRuntimeLayer = Layer.mergeAll(NodeServices.layer, NetService.layer);

const connectPublicConfigMissingMessage =
  "T3 Connect commands are unavailable: this build is missing T3 Connect public configuration.";

class ConnectPublicConfigMissingError extends CliError.UserError {
  override get message() {
    return connectPublicConfigMissingMessage;
  }
}

const connectUnavailableCommand = Command.make("connect", {
  command: Argument.string("command").pipe(Argument.variadic),
}).pipe(
  Command.withDescription("T3 Connect is unavailable in builds without public configuration."),
  Command.unlisted,
  Command.withHandler(() =>
    Effect.fail(
      new CliError.ShowHelp({
        commandPath: ["t3", "connect"],
        errors: [new ConnectPublicConfigMissingError({ cause: connectPublicConfigMissingMessage })],
      }),
    ),
  ),
);

export const makeCli = ({ cloudEnabled = hasCloudPublicConfig } = {}) =>
  Command.make("t3", { ...sharedServerCommandFlags }).pipe(
    Command.withDescription("Run the T3 Code server."),
    Command.withHandler((flags) => runServerCommand(flags)),
    Command.withSubcommands([
      startCommand,
      serveCommand,
      appCommand,
      pairCommand,
      authCommand,
      projectCommand,
      serviceCommand,
      servicePreflightCommand,
      themeCommand,
      triageCommand,
      cloudEnabled ? connectCommand : connectUnavailableCommand,
    ]),
  );

export const cli = makeCli();

export const runCliMain = () =>
  Command.run(cli, { version: packageJson.version }).pipe(
    Effect.scoped,
    Effect.provide(CliRuntimeLayer),
    NodeRuntime.runMain,
  );
