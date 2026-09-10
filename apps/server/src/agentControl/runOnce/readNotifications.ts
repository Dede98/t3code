import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

/** Read-side invalidation only; never drives execution or recovery. */
export const AgentControlRunOnceReadNotifications = Context.Reference<{
  readonly publishProject: (projectId: string) => Effect.Effect<void>;
  readonly subscribeProjects: Effect.Effect<Stream.Stream<string>, never, Scope.Scope>;
  readonly publish: (handoffId: string) => Effect.Effect<void>;
  readonly subscribe: Effect.Effect<Stream.Stream<string>, never, Scope.Scope>;
}>("t3/agentControl/runOnce/ReadNotifications", {
  defaultValue: () => ({
    publishProject: () => Effect.void,
    subscribeProjects: Effect.succeed(Stream.never),
    publish: () => Effect.void,
    subscribe: Effect.succeed(Stream.never),
  }),
});

export const AgentControlRunOnceReadNotificationsLive = Layer.effect(
  AgentControlRunOnceReadNotifications,
  Effect.gen(function* () {
    const projects = yield* PubSub.unbounded<string>();
    const updates = yield* PubSub.unbounded<string>();
    return {
      publishProject: (projectId: string) =>
        PubSub.publish(projects, projectId).pipe(Effect.asVoid),
      subscribeProjects: PubSub.subscribe(projects).pipe(Effect.map(Stream.fromSubscription)),
      publish: (handoffId: string) => PubSub.publish(updates, handoffId).pipe(Effect.asVoid),
      subscribe: PubSub.subscribe(updates).pipe(Effect.map(Stream.fromSubscription)),
    };
  }),
);
