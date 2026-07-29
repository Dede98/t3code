# Controlled Thread reservation materialization boundary

This directory owns reservation identity and the server-internal coordinator
that materializes an already prepared reservation. It must never be consumed
by a reactor and never creates a provider session, provider command, turn,
message, task execution, terminal, or process.

`AgentControlControlledThreadMaterializationCoordinator.materializeInitial`
accepts only a coordinator CommandId, ProjectId, and canonical ReservationId.
Before every first commit it resolves Project mode, complete Task/source,
initial Planning Stage/Attempt, current runtime-owned Lease/Fence, complete
reservation history, Ready Worktree, and current planner runtime policy
server-side. It re-enters `useReadyWorktree` and repeats the authoritative
resolution immediately inside the caller-owned transaction.

The coordinator drives only:

```text
prepared -> materializing -> bound
```

The transaction appends both reservation transitions, invokes the internal
`thread.agent-control.materialize` primitive for `thread.created@1` and
`thread.agent-control-bound@2`, persists both projections and both evidence
families, and finishes with the coordinator Accepted marker as the final
application SQL statement. Neither event family is published until that outer
transaction commits.

The orchestration primitive remains available to the existing direct
`dispatchAgentControl` path. Its trust boundary is intentionally narrow:

- only server-internal orchestration and coordinator layers may invoke it;
- it validates canonical identity and the initial Planning/controlled shape;
- its direct path atomically commits both events, the complete thread
  projection, immutable intent evidence, and one receipt; and
- the coordinator-owned form writes into the caller transaction and never
  publishes from a nested savepoint.

Accepted coordinator replay validates the immutable coordinator fingerprint,
all three reservation events and projection, the complete orchestration stream
and projection, both intent/receipt families, and both Accepted markers before
returning. It runs before current Project, policy, provider, Lease, or Worktree
checks. Release, takeover, invalidation, provider lifecycle, scheduling,
execution, GitHub writes, RPC mutation, and client UI remain outside this
boundary.
