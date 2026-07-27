# Controlled Thread reservation materialization boundary

This directory still reserves identity only. It must never be consumed by a
reactor and does not create an orchestration thread, provider session, command,
message, or turn.

The separately reviewed server-internal orchestration primitive
`thread.agent-control.materialize` can atomically create and bind the already
resolved identity. Its trust boundary is intentionally narrow:

- only `dispatchAgentControl` may invoke it;
- it validates canonical identity and the initial Planning/controlled shape;
- it atomically commits both events, the complete thread projection, immutable
  intent evidence, and one receipt; and
- it trusts the server-side caller for the current Reservation, Task, Stage,
  Lease, Fence, and Worktree histories.

A later coordinator must revalidate those histories, enter `useReadyWorktree`
again immediately around dispatch, and only then call the primitive. This slice
does not add that coordinator, change reservation status, or start a provider,
turn, task, scheduler, reactor, or GitHub write.
