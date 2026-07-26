# Controlled Thread reservation materialization boundary

This foundation reserves identity only. It must never be consumed by a reactor
and does not create an orchestration thread, provider session, command, message,
or turn.

A later, separately reviewed materialization slice must:

- use the already reserved `threadId`;
- create `thread.created` and `thread.agent-control-bound` atomically;
- assign Agent-Control authority server-side;
- revalidate current task, stage, lease, fence, and worktree authority;
- enter `useReadyWorktree` again immediately around materialization;
- fingerprint the complete internal materialization command in its receipt; and
- avoid every Create-then-Bind crash window.

None of that boundary is implemented by this directory.
