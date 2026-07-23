import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable operational state for GitHub Observe scheduling. There is
 * intentionally no foreign key: projection rebuilds and historical Agent
 * Control events must remain independent from this disposable recovery table.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_github_scheduler_states (
      project_id TEXT PRIMARY KEY,
      state_json TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation >= 1),
      last_github_event_sequence INTEGER NOT NULL CHECK (last_github_event_sequence >= 0),
      activity TEXT NOT NULL CHECK (activity IN ('active', 'suspended')),
      circuit_state TEXT NOT NULL CHECK (circuit_state IN ('closed', 'open', 'half-open')),
      consecutive_failures INTEGER NOT NULL CHECK (consecutive_failures >= 0),
      last_attempt_at TEXT,
      next_attempt_at TEXT,
      cooldown_until TEXT,
      reason_code TEXT CHECK (
        reason_code IS NULL OR reason_code IN (
          'github-unavailable',
          'github-authentication',
          'github-timeout',
          'github-command-failed',
          'github-decode-failed',
          'pagination-overflow',
          'timeline-incomplete',
          'repository-identity-changed',
          'issue-repository-changed',
          'poll-in-progress',
          'revision-conflict',
          'project-unavailable',
          'tracker-not-configured',
          'internal-coordination-error'
        )
      ),
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_github_scheduler_next_attempt
    ON agent_control_github_scheduler_states(activity, next_attempt_at)
  `;
});
