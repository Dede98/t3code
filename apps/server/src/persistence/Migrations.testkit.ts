import * as Migrator from "effect/sql/Migrator";

import { forkMigrationEntries, migrationEntries } from "./Migrations.ts";

// Official upstream's ledger: a fresh fork ledger without the Claude store it records at 57.
const upstreamMigrationEntries = migrationEntries
  .filter(([, name]) => name !== "ClaudeSessionStore")
  .map(([id, name, migration]) => [id > 57 ? id - 1 : id, name, migration] as const);

/** Migrates a test database as an official upstream build would, with upstream's ids. */
export const seedUpstreamMigrations = (through: number) =>
  Migrator.make({})({
    loader: Migrator.fromRecord(
      Object.fromEntries(
        upstreamMigrationEntries
          .filter(([id]) => id <= through)
          .map(([id, name, migration]) => [`${id}_${name}`, migration]),
      ),
    ),
  });

/**
 * Migrates a test database along the fork ledger lineage, which records
 * `ClaudeSessionStore` at 33 and every later legacy migration one id higher.
 * `runMigrations` follows that lineage only once 33 is recorded, so a test
 * that uses fork numbering seeds its starting point here.
 */
export const seedForkMigrations = (through: number) =>
  Migrator.make({})({
    loader: Migrator.fromRecord(
      Object.fromEntries(
        forkMigrationEntries
          .filter(([id]) => id <= through)
          .map(([id, name, migration]) => [`${id}_${name}`, migration]),
      ),
    ),
  });
