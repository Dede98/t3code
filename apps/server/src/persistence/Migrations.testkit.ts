import * as Migrator from "effect/sql/Migrator";

import { forkMigrationEntries } from "./Migrations.ts";

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
