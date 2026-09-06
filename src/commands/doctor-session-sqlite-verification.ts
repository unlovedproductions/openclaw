/** Offline destination ownership and conservative adoption of historical import evidence. */
import fs from "node:fs";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { withSqliteSessionImportStage } from "../config/sessions/session-accessor.sqlite-import-stage.js";
import { getSessionKysely } from "../config/sessions/session-accessor.sqlite-scope.js";
import { readFileDescriptorBoundedSync } from "../infra/boundary-file-read.js";
import { executeSqliteQueryTakeFirstSync, iterateSqliteQuerySync } from "../infra/kysely-sync.js";
import { resolveSqliteDatabaseFilePaths } from "../infra/sqlite-files.js";
import { normalizeLegacySessionEntryDelivery } from "../infra/state-migrations.legacy-session-store.js";
import { migrateLegacySessionCreator } from "../state/creator-namespace-migration.js";
import { withOpenClawAgentDatabaseReadOnly } from "../state/openclaw-agent-db-readonly.js";
import { inspectOpenClawAgentDatabaseOwner } from "../state/openclaw-agent-db.js";
import type { OpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import {
  readMigrationArtifactIdentity,
  type MigrationArtifact,
} from "./doctor-session-sqlite-artifact.js";
import {
  canonicalMigrationFilePath,
  uniqueRestoreMoves,
  type SessionSqliteMigrationMove,
  type SessionSqliteMigrationTargetManifest,
} from "./doctor-session-sqlite-migration-run.js";
import {
  createTranscriptEventReader,
  readTranscriptFingerprint,
  resolveLegacyTranscriptPaths,
} from "./doctor-session-sqlite-readers.js";
import { assertDoctorSqliteMaintenancePathsNotAliased } from "./doctor-sqlite-maintenance-lock.js";

type HistoricalSessionProof = "match" | "absent";

/** Keep one owner proof per database; fence in-place writes and sidecar changes after awaits. */
export function createRecoveryDestinationVerifier(stateDir: string) {
  const destinations = new Map<
    string,
    { agentId: string; files: (fs.BigIntStats | undefined)[] }
  >();
  return (refs: Iterable<{ target: SessionSqliteMigrationTargetManifest }>) => {
    for (const { target } of refs) {
      const paths = resolveSqliteDatabaseFilePaths(target.sqlitePath);
      assertDoctorSqliteMaintenancePathsNotAliased("update recovery cleanup", paths, [stateDir]);
      const expected = destinations.get(target.sqlitePath);
      if (!expected) {
        const owner = inspectOpenClawAgentDatabaseOwner(target.sqlitePath);
        if (owner.status !== "owned" || owner.agentId !== target.agentId) {
          throw new Error("destination database ownership cannot be verified");
        }
      }
      // Even a read-only SQLite connection can create WAL/SHM files. Establish the baseline
      // after owner inspection closes it; later checks only stat, never reopen or hash the DB.
      const files = paths.map((file) =>
        fs.lstatSync(file, { bigint: true, throwIfNoEntry: false }),
      );
      if (
        !files[0]?.isFile() ||
        files.some((file) => file && (!file.isFile() || file.nlink !== 1n)) ||
        (expected &&
          (expected.agentId !== target.agentId ||
            files.some((file, index) =>
              (["dev", "ino", "ctimeNs", "mtimeNs", "size"] as const).some(
                (key) => file?.[key] !== expected.files[index]?.[key],
              ),
            )))
      ) {
        throw new Error("Recovery destination database changed; preview cleanup again.");
      }
      if (!expected) {
        destinations.set(target.sqlitePath, { agentId: target.agentId, files });
      }
    }
  };
}

function readHistoricalIndex(archivePath: string): Record<string, unknown> {
  readMigrationArtifactIdentity(archivePath);
  const fd = fs.openSync(archivePath, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  let index: unknown;
  try {
    index = JSON.parse(readFileDescriptorBoundedSync(fd, fs.fstatSync(fd).size).toString("utf8"));
  } finally {
    fs.closeSync(fd);
  }
  if (!isRecord(index)) {
    throw new Error("historical-import-malformed-index");
  }
  return index;
}

function parseHistoricalSessionEntry(
  key: unknown,
  raw: unknown,
): { key: string; sessionId: string; entry: Record<string, unknown> } {
  if (
    typeof key !== "string" ||
    !isRecord(raw) ||
    typeof raw.sessionId !== "string" ||
    !raw.sessionId.trim() ||
    typeof raw.updatedAt !== "number"
  ) {
    throw new Error("historical-import-malformed-entry");
  }
  return {
    key,
    sessionId: raw.sessionId,
    entry: { ...raw, sessionId: raw.sessionId, updatedAt: raw.updatedAt },
  };
}

function isHistoricalSessionAbsent(
  database: Pick<OpenClawAgentDatabase, "db">,
  sessionKey: string,
  sessionId: string,
): boolean {
  const db = getSessionKysely(database.db);
  const byKey = executeSqliteQueryTakeFirstSync(
    database.db,
    db.selectFrom("session_nodes").select("session_key").where("session_key", "=", sessionKey),
  );
  if (byKey) {
    return false;
  }
  const byId = executeSqliteQueryTakeFirstSync(
    database.db,
    db
      .selectFrom("session_nodes")
      .select("session_key")
      .where("current_session_id", "=", sessionId),
  );
  if (byId) {
    return false;
  }
  const transcript = executeSqliteQueryTakeFirstSync(
    database.db,
    db.selectFrom("transcript_events").select("session_id").where("session_id", "=", sessionId),
  );
  if (transcript) {
    return false;
  }
  const trajectory = executeSqliteQueryTakeFirstSync(
    database.db,
    db
      .selectFrom("trajectory_runtime_events")
      .select("session_id")
      .where("session_id", "=", sessionId),
  );
  return !trajectory;
}

function proveHistoricalSessionEntry(
  database: Pick<OpenClawAgentDatabase, "db">,
  sessionKey: string,
  raw: Record<string, unknown>,
  sessionId: string,
): HistoricalSessionProof {
  const db = getSessionKysely(database.db);
  const row = executeSqliteQueryTakeFirstSync(
    database.db,
    db
      .selectFrom("session_nodes")
      .select(["current_session_id", "entry_json"])
      .where("session_key", "=", sessionKey),
  );
  if (!row) {
    if (!isHistoricalSessionAbsent(database, sessionKey, sessionId)) {
      throw new Error("historical-import-content-mismatch");
    }
    return "absent";
  }
  if (row.current_session_id !== sessionId) {
    throw new Error("historical-import-content-mismatch");
  }
  const current: unknown = JSON.parse(row.entry_json);
  const normalized = migrateLegacySessionCreator(normalizeLegacySessionEntryDelivery(raw));
  if (
    !isRecord(current) ||
    Object.entries(normalized).some(
      ([field, value]) =>
        field !== "sessionFile" && JSON.stringify(current[field]) !== JSON.stringify(value),
    )
  ) {
    throw new Error("historical-import-content-mismatch");
  }
  return "match";
}

function proveHistoricalTranscriptEvents(
  database: Pick<OpenClawAgentDatabase, "db">,
  archivePath: string,
  sourcePath: string,
  sessionId: string,
): void {
  const db = getSessionKysely(database.db);
  const complete = withSqliteSessionImportStage((stage) => {
    let seq = 0;
    const validate = createTranscriptEventReader(
      archivePath,
      sessionId,
      false,
      readTranscriptFingerprint(archivePath),
      sourcePath,
    )((event) => stage.append(0, seq++, JSON.stringify(event), null));
    const repair = stage.repairLegacyTranscript(0);
    // Old metadata cannot prove that a now-discarded branch was deliberately retired then.
    if (repair.repaired || !repair.recognized) {
      return false;
    }
    for (const event of iterateSqliteQuerySync(
      database.db,
      db.selectFrom("transcript_events").select("event_json").where("session_id", "=", sessionId),
    )) {
      stage.addSeen(event.event_json);
    }
    for (const event of stage.rows(0)) {
      if (!stage.contains(event.eventJson)) {
        return false;
      }
    }
    validate();
    return true;
  });
  if (!complete) {
    throw new Error("historical-import-content-mismatch");
  }
}

export function verifyHistoricalMigrationArtifact(params: {
  target: SessionSqliteMigrationTargetManifest;
  move: SessionSqliteMigrationMove;
  env: NodeJS.ProcessEnv;
}): MigrationArtifact {
  const { target, move, env } = params;
  if (move.kind !== "legacy-store" && move.kind !== "transcript" && move.kind !== "trajectory") {
    throw new Error("historical-import-proof-unavailable");
  }
  const identity = readMigrationArtifactIdentity(move.archivePath);
  const indexMove =
    move.kind === "legacy-store"
      ? move
      : uniqueRestoreMoves(target).find((item) => item.kind === "legacy-store");
  if (!indexMove) {
    throw new Error("historical-import-proof-unavailable");
  }
  const index = readHistoricalIndex(indexMove.archivePath);
  const transcriptMove =
    move.kind === "transcript"
      ? move
      : move.kind === "trajectory" && move.sessionKey
        ? uniqueRestoreMoves(target).find(
            (item) => item.kind === "transcript" && item.sessionKey === move.sessionKey,
          )
        : undefined;
  const entries =
    move.kind === "legacy-store"
      ? Object.entries(index)
      : move.sessionKey
        ? [[move.sessionKey, index[move.sessionKey]]]
        : [];
  if ((move.kind === "transcript" || move.kind === "trajectory") && entries.length !== 1) {
    throw new Error("historical-import-proof-unavailable");
  }
  if (move.kind === "trajectory" && !transcriptMove) {
    throw new Error("historical-import-proof-unavailable");
  }
  const dependencies = new Set(
    move.kind === "legacy-store"
      ? uniqueRestoreMoves(target)
          .filter((item) => item.kind === "transcript")
          .map((item) => item.sourcePath)
      : move.kind === "trajectory" && transcriptMove
        ? [transcriptMove.sourcePath]
        : [],
  );
  let matched = false;
  const verified = withOpenClawAgentDatabaseReadOnly(
    (database) => {
      for (const [key, raw] of entries) {
        const parsed = parseHistoricalSessionEntry(key, raw);
        const proof = proveHistoricalSessionEntry(
          database,
          parsed.key,
          parsed.entry,
          parsed.sessionId,
        );
        if (proof === "absent") {
          continue;
        }
        matched = true;
        if (move.kind === "legacy-store") {
          // Historical indexes can refer to transcripts published by an earlier run.
          // Reuse the producer's path contract rather than requiring same-run moves.
          for (const source of resolveLegacyTranscriptPaths(target, parsed.entry)
            .transcriptDependencies) {
            dependencies.add(canonicalMigrationFilePath(source));
          }
          continue;
        }
        if (move.kind === "transcript") {
          proveHistoricalTranscriptEvents(
            database,
            move.archivePath,
            move.sourcePath,
            parsed.sessionId,
          );
          continue;
        }
        // Trajectory sidecars follow the sibling transcript's session proof only.
        // Do not replay trajectory JSON into transcript_events.
        if (transcriptMove) {
          proveHistoricalTranscriptEvents(
            database,
            transcriptMove.archivePath,
            transcriptMove.sourcePath,
            parsed.sessionId,
          );
        }
      }
      return true;
    },
    { agentId: target.agentId, path: target.sqlitePath, env },
  );
  if (!verified.found || !verified.value) {
    throw new Error("historical-import-proof-unavailable");
  }
  return {
    identity,
    classification: "imported",
    reason: matched ? "verified-historical-import" : "verified-retired-session-original",
    dependencies: [...dependencies],
    disposal: { state: "retained" },
  };
}
