// chronos migrate — CLI surface over the auto-apply migration
// infrastructure.
//
// Chronos applies its schema on first Open via ensureSchema (sqlite)
// or the postgres equivalent. That's fine for greenfield deploys but
// it leaves operators without an explicit "what would change on
// upgrade?" tool. `chronos migrate` plugs that gap:
//
//	chronos migrate status   — current schema version + listed steps
//	chronos migrate up       — opens the DB (triggers auto-apply),
//	                           reports the new version
//
// Down is deliberately not supported in this revision: the embedded
// migrations are forward-only because they never DROP columns and the
// detector code reads every column they add. Adding a real down path
// requires per-step .down.sql files; that's the follow-up the issue
// hints at when it says "establishing the pattern before the first
// breaking change".

package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/store"
)

// migrationStep is one declared version step. The list lives in code,
// not on disk, so the binary always knows what it'll apply — useful
// for `migrate status` output even when the database is unreachable.
type migrationStep struct {
	Version     int
	Description string
}

// chronosMigrations is the canonical version ladder. Append a new
// entry whenever a schema-changing PR lands so `migrate status`
// stays accurate.
var chronosMigrations = []migrationStep{
	{1, "initial schema (entity_states, signals, signal_evidence) — auto-applied via ensureSchema on Open"},
	{2, "signals.explanation JSONB column (Refs #21) — auto-applied"},
	{3, "signals.confidence_class TEXT column (Refs #24) — auto-applied"},
	{4, "fixed-width UTC timestamps on SQLite/libSQL (nine fractional digits) — auto-applied via normalizeTimestampText on Open"},
}

// migrateStatusReport is the per-command output shape so tests can
// assert against a stable struct instead of parsing stdout.
type migrateStatusReport struct {
	DSN            string
	CurrentVersion int
	LatestVersion  int
	PendingSteps   []migrationStep
	HistorySteps   []migrationStep
}

func runMigrate(args []string) error {
	if len(args) == 0 {
		return NewUserError("missing subcommand: chronos migrate <status|up>")
	}
	switch args[0] {
	case "status":
		return runMigrateStatus()
	case "up":
		return runMigrateUp()
	case "down":
		return NewUserError("down migrations are not supported yet; see docs for the forward-only contract")
	default:
		return NewUserError("unknown subcommand %q (expected status / up)", args[0])
	}
}

func runMigrateStatus() error {
	cfg := config.Default()
	dsn, err := resolveDSN(cfg)
	if err != nil {
		return err
	}
	report := buildMigrateStatusReport(dsn, currentMigrationVersion())
	renderMigrateStatus(report)
	return nil
}

func runMigrateUp() error {
	cfg := config.Default()
	dsn, err := resolveDSN(cfg)
	if err != nil {
		return err
	}
	// Opening the store triggers the bundled migration code path.
	conn, err := store.Open(context.Background(), dsn)
	if err != nil {
		return fmt.Errorf("open store (migrate up): %w", err)
	}
	defer func() { _ = conn.Close() }()

	report := buildMigrateStatusReport(dsn, currentMigrationVersion())
	fmt.Printf("chronos migrate up — applied through version %d\n", report.LatestVersion)
	renderMigrateStatus(report)
	return nil
}

// buildMigrateStatusReport composes the report struct from a DSN and
// a "currently applied" version number. Split out from runMigrateStatus
// so tests can assert against the report without reaching for a real DB.
func buildMigrateStatusReport(dsn string, current int) migrateStatusReport {
	report := migrateStatusReport{
		DSN:            dsn,
		CurrentVersion: current,
	}
	if len(chronosMigrations) > 0 {
		report.LatestVersion = chronosMigrations[len(chronosMigrations)-1].Version
	}
	for _, step := range chronosMigrations {
		if step.Version <= current {
			report.HistorySteps = append(report.HistorySteps, step)
		} else {
			report.PendingSteps = append(report.PendingSteps, step)
		}
	}
	sort.Slice(report.HistorySteps, func(i, j int) bool {
		return report.HistorySteps[i].Version < report.HistorySteps[j].Version
	})
	sort.Slice(report.PendingSteps, func(i, j int) bool {
		return report.PendingSteps[i].Version < report.PendingSteps[j].Version
	})
	return report
}

// renderMigrateStatus prints the report to stdout. Kept separate from
// the report builder so tests can verify the data shape without
// scraping stdout.
func renderMigrateStatus(report migrateStatusReport) {
	fmt.Printf("DSN:              %s\n", report.DSN)
	fmt.Printf("Latest version:   %d\n", report.LatestVersion)
	fmt.Printf("Applied through:  %d (assumed; chronos auto-applies on Open)\n", report.CurrentVersion)
	if len(report.HistorySteps) > 0 {
		fmt.Println("Applied steps:")
		for _, s := range report.HistorySteps {
			fmt.Printf("  v%d  %s\n", s.Version, s.Description)
		}
	}
	if len(report.PendingSteps) > 0 {
		fmt.Println("Pending steps (run `chronos migrate up`):")
		for _, s := range report.PendingSteps {
			fmt.Printf("  v%d  %s\n", s.Version, s.Description)
		}
	} else {
		fmt.Println("No pending steps.")
	}
}

// currentMigrationVersion reports what version this binary knows it
// has applied. Since chronos auto-applies on Open, the running binary
// is always at the latest version it knows about — there is no
// separate schema_migrations table yet. When the project moves to a
// goose-style runner this becomes a real query.
func currentMigrationVersion() int {
	if len(chronosMigrations) == 0 {
		return 0
	}
	return chronosMigrations[len(chronosMigrations)-1].Version
}
