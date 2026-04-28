package com.cryptolake.verify.cli;

import com.cryptolake.verify.maintenance.MaintenanceIntent;
import com.cryptolake.verify.maintenance.MaintenanceWriter;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Records a planned maintenance intent to PostgreSQL.
 *
 * <p>Ports {@code @cli.command("mark-maintenance") def mark_maintenance(...)} from {@code
 * verify.py:401-430}.
 *
 * <p>Timestamps use {@link Instant#now()#toString()} — ISO-8601 UTC with Z suffix (Tier 5 F1). JDBC
 * write is synchronous on a virtual-thread-compatible path (Tier 5 A2).
 *
 * <p>Tier 5 K3 — returns exit code; never {@link System#exit} from command body.
 */
@Command(name = "mark-maintenance", description = "Record a planned maintenance intent.")
public final class MarkMaintenanceCommand implements Callable<Integer> {

  @Option(names = "--db-url", required = true, description = "PostgreSQL connection URL")
  private String dbUrl;

  @Option(names = "--scope", required = true, description = "Scope of maintenance")
  private String scope;

  @Option(
      names = "--maintenance-id",
      required = true,
      description = "Unique maintenance identifier")
  private String maintenanceId;

  @Option(names = "--reason", required = true, description = "Reason for maintenance")
  private String reason;

  @Option(
      names = "--ttl-minutes",
      defaultValue = "60",
      description = "Minutes until intent expires (default: 60)")
  private int ttlMinutes;

  @Override
  public Integer call() throws SQLException {
    Instant now = Instant.now(); // Tier 5 F1 — Instant.now()
    Instant expiresAt = now.plus(Duration.ofMinutes(ttlMinutes));

    MaintenanceIntent intent =
        new MaintenanceIntent(
            maintenanceId,
            scope,
            "cli", // plannedBy — matches Python's `planned_by="cli"`
            reason,
            now.toString(), // ISO-8601 UTC with Z suffix (Tier 5 F1)
            expiresAt.toString());

    MaintenanceWriter.write(dbUrl, intent);
    System.out.println(
        "Maintenance intent recorded: "
            + maintenanceId
            + " (scope="
            + scope
            + ", ttl="
            + ttlMinutes
            + "m)");
    return 0;
  }
}
