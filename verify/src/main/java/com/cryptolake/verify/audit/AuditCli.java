package com.cryptolake.verify.audit;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Picocli sub-tree for end-to-end data-integrity audit commands.
 *
 * <p>Top-level parent for the {@code audit} command group. When invoked without a subcommand,
 * prints usage to {@link System#out}.
 */
@Command(
    name = "audit",
    description = "Audit data integrity end-to-end.",
    subcommands = {AuditFilesCommand.class})
public final class AuditCli implements Runnable {

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }
}
