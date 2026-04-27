package com.cryptolake.verify;

import com.cryptolake.verify.cli.ManifestCommand;
import com.cryptolake.verify.cli.MarkMaintenanceCommand;
import com.cryptolake.verify.cli.VerifyCommand;
import com.cryptolake.verify.gaps.GapsCli;
import com.cryptolake.verify.integrity.IntegrityCli;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Entry point for the {@code cryptolake-verify} CLI.
 *
 * <p>Overrides {@link System#out} to use UTF-8 and force {@code \n} line endings on all platforms
 * (design §6.7 byte-level invariants; Q13 preferred path). Constructs the picocli root and
 * dispatches to subcommands.
 *
 * <p>Tier 5 K1 — picocli {@code @Command}. Tier 5 K3 — exit code from picocli, not {@code
 * System.exit} inside commands.
 *
 * <p>Tier 2 §14 — single shared {@link com.fasterxml.jackson.databind.ObjectMapper} constructed
 * here (via {@link com.cryptolake.common.envelope.EnvelopeCodec#newMapper()}) and passed to each
 * subcommand.
 */
@Command(
    name = "cryptolake-verify",
    description = "CryptoLake data verification CLI.",
    subcommands = {
      VerifyCommand.class,
      ManifestCommand.class,
      MarkMaintenanceCommand.class,
      GapsCli.class,
      IntegrityCli.class
    })
public final class Main {

  public static void main(String[] args) {
    // Force UTF-8 stdout with \n line endings (design §6.7 — Tier 3 §20 byte-identity)
    // Tier 5 Q13: override System.out in Main directly (preferred path)
    PrintStream utf8Out =
        new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8) {
          @Override
          public void println(String s) {
            print(s);
            print('\n');
          }

          @Override
          public void println() {
            print('\n');
          }
        };
    System.setOut(utf8Out);

    System.exit(new CommandLine(new Main()).execute(args));
  }
}
