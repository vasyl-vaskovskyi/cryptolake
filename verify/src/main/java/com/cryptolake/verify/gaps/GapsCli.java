package com.cryptolake.verify.gaps;

import picocli.CommandLine.Command;

/**
 * Picocli sub-tree for gap analysis and backfill commands.
 *
 * <p>Ports the {@code cli} group from {@code gaps.py}. Mirrors Python's CLI group structure: {@code
 * cryptolake-verify gaps backfill ...} maps to {@code python -m src.cli.gaps backfill ...}.
 */
@Command(
    name = "gaps",
    description = "Gap analysis and backfill commands.",
    subcommands = {AnalyzeCommand.class, BackfillCommand.class})
public final class GapsCli {}
