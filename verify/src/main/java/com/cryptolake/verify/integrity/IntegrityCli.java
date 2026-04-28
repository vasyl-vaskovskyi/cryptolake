package com.cryptolake.verify.integrity;

import picocli.CommandLine.Command;

/**
 * Picocli sub-tree for archive integrity checks.
 *
 * <p>Ports the {@code cli} group from {@code integrity.py}. Subcommands: check-trades, check-depth,
 * check-bookticker.
 */
@Command(
    name = "integrity",
    description = "Archive ID continuity checks.",
    subcommands = {CheckTradesCommand.class, CheckDepthCommand.class, CheckBooktickerCommand.class})
public final class IntegrityCli {}
