package com.cryptolake.verify.audit;

import java.util.List;

/**
 * Source of {@link GapRecord}s over a given {@link AuditScope}.
 *
 * <p>Implementations may read from the on-disk archive, the PG component_runtime table, the
 * lifecycle ledger, or any other signal. Each implementation identifies itself via {@link #name()}.
 */
public interface GapSource {

  /** Short identifier for this source, used in log messages and diff output. */
  String name();

  /**
   * Returns all gap records visible to this source within the given scope.
   *
   * @param scope defines the time range, optional exchange/symbol/stream filters, and base dir
   * @return list of gap records (may be empty; never {@code null})
   */
  List<GapRecord> read(AuditScope scope);
}
