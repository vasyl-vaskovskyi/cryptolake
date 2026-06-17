package com.cryptopanner.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Factory for the project's shared {@link ObjectMapper} instances. Each service constructs one
 * mapper at startup via {@link #newMapper()} and threads it through wiring; ad-hoc {@code new
 * ObjectMapper()} elsewhere is discouraged because per-call construction is hot enough to matter
 * and config drift between mappers is a known source of subtle parse differences.
 *
 * <p>{@link #newPrettyMapper()} is the variant used by ManifestWriter — pretty-printed output with
 * LF line endings, per master spec §10.d.
 */
public final class EnvelopeCodec {

  private EnvelopeCodec() {}

  /** General-purpose mapper for envelope parsing and serialization. */
  public static ObjectMapper newMapper() {
    return new ObjectMapper();
  }

  /** Pretty-printed mapper for human-readable artefacts (manifests). */
  public static ObjectMapper newPrettyMapper() {
    return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  }
}
