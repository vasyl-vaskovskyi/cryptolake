package com.cryptolake.common.config;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Loads and validates CryptoLake YAML configuration.
 *
 * <p>Ports Python's {@code load_config}. Algorithm (design §2.2):
 *
 * <ol>
 *   <li>Check file exists — throw {@link CryptoLakeConfigException} if not.
 *   <li>Read YAML into {@code Map<String,Object>}.
 *   <li>Compute effective overrides (caller-supplied or System.getenv-filtered).
 *   <li>Normalize ({@code HOST_DATA_DIR} alias — Tier 5 J4).
 *   <li>Apply overrides to map.
 *   <li>Convert to {@link AppConfig} via Jackson {@code convertValue}.
 *   <li>Validate via Hibernate Validator; throw on violations (Tier 5 J3).
 * </ol>
 *
 * <p>Tier 2 §14: mappers and validator are created once at class load as static finals. Tier 5
 * B7: YAML mapper shares the same feature set as the JSON mapper.
 *
 * <p>Thread safety: all static + stateless; safe from any thread.
 */
public final class YamlConfigLoader {

  // Static finals — created once (Tier 2 §14)
  private static final ObjectMapper YAML_MAPPER = EnvelopeCodec.newYamlMapper();
  private static final ObjectMapper JSON_MAPPER = EnvelopeCodec.newMapper();
  private static final ValidatorFactory VALIDATOR_FACTORY;
  private static final Validator VALIDATOR;

  /**
   * Package-private seam for testing: allows tests to inject a fake env supplier so the 2-arg
   * overload never reads System.getenv() (design §11 Q3 option b).
   */
  static Supplier<Map<String, String>> envSupplier = System::getenv;

  static {
    VALIDATOR_FACTORY = Validation.buildDefaultValidatorFactory();
    VALIDATOR = VALIDATOR_FACTORY.getValidator();
    Runtime.getRuntime().addShutdownHook(new Thread(VALIDATOR_FACTORY::close));
  }

  private YamlConfigLoader() {}

  /**
   * Loads configuration from {@code path}, consulting {@link System#getenv()} for overrides.
   *
   * <p>Only env vars matching known module prefixes ({@code DATABASE__}, {@code EXCHANGES__}, etc.)
   * plus {@code HOST_DATA_DIR} are considered.
   */
  public static AppConfig load(Path path) {
    Map<String, String> env = envSupplier.get();
    Set<String> allowedPrefixes =
        Set.of("database", "exchanges", "redpanda", "writer", "monitoring", "collector");

    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> e : env.entrySet()) {
      String k = e.getKey();
      if (k.contains("__")) {
        String prefix = k.split("__", 2)[0].toLowerCase();
        if (allowedPrefixes.contains(prefix)) {
          filtered.put(k, e.getValue());
        }
      }
    }
    if (env.containsKey("HOST_DATA_DIR")) {
      filtered.put("HOST_DATA_DIR", env.get("HOST_DATA_DIR"));
    }
    return load(path, EnvOverrides.normalize(filtered));
  }

  /**
   * Loads configuration from {@code path} using the supplied override map (explicit; does NOT
   * consult {@link System#getenv()}).
   *
   * <p>Passing an empty map (or an already-normalized map) is valid.
   */
  public static AppConfig load(Path path, Map<String, String> envOverrides) {
    if (!Files.exists(path)) {
      throw new CryptoLakeConfigException("Config file not found: " + path, null);
    }

    Map<String, Object> data;
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> raw = YAML_MAPPER.readValue(path.toFile(), Map.class);
      data = (raw != null) ? new HashMap<>(raw) : new HashMap<>();
    } catch (IOException e) {
      throw new CryptoLakeConfigException("Failed to read config file: " + path, e);
    }

    if (envOverrides != null && !envOverrides.isEmpty()) {
      // Already normalized by caller (load(Path) pre-normalizes; direct callers pass normalized)
      EnvOverrides.apply(data, envOverrides);
    }

    AppConfig config;
    try {
      config = JSON_MAPPER.convertValue(data, AppConfig.class);
    } catch (IllegalArgumentException e) {
      Throwable cause = e.getCause();
      if (cause instanceof JsonProcessingException) {
        throw new CryptoLakeConfigException("Config schema error: " + cause.getMessage(), cause);
      }
      throw new CryptoLakeConfigException("Config conversion error: " + e.getMessage(), e);
    }

    Set<ConstraintViolation<AppConfig>> violations = VALIDATOR.validate(config);
    if (!violations.isEmpty()) {
      String msg =
          violations.stream()
              .map(v -> v.getPropertyPath() + ": " + v.getMessage())
              .collect(Collectors.joining("; "));
      throw new CryptoLakeConfigException("Config validation failed: " + msg);
    }

    return config;
  }

  /**
   * Returns the default archive directory: {@code HOST_DATA_DIR} env var or {@code "/data"} (ports
   * Python's {@code default_archive_dir()}).
   */
  public static String defaultArchiveDir() {
    return System.getenv().getOrDefault("HOST_DATA_DIR", "/data");
  }
}
