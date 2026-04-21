package com.cryptolake.common.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Ports Python's {@code _normalize_env_overrides} and {@code _apply_env_overrides}.
 *
 * <p>Package-private — used only by {@link YamlConfigLoader}. Stateless; safe to call from any
 * thread.
 */
final class EnvOverrides {

  private EnvOverrides() {}

  /**
   * Normalizes the raw override map:
   *
   * <ul>
   *   <li>If {@code HOST_DATA_DIR} is present and {@code WRITER__BASE_DIR} is not, aliases
   *       {@code HOST_DATA_DIR} → {@code WRITER__BASE_DIR} (Tier 5 J4 watch-out).
   *   <li>Removes {@code HOST_DATA_DIR} from the result.
   * </ul>
   */
  static Map<String, String> normalize(Map<String, String> in) {
    Map<String, String> normalized = new HashMap<>(in);
    String hostDataDir = normalized.remove("HOST_DATA_DIR");
    if (hostDataDir != null && !normalized.containsKey("WRITER__BASE_DIR")) {
      normalized.put("WRITER__BASE_DIR", hostDataDir);
    }
    return normalized;
  }

  /**
   * Applies the normalized overrides to the YAML data map (ports Python's {@code
   * _apply_env_overrides}).
   *
   * <ul>
   *   <li>Keys are split on {@code "__"} (case-insensitively lowercased) to navigate/create
   *       nested map entries.
   *   <li>Values containing {@code ","} are split into a {@code List<String>} (comma-separated).
   * </ul>
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> apply(Map<String, Object> data, Map<String, String> overrides) {
    for (Map.Entry<String, String> entry : overrides.entrySet()) {
      String key = entry.getKey();
      String rawValue = entry.getValue();

      String[] parts = key.toLowerCase().split("__", -1);

      Map<String, Object> target = data;
      for (int i = 0; i < parts.length - 1; i++) {
        String part = parts[i];
        Object nested = target.get(part);
        if (!(nested instanceof Map)) {
          nested = new HashMap<String, Object>();
          target.put(part, nested);
        }
        target = (Map<String, Object>) nested;
      }

      Object value;
      if (rawValue.contains(",")) {
        List<String> list =
            Arrays.stream(rawValue.split(","))
                .map(String::strip)
                .collect(Collectors.toList());
        value = list;
      } else {
        value = rawValue;
      }

      target.put(parts[parts.length - 1], value);
    }
    return data;
  }
}
