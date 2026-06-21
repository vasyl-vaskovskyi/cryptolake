package com.cryptopanner.common.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Shared YAML config loader with {@code CRYPTOPANNER_<KEY_PATH>} environment overrides (§15.d).
 * Used by both {@link NodeConfig} and {@link MonitorConfig}: parse the YAML tree, override any
 * scalar leaf whose derived env name is present, then bind to the record type (snake_case mapping).
 * Validation is the caller's responsibility (it is type-specific).
 */
final class ConfigLoader {

  private ConfigLoader() {}

  static <T> T load(Path yaml, Class<T> type, Map<String, String> env) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    JsonNode tree = mapper.readTree(yaml.toFile());
    if (tree instanceof ObjectNode root) {
      applyEnvOverrides(root, new ArrayList<>(), env);
    }
    return mapper.treeToValue(tree, type);
  }

  /**
   * Replaces any scalar leaf whose derived {@code CRYPTOPANNER_<PATH>} name (uppercased dotted
   * path, separators as underscores) is present in {@code env}. Object nodes recurse;
   * arrays/containers are not overridable.
   */
  private static void applyEnvOverrides(
      ObjectNode node, List<String> path, Map<String, String> env) {
    for (String field : new ArrayList<>(iterable(node.fieldNames()))) {
      JsonNode child = node.get(field);
      List<String> childPath = new ArrayList<>(path);
      childPath.add(field);
      if (child instanceof ObjectNode obj) {
        applyEnvOverrides(obj, childPath, env);
      } else if (child != null && child.isValueNode()) {
        String envName = "CRYPTOPANNER_" + String.join("_", childPath).toUpperCase(Locale.ROOT);
        String override = env.get(envName);
        if (override != null) {
          node.put(field, override);
        }
      }
    }
  }

  private static List<String> iterable(Iterator<String> it) {
    List<String> out = new ArrayList<>();
    it.forEachRemaining(out::add);
    return out;
  }
}
