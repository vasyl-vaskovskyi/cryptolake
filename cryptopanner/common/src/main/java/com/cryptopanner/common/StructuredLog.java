package com.cryptopanner.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * JSON Lines structured logger (master spec §11.a). Each component writes one JSON object per line
 * to {@code /data/cryptopanner/logs/<file>.jsonl} with the required fields {@code ts} (ISO-8601
 * ms), {@code component}, {@code event} (a §11.e machine-readable identifier), {@code level}, plus
 * {@code slot} for collector logs and any domain-specific fields. {@code ERROR} entries are
 * additionally mirrored to a never-rotated {@code critical-events.jsonl} sidecar for off-node
 * incident analysis.
 *
 * <p>File-size/weekly rotation is handled by {@code logrotate} on the node (ops), per §11.a.
 */
public final class StructuredLog {

  private final Path logFile;
  private final Path criticalFile;
  private final String component;
  private final String slot; // nullable — present only for collector logs
  private final ObjectMapper mapper;
  private final Supplier<Instant> clock;

  public StructuredLog(Path logFile, String component, String slot) {
    this(logFile, component, slot, new ObjectMapper(), Instant::now);
  }

  public StructuredLog(
      Path logFile, String component, String slot, ObjectMapper mapper, Supplier<Instant> clock) {
    this.logFile = logFile;
    this.criticalFile = logFile.resolveSibling("critical-events.jsonl");
    this.component = component;
    this.slot = slot;
    this.mapper = mapper;
    this.clock = clock;
  }

  public void info(String event, Map<String, ?> fields) {
    write("INFO", event, fields);
  }

  public void warn(String event, Map<String, ?> fields) {
    write("WARN", event, fields);
  }

  public void error(String event, Map<String, ?> fields) {
    write("ERROR", event, fields);
  }

  private void write(String level, String event, Map<String, ?> fields) {
    ObjectNode node = mapper.createObjectNode();
    node.put("ts", clock.get().toString());
    node.put("component", component);
    if (slot != null) {
      node.put("slot", slot);
    }
    node.put("event", event);
    node.put("level", level);
    for (Map.Entry<String, ?> e : fields.entrySet()) {
      node.set(e.getKey(), mapper.valueToTree(e.getValue()));
    }
    byte[] line;
    try {
      line = (mapper.writeValueAsString(node) + "\n").getBytes(StandardCharsets.UTF_8);
    } catch (Exception ex) {
      System.err.println("[log] serialize failed for event " + event + ": " + ex.getMessage());
      return;
    }
    append(logFile, line);
    if ("ERROR".equals(level)) {
      append(criticalFile, line);
    }
  }

  private static void append(Path file, byte[] line) {
    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      try (FileChannel ch =
          FileChannel.open(
              file,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND)) {
        ch.write(ByteBuffer.wrap(line));
      }
    } catch (IOException e) {
      System.err.println("[log] append failed to " + file + ": " + e.getMessage());
    }
  }
}
