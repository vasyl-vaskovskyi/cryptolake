package com.cryptopanner.monitor.testutil;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.monitor.StatusSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/** Builds {@link StatusSnapshot} instances from terse parameters for monitor tests. */
public final class Snapshots {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  private Snapshots() {}

  /** A snapshot with the given component states, optional lock holder, and connection age. */
  public static StatusSnapshot status(
      String node,
      String activeSlot,
      Map<String, String> componentStates,
      String lockHolder,
      double connectionAgeS) {
    StringBuilder c = new StringBuilder();
    boolean first = true;
    for (var e : componentStates.entrySet()) {
      if (!first) c.append(",");
      first = false;
      boolean down = e.getValue().equals("down");
      c.append("\"")
          .append(e.getKey())
          .append("\":{\"state\":\"")
          .append(e.getValue())
          .append("\",\"pid\":")
          .append(down ? "null" : "5")
          .append(",\"heartbeat_age_s\":")
          .append(down ? "null" : "1.0")
          .append(",\"uptime_s\":10}");
    }
    String held = lockHolder == null ? "null" : "\"" + lockHolder + "\"";
    String json =
        "{\"node\":\""
            + node
            + "\",\"components\":{"
            + c
            + "},\"active_slot\":\""
            + activeSlot
            + "\",\"fs_heavy_lock\":{\"held_by\":"
            + held
            + "},\"deploy\":{\"state\":\"IDLE\"},\"rotation\":{\"state\":\"IDLE\",\"current_connection_age_s\":"
            + connectionAgeS
            + "},\"vps\":{\"disk\":{\"/data\":{\"percent\":50.0,\"free_bytes\":1}}}}";
    try {
      return StatusSnapshot.parse(json, MAPPER);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
