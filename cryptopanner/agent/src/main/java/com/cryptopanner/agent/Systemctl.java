package com.cryptopanner.agent;

import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper over {@code systemctl} for the Node Agent's restart endpoint and liveness checks
 * (master spec §11.c). Side-effecting and host-specific, so it is kept out of the unit-tested core
 * (the {@link AgentServer} takes the restart action as an injected function).
 */
public final class Systemctl {

  private Systemctl() {}

  /** Maps a {@code /restart/{component}} path segment to its systemd unit name. */
  public static String unit(String component) {
    // collector/a → cryptopanner-collector@a.service; sealer → cryptopanner-sealer.service
    if (component.startsWith("collector/")) {
      return "cryptopanner-collector@" + component.substring("collector/".length()) + ".service";
    }
    return "cryptopanner-" + component + ".service";
  }

  /** {@code systemctl is-active <unit>} → true when the unit reports active. */
  public static boolean isActive(String unit) {
    return run("is-active", unit) == 0;
  }

  /** {@code systemctl restart <unit>} → true on success. */
  public static boolean restart(String unit) {
    return run("restart", unit) == 0;
  }

  private static int run(String... args) {
    try {
      String[] cmd = new String[args.length + 1];
      cmd[0] = "systemctl";
      System.arraycopy(args, 0, cmd, 1, args.length);
      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      if (!p.waitFor(15, TimeUnit.SECONDS)) {
        p.destroyForcibly();
        return -1;
      }
      return p.exitValue();
    } catch (Exception e) {
      System.err.println(
          "[agent] systemctl " + String.join(" ", args) + " failed: " + e.getMessage());
      return -1;
    }
  }
}
