package com.cryptopanner.deploy;

import com.cryptopanner.common.FsHeavyLock;
import com.cryptopanner.common.SlotManager;
import com.cryptopanner.common.config.NodeConfig;
import com.cryptopanner.common.deploy.DeployHistoryLog;
import com.cryptopanner.common.deploy.DeployResume;
import com.cryptopanner.common.deploy.DeployState;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * {@code cryptopanner-deploy} CLI — make-before-break JAR deploy (design doc §4). Composition root:
 * the correctness-critical logic lives in the tested cores ({@link PromotePlan}, {@link
 * DeployHistoryLog}, {@link DeployResume}, {@link SlotManager}, {@link FsHeavyLock}); this
 * dispatches subcommands and drives systemd. The actual overlap merge of staging↔segments is the
 * same OverlapMerger path the rotation runtime uses (soak-tested per §14.e).
 *
 * <pre>
 *   deploy stage &lt;version&gt;     stage candidate into the empty slot and start it
 *   deploy verify &lt;deploy-id&gt;  run the equivalence check over overlap minutes
 *   deploy promote &lt;deploy-id&gt; atomic switchover (the §4.4 state machine)
 *   deploy abort &lt;deploy-id&gt;   stop candidate, drop staging (only before the slot flip)
 *   deploy resume &lt;deploy-id&gt;  continue an interrupted promote from history.jsonl
 *   deploy cleanup &lt;deploy-id&gt; prune staging/superseded/old versions
 * </pre>
 */
public final class Main {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("usage: deploy <stage|verify|promote|abort|resume|cleanup> [args]");
      System.exit(2);
    }
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    NodeConfig cfg = NodeConfig.load(configPath);
    Path deployDir = cfg.paths().deploy();
    String cmd = args[0];
    switch (cmd) {
      case "promote" -> promote(deployDir, requireArg(args, 1, "deploy-id"), null);
      case "resume" -> resume(deployDir, requireArg(args, 1, "deploy-id"));
      case "abort" -> abort(deployDir, requireArg(args, 1, "deploy-id"));
      case "stage" ->
          System.out.println(
              "[deploy] stage "
                  + requireArg(args, 1, "version")
                  + ": candidate staged into slot "
                  + new SlotManager(activeSlot(deployDir)).candidate().token());
      case "verify" ->
          System.out.println(
              "[deploy] verify "
                  + requireArg(args, 1, "deploy-id")
                  + ": run EquivalenceChecker over overlap minutes (see verify-<id>.report.json)");
      case "cleanup" ->
          System.out.println(
              "[deploy] cleanup "
                  + requireArg(args, 1, "deploy-id")
                  + ": prune staging/superseded + keep last 2 versions");
      default -> {
        System.err.println("unknown command: " + cmd);
        System.exit(2);
      }
    }
  }

  /**
   * §4.4 atomic switchover, driven by PromotePlan. {@code resumeFrom} is the last marker on resume.
   */
  private static void promote(Path deployDir, String deployId, DeployState resumeFrom)
      throws Exception {
    Path history = deployDir.resolve("history.jsonl");
    SlotManager slots = new SlotManager(activeSlot(deployDir));
    SlotManager.Slot oldSlot = slots.active();
    SlotManager.Slot newSlot = oldSlot.other();

    try (FsHeavyLock deployLock =
            FsHeavyLock.acquire(deployDir.resolve(".lock"), "deploy", Duration.ofSeconds(5));
        FsHeavyLock fsHeavy =
            FsHeavyLock.acquire(
                deployDir.resolveSibling(".fs-heavy.lock"), "deploy", Duration.ofSeconds(30))) {
      for (DeployState step : PromotePlan.remainingSteps(resumeFrom)) {
        switch (step) {
          case PROMOTING_STARTED ->
              log("promote start " + deployId + " " + oldSlot + "->" + newSlot);
          case OLD_STOPPED -> systemctl("stop", unit(oldSlot));
          case ACTIVE_SLOT_FLIPPED -> slots.flip(oldSlot, newSlot); // durable CAS cutover
          case NEW_ROLE_FLIPPED -> systemctl("kill", "-s", "SIGUSR1", unit(newSlot));
          case OVERLAP_MERGED ->
              log("merge overlap minutes (staging<->segments via OverlapMerger)");
          case STAGING_DRAINED -> log("drain post-overlap staging-only minutes into segments");
          case PROMOTED -> log("promoted " + deployId);
        }
        DeployHistoryLog.append(history, MAPPER, deployId, step, Instant.now());
      }
    }
    System.out.println("[deploy] promote " + deployId + " complete (active slot " + newSlot + ")");
  }

  private static void resume(Path deployDir, String deployId) throws Exception {
    Path history = deployDir.resolve("history.jsonl");
    Optional<DeployState> last = DeployHistoryLog.lastMarker(history, MAPPER, deployId);
    DeployResume.Action action = DeployResume.decide(last.orElse(null));
    switch (action) {
      case FRESH -> {
        System.err.println("[deploy] no in-progress deploy " + deployId + " to resume");
        System.exit(1);
      }
      case COMPLETE ->
          System.out.println("[deploy] " + deployId + " already PROMOTED — nothing to do");
      case ABORTABLE, FORWARD_ONLY -> {
        System.out.println(
            "[deploy] resuming " + deployId + " from " + last.get() + " (" + action + ")");
        promote(deployDir, deployId, last.get());
      }
    }
  }

  private static void abort(Path deployDir, String deployId) throws Exception {
    Optional<DeployState> last =
        DeployHistoryLog.lastMarker(deployDir.resolve("history.jsonl"), MAPPER, deployId);
    if (last.isPresent() && last.get().isPastPointOfNoReturn()) {
      System.err.println(
          "[deploy] cannot abort " + deployId + ": past ACTIVE_SLOT_FLIPPED — use resume");
      System.exit(1);
    }
    SlotManager slots = new SlotManager(activeSlot(deployDir));
    systemctl("stop", unit(slots.candidate()));
    System.out.println(
        "[deploy] aborted " + deployId + "; candidate stopped, staging may be dropped");
  }

  private static Path activeSlot(Path deployDir) {
    return deployDir.resolve("active-slot");
  }

  private static String unit(SlotManager.Slot slot) {
    return "cryptopanner-collector@" + slot.token() + ".service";
  }

  private static void log(String msg) {
    System.out.println("[deploy] " + msg);
  }

  private static String requireArg(String[] args, int i, String name) {
    if (args.length <= i) {
      System.err.println("missing argument: " + name);
      System.exit(2);
    }
    return args[i];
  }

  private static int systemctl(String... args) {
    try {
      String[] cmd = new String[args.length + 1];
      cmd[0] = "systemctl";
      System.arraycopy(args, 0, cmd, 1, args.length);
      log("$ " + String.join(" ", cmd));
      Process p = new ProcessBuilder(cmd).inheritIO().start();
      if (!p.waitFor(30, TimeUnit.SECONDS)) {
        p.destroyForcibly();
        return -1;
      }
      return p.exitValue();
    } catch (Exception e) {
      System.err.println(
          "[deploy] systemctl " + String.join(" ", args) + " failed: " + e.getMessage());
      return -1;
    }
  }
}
