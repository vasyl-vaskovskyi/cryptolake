package com.cryptopanner.verify;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.NodeConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * {@code gaps} subcommand (master spec §14.f): report the sequence gaps and missing minutes that
 * sealed-hour manifests recorded — the operator's gap-analysis view. Verifies one {@code (symbol,
 * stream)} when both are set, otherwise every subscription in {@code --config}. Prints one line per
 * gap plus a trailing {@code GAPS=<n>} summary; always exits 0 (it is a report, not a gate).
 */
@Command(
    name = "gaps",
    description = "Report sequence gaps + missing minutes recorded in sealed-hour manifests.")
public final class GapsCommand implements Callable<Integer> {

  @Mixin S3InspectOptions opts;

  @Option(names = "--symbol")
  String symbol;

  @Option(names = "--stream")
  String stream;

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  @Override
  public Integer call() throws Exception {
    NodeConfig cfg = opts.resolve();
    if (!opts.ready()) {
      System.err.println("gaps needs --endpoint, --bucket, --node-id (or pass --config)");
      return 2;
    }

    List<NodeConfig.Subscription> targets = new ArrayList<>();
    if (symbol != null && stream != null) {
      targets.add(new NodeConfig.Subscription(symbol, stream));
    } else if (cfg != null) {
      targets.addAll(cfg.effectiveSubscriptions());
    } else {
      System.err.println("gaps needs either (--symbol and --stream) or --config");
      return 2;
    }

    S3Client s3 = opts.client();
    int totalGaps = 0;
    for (NodeConfig.Subscription sub : targets) {
      String label = sub.symbol() + "@" + sub.stream();
      try {
        JsonNode root =
            mapper.readTree(opts.fetch(s3, opts.manifestKey(sub.symbol(), sub.stream())));
        JsonNode gaps = root.path("sequence_gaps");
        if (gaps.isArray()) {
          for (JsonNode g : gaps) {
            System.out.println(
                label
                    + " gap "
                    + g.path("from").asLong()
                    + ".."
                    + g.path("to").asLong()
                    + " ("
                    + g.path("count").asLong()
                    + " ids) backfill="
                    + g.path("backfill_outcome").asText("?"));
            totalGaps++;
          }
        }
        JsonNode missing = root.path("minutes_missing");
        if (missing.isArray() && !missing.isEmpty()) {
          System.out.println(label + " missing_minutes=" + missing.size());
        }
      } catch (Exception e) {
        System.out.println(label + " NO MANIFEST (" + e.getMessage() + ")");
      }
    }
    System.out.println("GAPS=" + totalGaps);
    return 0;
  }
}
