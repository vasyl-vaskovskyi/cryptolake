package com.cryptopanner.verify;

import com.cryptopanner.common.EnvelopeCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * {@code manifest} subcommand (master spec §14.f): fetch and pretty-print the sealed-hour manifest
 * for one {@code (symbol, stream)} from S3 — an operator inspection tool. Exit 0 when found, 1 when
 * the manifest is absent, 2 on missing connection options.
 */
@Command(
    name = "manifest",
    description = "Print the sealed-hour manifest JSON for a (symbol, stream).")
public final class ManifestCommand implements Callable<Integer> {

  @Mixin S3InspectOptions opts;

  @Option(names = "--symbol", required = true)
  String symbol;

  @Option(names = "--stream", required = true)
  String stream;

  private final ObjectMapper mapper = EnvelopeCodec.newPrettyMapper();

  @Override
  public Integer call() throws Exception {
    opts.resolve();
    if (!opts.ready()) {
      System.err.println("manifest needs --endpoint, --bucket, --node-id (or pass --config)");
      return 2;
    }
    S3Client s3 = opts.client();
    try {
      byte[] manifest = opts.fetch(s3, opts.manifestKey(symbol, stream));
      System.out.println(mapper.writeValueAsString(mapper.readTree(manifest)));
      return 0;
    } catch (Exception e) {
      System.err.println("manifest not found for " + symbol + "@" + stream + ": " + e.getMessage());
      return 1;
    }
  }
}
