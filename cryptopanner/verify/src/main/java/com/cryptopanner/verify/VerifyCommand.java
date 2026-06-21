package com.cryptopanner.verify;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.NodeConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Command(
    name = "verify",
    description =
        "Integrity-check sealed hours in S3. Verifies one (symbol, stream) triple if both "
            + "--symbol and --stream are set; otherwise iterates all subscriptions in --config.")
public final class VerifyCommand implements Callable<Integer> {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  @Option(
      names = "--config",
      description =
          "Path to config.yaml. Pulls bucket, node-id, "
              + "credentials, and subscription list from it. CLI flags override config values.")
  String configPath;

  @Option(names = "--endpoint")
  String endpoint;

  @Option(names = "--bucket")
  String bucket;

  @Option(names = "--node-id")
  String nodeId;

  @Option(names = "--symbol")
  String symbol;

  @Option(names = "--stream")
  String stream;

  @Option(names = "--date", required = true)
  String date;

  @Option(names = "--hour", required = true)
  int hour;

  @Option(names = "--access-key")
  String accessKey;

  @Option(names = "--secret-key")
  String secretKey;

  @Option(names = "--region")
  String region;

  @Override
  public Integer call() throws Exception {
    NodeConfig cfg = null;
    if (configPath != null) {
      cfg = NodeConfig.load(Path.of(configPath));
      if (endpoint == null) endpoint = cfg.storage().endpoint();
      if (bucket == null) bucket = cfg.storage().bucket();
      if (nodeId == null) nodeId = cfg.nodeId();
      if (accessKey == null) accessKey = cfg.storage().accessKey();
      if (secretKey == null) secretKey = cfg.storage().secretKey();
      if (region == null) region = cfg.storage().region();
    }
    if (accessKey == null) accessKey = "cryptopanner";
    if (secretKey == null) secretKey = "changeme-dev";
    if (region == null) region = "us-east-1";

    if (endpoint == null || bucket == null || nodeId == null) {
      System.err.println(
          "verify needs --endpoint, --bucket, --node-id (or pass --config to default them)");
      return 2;
    }

    List<NodeConfig.Subscription> targets = new ArrayList<>();
    if (symbol != null && stream != null) {
      targets.add(new NodeConfig.Subscription(symbol, stream));
    } else if (cfg != null) {
      targets.addAll(cfg.effectiveSubscriptions());
    } else {
      System.err.println("verify needs either (--symbol and --stream) or --config");
      return 2;
    }

    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .region(Region.of(region))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();

    int errors = 0;
    for (NodeConfig.Subscription sub : targets) {
      errors += verifyOne(s3, sub.symbol(), sub.stream());
    }

    System.out.println("ERRORS=" + errors);
    return errors == 0 ? 0 : 1;
  }

  private int verifyOne(S3Client s3, String symbol, String stream) throws Exception {
    String prefix = nodeId + "/" + symbol + "/" + stream + "/" + date;
    String dataKey = prefix + "/hour-" + String.format("%02d", hour) + ".jsonl.zst";
    String sidecarKey = dataKey + ".sha256";
    String manifestKey = prefix + "/hour-" + String.format("%02d", hour) + ".manifest.json";

    int errors = 0;
    try {
      byte[] data = fetch(s3, dataKey);
      byte[] sidecar = fetch(s3, sidecarKey);
      byte[] manifest = fetch(s3, manifestKey);

      String computedSha = sha256Hex(data);
      String sidecarHex = new String(sidecar).split("\\s+")[0];
      String manifestSha = mapper.readTree(manifest).get("file_sha256").asText();

      if (!computedSha.equals(sidecarHex)) {
        System.err.println(
            symbol
                + "@"
                + stream
                + " MISMATCH: computed="
                + computedSha
                + " sidecar="
                + sidecarHex);
        errors++;
      }
      if (!computedSha.equals(manifestSha)) {
        System.err.println(
            symbol
                + "@"
                + stream
                + " MISMATCH: computed="
                + computedSha
                + " manifest="
                + manifestSha);
        errors++;
      }
      if (errors == 0) {
        System.out.println(
            symbol + "@" + stream + " OK (sha=" + computedSha.substring(0, 12) + ")");
      }
    } catch (Exception e) {
      System.err.println(symbol + "@" + stream + " FAILED — " + e.getMessage());
      errors++;
    }
    return errors;
  }

  private byte[] fetch(S3Client s3, String key) throws Exception {
    try (ResponseInputStream<GetObjectResponse> in =
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return in.readAllBytes();
    }
  }

  private static String sha256Hex(byte[] bytes) throws Exception {
    byte[] d = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder sb = new StringBuilder(64);
    for (byte b : d) sb.append(String.format("%02x", b));
    return sb.toString();
  }
}
