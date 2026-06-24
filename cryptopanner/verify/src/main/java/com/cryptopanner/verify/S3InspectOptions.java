package com.cryptopanner.verify;

import com.cryptopanner.common.config.NodeConfig;
import java.net.URI;
import java.nio.file.Path;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Shared picocli {@code @Mixin} for the read-only S3 inspector subcommands ({@code manifest},
 * {@code gaps}): the S3 connection + node addressing options, with {@code --config} defaulting and
 * the key/fetch helpers. Mirrors {@code VerifyCommand}'s option set so the CLI is consistent.
 */
final class S3InspectOptions {

  @Option(
      names = "--config",
      description = "Path to config.yaml; defaults endpoint/bucket/node-id/credentials from it.")
  String configPath;

  @Option(names = "--endpoint")
  String endpoint;

  @Option(names = "--bucket")
  String bucket;

  @Option(names = "--node-id")
  String nodeId;

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

  /**
   * Applies {@code --config} defaults and credential fallbacks; returns the loaded config or null.
   */
  NodeConfig resolve() throws Exception {
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
    return cfg;
  }

  boolean ready() {
    return endpoint != null && bucket != null && nodeId != null;
  }

  S3Client client() {
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .region(Region.of(region))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build();
  }

  String manifestKey(String symbol, String stream) {
    return nodeId
        + "/"
        + symbol
        + "/"
        + stream
        + "/"
        + date
        + "/hour-"
        + String.format("%02d", hour)
        + ".manifest.json";
  }

  byte[] fetch(S3Client s3, String key) throws Exception {
    try (ResponseInputStream<GetObjectResponse> in =
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return in.readAllBytes();
    }
  }
}
