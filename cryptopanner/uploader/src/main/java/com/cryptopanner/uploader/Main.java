package com.cryptopanner.uploader;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.config.SkeletonConfig;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);
    String dateStr = required(args, "--date");
    int hour = Integer.parseInt(required(args, "--hour"));
    Instant hourStart =
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0)).toInstant(ZoneOffset.UTC);

    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(cfg.storage().endpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        cfg.storage().accessKey(), cfg.storage().secretKey())))
            .region(Region.of(cfg.storage().region()))
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(cfg.storage().pathStyleAccess())
                    .build())
            .build();

    // Idempotent bucket-create — skeleton convenience; production would do this once.
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(cfg.storage().bucket()).build());
    } catch (Exception ignored) {
      // already exists
    }

    S3Uploader uploader = new S3Uploader(s3, cfg.storage().bucket());

    System.out.println(
        "[uploader] uploading hour "
            + hour
            + " of "
            + dateStr
            + " for "
            + cfg.effectiveSubscriptions().size()
            + " subscription(s) to s3://"
            + cfg.storage().bucket());

    int failures = 0;
    for (SkeletonConfig.Subscription sub : cfg.effectiveSubscriptions()) {
      String symbol = sub.symbol();
      String stream = sub.stream();
      try {
        Path data = Paths.hourSealed(cfg.paths().sealed(), symbol, stream, hourStart);
        Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
        Path manifest =
            data.resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
        if (!Files.exists(data) || !Files.exists(sidecar) || !Files.exists(manifest)) {
          throw new IllegalStateException(
              "missing sealed files for " + symbol + "@" + stream + "; run sealer first");
        }

        String key = Paths.s3Key(cfg.nodeId(), symbol, stream, hourStart);
        System.out.println("[uploader] " + symbol + "@" + stream + ": -> " + key);
        uploader.upload(key, data, sidecar, manifest);

        Files.delete(data);
        Files.delete(sidecar);
        Files.delete(manifest);
      } catch (Exception e) {
        System.err.println("[uploader] " + symbol + "@" + stream + ": FAILED — " + e.getMessage());
        failures++;
      }
    }

    if (failures > 0) {
      System.err.println("[uploader] " + failures + " subscription(s) failed");
      System.exit(1);
    }
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing flag: " + flag);
  }
}
