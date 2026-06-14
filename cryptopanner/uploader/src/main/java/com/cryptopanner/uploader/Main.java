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
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0))
            .toInstant(ZoneOffset.UTC);

    Path data = Paths.hourSealed(cfg.paths().sealed(), cfg.symbol(), cfg.stream(), hourStart);
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    Path manifest =
        data.resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
    if (!Files.exists(data) || !Files.exists(sidecar) || !Files.exists(manifest)) {
      throw new IllegalStateException("missing sealed files; run sealer first");
    }

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

    String key = Paths.s3Key(cfg.nodeId(), cfg.symbol(), cfg.stream(), hourStart);
    System.out.println("[uploader] uploading to s3://" + cfg.storage().bucket() + "/" + key);
    new S3Uploader(s3, cfg.storage().bucket()).upload(key, data, sidecar, manifest);

    System.out.println("[uploader] success; cleaning local sealed files");
    Files.delete(data);
    Files.delete(sidecar);
    Files.delete(manifest);
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing flag: " + flag);
  }
}
