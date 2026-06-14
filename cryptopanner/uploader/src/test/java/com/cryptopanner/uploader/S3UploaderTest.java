package com.cryptopanner.uploader;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

class S3UploaderTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Test
  void uploadsAllThreeObjectsInOrder(@TempDir Path tmp) throws IOException {
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");
    Path manifest = tmp.resolve("hour-14.manifest.json");
    Files.writeString(data, "compressed-bytes-stand-in");
    Files.writeString(sidecar, "deadbeef".repeat(8) + "  hour-14.jsonl.zst\n");
    Files.writeString(manifest, "{\"manifest_schema_version\":1}");

    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(S3.getServiceEndpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")))
            .region(Region.US_EAST_1)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();
    s3.createBucket(CreateBucketRequest.builder().bucket("bucket").build());

    new S3Uploader(s3, "bucket")
        .upload("vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst", data, sidecar, manifest);

    assertTrue(objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst"));
    assertTrue(
        objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst.sha256"));
    assertTrue(
        objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.manifest.json"));
  }

  private static boolean objectExists(S3Client s3, String bucket, String key) {
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
