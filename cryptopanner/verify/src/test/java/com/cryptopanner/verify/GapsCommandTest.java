package com.cryptopanner.verify;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class GapsCommandTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  private void putManifest(String json) {
    S3Client s3 = client();
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket("bkt").build());
    } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException ignored) {
      // shared mock — bucket persists across this class's tests
    }
    s3.putObject(
        PutObjectRequest.builder()
            .bucket("bkt")
            .key("vps-fra-1/btcusdt/trade/2026-06-14/hour-14.manifest.json")
            .build(),
        RequestBody.fromString(json));
  }

  private String run(ByteArrayOutputStream out) {
    PrintStream original = System.out;
    int code;
    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
      code =
          new CommandLine(new Main())
              .execute(
                  "gaps",
                  "--endpoint",
                  S3.getServiceEndpoint(),
                  "--bucket",
                  "bkt",
                  "--node-id",
                  "vps-fra-1",
                  "--symbol",
                  "btcusdt",
                  "--stream",
                  "trade",
                  "--date",
                  "2026-06-14",
                  "--hour",
                  "14");
    } finally {
      System.setOut(original);
    }
    assertEquals(0, code);
    return out.toString(StandardCharsets.UTF_8);
  }

  @Test
  void reportsSequenceGapsFromManifest() {
    putManifest(
        "{\"manifest_schema_version\":1,\"sequence_gaps\":["
            + "{\"from\":102,\"to\":103,\"count\":2,\"backfill_outcome\":\"FILLED\"}]}");
    String printed = run(new ByteArrayOutputStream());
    assertTrue(printed.contains("btcusdt@trade gap 102..103"), printed);
    assertTrue(printed.contains("backfill=FILLED"), printed);
    assertTrue(printed.contains("GAPS=1"), printed);
  }

  @Test
  void reportsZeroGapsForACleanManifest() {
    putManifest("{\"manifest_schema_version\":1,\"sequence_gaps\":[],\"record_count\":4242}");
    assertTrue(run(new ByteArrayOutputStream()).contains("GAPS=0"));
  }

  private static S3Client client() {
    return S3Client.builder()
        .endpointOverride(URI.create(S3.getServiceEndpoint()))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")))
        .region(Region.US_EAST_1)
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build();
  }
}
