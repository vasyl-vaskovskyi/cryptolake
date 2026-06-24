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

class ManifestCommandTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  private static final String MANIFEST =
      "{\n  \"manifest_schema_version\": 1,\n  \"symbol\": \"btcusdt\",\n  \"stream\": \"trade\",\n"
          + "  \"record_count\": 4242,\n  \"file_sha256\": \"abc123\"\n}";

  private int run(ByteArrayOutputStream out, String... extra) {
    String[] base = {
      "manifest",
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
      "14"
    };
    String[] argv = new String[base.length + extra.length];
    System.arraycopy(base, 0, argv, 0, base.length);
    System.arraycopy(extra, 0, argv, base.length, extra.length);
    PrintStream original = System.out;
    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
      return new CommandLine(new Main()).execute(argv);
    } finally {
      System.setOut(original);
    }
  }

  @Test
  void printsTheManifestJson() {
    S3Client s3 = client();
    ensureBucket(s3);
    s3.putObject(
        PutObjectRequest.builder()
            .bucket("bkt")
            .key("vps-fra-1/btcusdt/trade/2026-06-14/hour-14.manifest.json")
            .build(),
        RequestBody.fromString(MANIFEST));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = run(out);

    assertEquals(0, code);
    String printed = out.toString(StandardCharsets.UTF_8);
    assertTrue(printed.contains("\"record_count\""), printed);
    assertTrue(printed.contains("4242"), printed);
  }

  @Test
  void exitsNonZeroWhenManifestMissing() {
    ensureBucket(client());
    int code = run(new ByteArrayOutputStream());
    assertEquals(1, code);
  }

  private static void ensureBucket(S3Client s3) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket("bkt").build());
    } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException ignored) {
      // shared mock — bucket persists across this class's tests
    }
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
