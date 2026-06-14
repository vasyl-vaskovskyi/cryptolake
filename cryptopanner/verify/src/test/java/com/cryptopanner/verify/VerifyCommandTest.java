package com.cryptopanner.verify;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.cryptopanner.common.Sha256Sidecar;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class VerifyCommandTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Test
  void exitsZeroWhenAllChecksPass(@TempDir Path tmp) throws IOException {
    S3Client s3 = client();
    s3.createBucket(CreateBucketRequest.builder().bucket("bkt").build());
    String key = "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst";
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Files.writeString(data, "compressed-stand-in");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");
    Sha256Sidecar.computeAndWrite(data, sidecar);
    String hex = Sha256Sidecar.readHash(sidecar);
    String manifestJson =
        "{\n  \"manifest_schema_version\": 1,\n  \"file_sha256\": \"" + hex + "\"\n}";

    s3.putObject(
        PutObjectRequest.builder().bucket("bkt").key(key).build(), RequestBody.fromFile(data));
    s3.putObject(
        PutObjectRequest.builder().bucket("bkt").key(key + ".sha256").build(),
        RequestBody.fromFile(sidecar));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket("bkt")
            .key(key.replace(".jsonl.zst", ".manifest.json"))
            .build(),
        RequestBody.fromString(manifestJson));

    int code =
        new CommandLine(new Main())
            .execute(
                "verify",
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
    assertEquals(0, code);
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
