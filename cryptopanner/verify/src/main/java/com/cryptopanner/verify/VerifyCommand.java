package com.cryptopanner.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.security.MessageDigest;
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

@Command(name = "verify", description = "Download and integrity-check one (symbol, stream, hour).")
public final class VerifyCommand implements Callable<Integer> {

  @Option(names = "--endpoint", required = true)
  String endpoint;

  @Option(names = "--bucket", required = true)
  String bucket;

  @Option(names = "--node-id", required = true)
  String nodeId;

  @Option(names = "--symbol", required = true)
  String symbol;

  @Option(names = "--stream", required = true)
  String stream;

  @Option(names = "--date", required = true)
  String date;

  @Option(names = "--hour", required = true)
  int hour;

  @Option(names = "--access-key", defaultValue = "cryptopanner")
  String accessKey;

  @Option(names = "--secret-key", defaultValue = "changeme-dev")
  String secretKey;

  @Option(names = "--region", defaultValue = "us-east-1")
  String region;

  @Override
  public Integer call() throws Exception {
    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .region(Region.of(region))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();

    String prefix = nodeId + "/" + symbol + "/" + stream + "/" + date;
    String dataKey = prefix + "/hour-" + String.format("%02d", hour) + ".jsonl.zst";
    String sidecarKey = dataKey + ".sha256";
    String manifestKey = prefix + "/hour-" + String.format("%02d", hour) + ".manifest.json";

    byte[] data = fetch(s3, dataKey);
    byte[] sidecar = fetch(s3, sidecarKey);
    byte[] manifest = fetch(s3, manifestKey);

    int errors = 0;
    String computedSha = sha256Hex(data);
    String sidecarHex = new String(sidecar).split("\\s+")[0];
    String manifestSha = new ObjectMapper().readTree(manifest).get("file_sha256").asText();

    if (!computedSha.equals(sidecarHex)) {
      System.err.println("MISMATCH: computed=" + computedSha + " sidecar=" + sidecarHex);
      errors++;
    }
    if (!computedSha.equals(manifestSha)) {
      System.err.println("MISMATCH: computed=" + computedSha + " manifest=" + manifestSha);
      errors++;
    }

    System.out.println("ERRORS=" + errors);
    return errors == 0 ? 0 : 1;
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
