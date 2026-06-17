package com.cryptopanner.uploader;

import java.io.IOException;
import java.nio.file.Path;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3 upload in master spec §9.c.1 order: data → sidecar → manifest.
 *
 * <p><b>Known limitation (master spec §12.f):</b> there is no retry — a single network failure
 * during {@link #upload} aborts the hour's upload until the next sealer/uploader cycle. §12.f
 * mandates infinite backoff with the manifest-last sequence preserved. The retry strategy
 * (in-process loop vs systemd respawn vs persistent queue) is an open design decision; this class
 * is the spot where it lands when chosen.
 */
public final class S3Uploader {

  private final S3Client s3;
  private final String bucket;

  public S3Uploader(S3Client s3, String bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  /**
   * Upload the three objects for one (symbol, stream, hour). Verifies via HeadObject after each
   * PUT.
   */
  public void upload(String dataKey, Path data, Path sidecar, Path manifest) throws IOException {
    put(dataKey, data);
    headOrThrow(dataKey);
    put(dataKey + ".sha256", sidecar);
    headOrThrow(dataKey + ".sha256");
    String manifestKey = dataKey.replaceFirst("\\.jsonl\\.zst$", ".manifest.json");
    put(manifestKey, manifest);
    headOrThrow(manifestKey);
  }

  private void put(String key, Path file) {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(file));
  }

  private void headOrThrow(String key) {
    s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
  }
}
