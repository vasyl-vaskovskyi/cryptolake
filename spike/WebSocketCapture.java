// Gate 3 spike: prove java.net.http.WebSocket can deliver raw_text bytes that,
// after UTF-8 round-trip, are byte-identical to what was sent on the wire.
//
// Usage: java WebSocketCapture.java <ws-url> <payloads-file>
// Exits 0 on PASS, 1 on FAIL, printing a per-payload diff on failure.

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class WebSocketCapture {

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "ws://127.0.0.1:8765";
        Path payloadsFile = args.length > 1 ? Path.of(args[1]) : Path.of("spike/payloads.jsonl");

        List<String> expected = Files.readAllLines(payloadsFile);
        expected.removeIf(String::isEmpty);
        int expectedCount = expected.size();
        System.out.println("expecting " + expectedCount + " frames");

        List<byte[]> captured = new ArrayList<>();
        CompletableFuture<Void> allDone = new CompletableFuture<>();

        WebSocket.Listener listener = new WebSocket.Listener() {
            final StringBuilder accText = new StringBuilder();
            ByteBuffer accBin = null;

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                accText.append(data);
                if (last) {
                    byte[] bytes = accText.toString().getBytes(StandardCharsets.UTF_8);
                    captured.add(bytes);
                    accText.setLength(0);
                    if (captured.size() >= expectedCount) allDone.complete(null);
                }
                ws.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
                byte[] chunk = new byte[data.remaining()];
                data.get(chunk);
                if (accBin == null) {
                    accBin = ByteBuffer.allocate(chunk.length);
                } else {
                    ByteBuffer grown = ByteBuffer.allocate(accBin.position() + chunk.length);
                    accBin.flip();
                    grown.put(accBin);
                    accBin = grown;
                }
                accBin.put(chunk);
                if (last) {
                    accBin.flip();
                    byte[] bytes = new byte[accBin.remaining()];
                    accBin.get(bytes);
                    captured.add(bytes);
                    accBin = null;
                    if (captured.size() >= expectedCount) allDone.complete(null);
                }
                ws.request(1);
                return null;
            }

            @Override
            public void onError(WebSocket ws, Throwable error) {
                allDone.completeExceptionally(error);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                if (!allDone.isDone()) {
                    allDone.completeExceptionally(new IllegalStateException(
                        "server closed before receiving all frames: got " + captured.size()
                        + " / expected " + expectedCount));
                }
                return null;
            }
        };

        HttpClient http = HttpClient.newHttpClient();
        WebSocket ws = http.newWebSocketBuilder()
                .buildAsync(URI.create(url), listener)
                .get(10, TimeUnit.SECONDS);

        try {
            allDone.get(15, TimeUnit.SECONDS);
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").orTimeout(2, TimeUnit.SECONDS);
        }

        // Compare captured bytes against file-source bytes per frame.
        boolean allPass = true;
        HexFormat hex = HexFormat.of();
        for (int i = 0; i < expectedCount; i++) {
            byte[] expectedBytes = expected.get(i).getBytes(StandardCharsets.UTF_8);
            byte[] actualBytes = captured.get(i);
            boolean ok = Arrays.equals(expectedBytes, actualBytes);
            String expSha = sha256Hex(expectedBytes);
            String actSha = sha256Hex(actualBytes);
            System.out.printf("frame %d: %s   expected(%d bytes, sha=%s...)  actual(%d bytes, sha=%s...)%n",
                    i,
                    ok ? "PASS" : "FAIL",
                    expectedBytes.length,
                    expSha.substring(0, 12),
                    actualBytes.length,
                    actSha.substring(0, 12));
            if (!ok) {
                System.out.println("  expected hex: " + hex.formatHex(expectedBytes));
                System.out.println("  actual   hex: " + hex.formatHex(actualBytes));
                allPass = false;
            }
        }

        if (allPass) {
            System.out.println("==== ALL " + expectedCount + " FRAMES BYTE-IDENTICAL ====");
            System.exit(0);
        } else {
            System.out.println("==== GATE 3 SPIKE FAILED ====");
            System.exit(1);
        }
    }

    static String sha256Hex(byte[] input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(input));
    }
}
