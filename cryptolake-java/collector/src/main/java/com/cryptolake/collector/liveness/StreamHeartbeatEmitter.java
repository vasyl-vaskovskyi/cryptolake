package com.cryptolake.collector.liveness;

import com.cryptolake.collector.capture.RawFrameCapture;
import com.cryptolake.collector.capture.SessionSeqAllocator;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.kafka.TopicNames;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Emits per-{@code (symbol, stream)} liveness heartbeat envelopes to Kafka every 5 seconds.
 *
 * <p>Ports {@code StreamHeartbeatEmitter} from {@code src/collector/heartbeat.py} (commit 3e068b7).
 *
 * <p>Three states:
 * <ul>
 *   <li>{@link HeartbeatStatus#ALIVE} — data flowed within 2 × interval (10s)
 *   <li>{@link HeartbeatStatus#SUBSCRIBED_SILENT} — WS up, but no data within 2 × interval
 *   <li>{@link HeartbeatStatus#DISCONNECTED} — WebSocket currently down
 * </ul>
 *
 * <p>Heartbeats bypass the per-stream cap path via
 * {@link KafkaProducerBridge#publishRaw(String, byte[], byte[])} — they ARE the liveness signal,
 * so cap-dropping would defeat the point (design §2.10b).
 *
 * <p>Thread safety: virtual-thread loop; shared map reads are safe via CHM.
 */
public final class StreamHeartbeatEmitter {

  private static final StructuredLogger log = StructuredLogger.of(StreamHeartbeatEmitter.class);

  private final KafkaProducerBridge producer;
  private final EnvelopeCodec codec;
  private final String exchange;
  private final List<String> symbols;
  private final List<String> streams;
  private final String collectorSessionId;
  private final RawFrameCapture capture;
  private final SessionSeqAllocator seqAllocator;
  private final java.util.concurrent.ConcurrentHashMap<String, Boolean> wsConnected;
  private final ClockSupplier clock;
  private final CollectorMetrics metrics;
  private final String topicPrefix;
  private final Duration interval;

  private final CountDownLatch stopLatch = new CountDownLatch(1);

  public StreamHeartbeatEmitter(
      KafkaProducerBridge producer,
      EnvelopeCodec codec,
      String exchange,
      String topicPrefix,
      List<String> symbols,
      List<String> streams,
      String collectorSessionId,
      RawFrameCapture capture,
      SessionSeqAllocator seqAllocator,
      java.util.concurrent.ConcurrentHashMap<String, Boolean> wsConnected,
      ClockSupplier clock,
      CollectorMetrics metrics,
      Duration interval) {
    this.producer = producer;
    this.codec = codec;
    this.exchange = exchange;
    this.topicPrefix = topicPrefix;
    this.symbols = symbols;
    this.streams = streams;
    this.collectorSessionId = collectorSessionId;
    this.capture = capture;
    this.seqAllocator = seqAllocator;
    this.wsConnected = wsConnected;
    this.clock = clock;
    this.metrics = metrics;
    this.interval = interval;
  }

  /** Starts the heartbeat loop on a virtual thread. */
  public void start() {
    Thread.ofVirtual()
        .name("stream-heartbeat-emitter")
        .start(this::loop);
  }

  /** Stops the heartbeat loop. */
  public void stop() {
    stopLatch.countDown();
  }

  private void loop() {
    log.info("stream_heartbeat_started", "interval_ms", interval.toMillis());
    while (true) {
      try {
        if (stopLatch.await(interval.toMillis(), TimeUnit.MILLISECONDS)) {
          break; // stop requested (Tier 5 A3)
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      emitAll();
    }
    log.info("stream_heartbeat_stopped");
  }

  private void emitAll() {
    long nowNs = clock.nowNs();
    long twoIntervalNs = interval.toNanos() * 2;

    for (String symbol : symbols) {
      for (String stream : streams) {
        if (com.cryptolake.collector.adapter.StreamKey.REST_ONLY_STREAMS.contains(stream)) {
          continue; // no heartbeats for REST-only streams
        }

        String key = RawFrameCapture.tupleKey(symbol, stream);
        Long lastDataNs = capture.lastReceivedAt.get(key);
        long lastSeq = seqAllocator.current(symbol, stream);

        // Determine status
        HeartbeatStatus status;
        Boolean connected = wsConnected.get("ws");
        boolean wsUp = Boolean.TRUE.equals(connected);

        if (!wsUp) {
          status = HeartbeatStatus.DISCONNECTED;
        } else if (lastDataNs == null || (nowNs - lastDataNs) > twoIntervalNs) {
          status = HeartbeatStatus.SUBSCRIBED_SILENT;
        } else {
          status = HeartbeatStatus.ALIVE;
        }

        HeartbeatEnvelope env = HeartbeatEnvelope.of(
            exchange, symbol, stream, collectorSessionId,
            nowNs, lastDataNs, lastSeq, status);

        try {
          byte[] bytes = codec.toJsonBytes(env);
          String topic = TopicNames.forStream(topicPrefix, exchange, stream);
          byte[] kafkaKey = TopicNames.symbolKey(symbol);
          producer.publishRaw(topic, kafkaKey, bytes);
          metrics.heartbeatsEmitted(exchange, symbol, stream, status.toJsonValue()).increment();
        } catch (UncheckedIOException e) {
          log.warn("heartbeat_serialize_failed", "symbol", symbol, "stream", stream,
              "error", e.getMessage());
        }
      }
    }
  }
}
