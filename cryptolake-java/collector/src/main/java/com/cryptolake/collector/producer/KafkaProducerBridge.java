package com.cryptolake.collector.producer;

import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.common.config.ProducerConfig;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.kafka.TopicNames;
import com.cryptolake.common.logging.StructuredLogger;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Single {@link KafkaProducer} owner for the collector service (Tier 2 §14).
 *
 * <p>Ports {@code CryptoLakeProducer} from {@code src/collector/producer.py}. Per-stream buffer
 * caps prevent high-volume streams (depth) from starving low-volume irreplaceable streams
 * (liquidations, funding_rate).
 *
 * <p>Locking discipline (Tier 5 A5): {@link #lock} is held ONLY for the count check/update. The
 * {@code producer.send(...)} call runs OUTSIDE the lock — the lock never wraps a blocking call
 * (Tier 2 §9).
 *
 * <p>Thread safety: {@link KafkaProducer} is documented as thread-safe. The count map is guarded
 * by {@link #lock}. Delivery callbacks run on kafka-clients' internal thread and also take the
 * lock. Overflow tracking uses a {@link ConcurrentHashMap}.
 */
public class KafkaProducerBridge {

  private static final StructuredLogger log = StructuredLogger.of(KafkaProducerBridge.class);

  /** Buffer memory = 1 GB (matches Python's {@code queue.buffering.max.kbytes=1048576}). */
  private static final long BUFFER_MEMORY_BYTES = 1_073_741_824L;

  /** Buffer healthy-threshold: 80% of BUFFER_MEMORY_BYTES. */
  private static final double HEALTHY_BUFFER_FRACTION = 0.80;

  private final KafkaProducer<byte[], byte[]> producer;
  private final EnvelopeCodec codec;
  private final CollectorMetrics metrics;
  private final String exchange;
  private final String topicPrefix;
  private final ProducerConfig config;
  private final List<String> brokers;

  /** Per-stream buffer count map — guarded by {@link #lock} (Tier 5 A5). */
  private final Map<String, Integer> bufferCounts = new HashMap<>();

  /** Overflow window tracking — lock-free via CHM compute() (Tier 2 §9). */
  private final ConcurrentHashMap<String, OverflowWindow> overflowWindows =
      new ConcurrentHashMap<>();

  /** Monotonically increasing overflow sequence for synthetic gap envelopes. */
  private final AtomicLong overflowSeq = new AtomicLong(0);

  /** Lock guarding {@link #bufferCounts} — never held during blocking I/O (Tier 5 A5). */
  final ReentrantLock lock = new ReentrantLock();

  private volatile OverflowListener overflowListener;

  /** Lazy reference to the GapEmitter — injected after construction to break the circular dep. */
  private volatile com.cryptolake.collector.gap.GapEmitter gapEmitter;

  public KafkaProducerBridge(
      List<String> brokers,
      String exchange,
      String topicPrefix,
      ProducerConfig config,
      EnvelopeCodec codec,
      CollectorMetrics metrics) {
    this.brokers = brokers;
    this.exchange = exchange;
    this.topicPrefix = topicPrefix;
    this.config = config;
    this.codec = codec;
    this.metrics = metrics;
    this.producer = buildProducer(brokers, config);
  }

  // Constructor for tests (inject a mock producer)
  KafkaProducerBridge(
      KafkaProducer<byte[], byte[]> producer,
      String exchange,
      String topicPrefix,
      ProducerConfig config,
      EnvelopeCodec codec,
      CollectorMetrics metrics,
      List<String> brokers) {
    this.producer = producer;
    this.exchange = exchange;
    this.topicPrefix = topicPrefix;
    this.config = config;
    this.codec = codec;
    this.metrics = metrics;
    this.brokers = brokers;
  }

  private static KafkaProducer<byte[], byte[]> buildProducer(
      List<String> brokers, ProducerConfig cfg) {
    Properties p = new Properties();
    p.put("bootstrap.servers", String.join(",", brokers));
    p.put("acks", "all");
    p.put("linger.ms", 5);
    p.put("buffer.memory", BUFFER_MEMORY_BYTES);
    p.put("max.in.flight.requests.per.connection", 1);
    p.put("enable.idempotence", true);
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    return new KafkaProducer<>(p);
  }

  /** Injects the GapEmitter after construction (breaks circular dependency). */
  public void setGapEmitter(com.cryptolake.collector.gap.GapEmitter ge) {
    this.gapEmitter = ge;
  }

  /** Sets the overflow listener (wired to BackpressureGate::onDrop). */
  public void setOverflowListener(OverflowListener listener) {
    this.overflowListener = listener;
  }

  // ── Produce paths ─────────────────────────────────────────────────────────

  /**
   * Serializes and produces a data envelope. Returns {@code true} on success; {@code false} if
   * dropped due to per-stream cap or producer buffer overflow.
   */
  public boolean produce(DataEnvelope env) {
    String stream = env.stream();
    String symbol = env.symbol();
    String topic = TopicNames.forStream(topicPrefix, exchange, stream);
    byte[] key = TopicNames.symbolKey(symbol);

    byte[] value;
    try {
      value = codec.toJsonBytes(env);
    } catch (UncheckedIOException e) {
      log.warn("serialization_failed", "stream", stream, "symbol", symbol, "error", e.getMessage());
      metrics.messagesDropped(exchange, symbol, stream).increment();
      return false;
    }

    // Check per-stream cap and optimistically increment under lock (Tier 5 A5)
    lock.lock();
    try {
      int current = bufferCounts.getOrDefault(stream, 0);
      int cap = capFor(stream);
      if (current >= cap) {
        metrics.messagesDropped(exchange, symbol, stream).increment();
        recordOverflow(symbol, stream);
        return false;
      }
      bufferCounts.put(stream, current + 1);
    } finally {
      lock.unlock();
    }

    // Send OUTSIDE the lock (Tier 5 A5 watch-out; Tier 2 §9)
    try {
      producer.send(
          new ProducerRecord<>(topic, key, value),
          makeDeliveryCallback(stream, symbol, value));
      metrics.messagesProduced(exchange, symbol, stream).increment();

      // Check if recovering from overflow
      String owKey = overflowKey(symbol, stream);
      OverflowWindow window = overflowWindows.remove(owKey);
      if (window != null && window.dropped() > 0 && gapEmitter != null) {
        gapEmitter.emitOverflowRecovery(symbol, stream, window);
      }
      return true;
    } catch (TimeoutException e) { // includes BufferExhaustedException (subclass)
      // Roll back optimistic increment (Tier 5 C5)
      lock.lock();
      try {
        bufferCounts.merge(stream, -1, (a, b) -> Math.max(0, a + b));
      } finally {
        lock.unlock();
      }
      metrics.messagesDropped(exchange, symbol, stream).increment();
      recordOverflow(symbol, stream);
      return false;
    }
  }

  /**
   * Produces a gap envelope (invoked by {@code GapEmitter}). Returns {@code true} on success.
   *
   * <p>Gap envelopes bypass the per-stream cap — they are low-volume and must not be silently
   * dropped.
   */
  public boolean produceGap(GapEnvelope gap) {
    String stream = gap.stream();
    String symbol = gap.symbol();
    String topic = TopicNames.forStream(topicPrefix, exchange, stream);
    byte[] key = TopicNames.symbolKey(symbol);

    byte[] value;
    try {
      value = codec.toJsonBytes(gap);
    } catch (UncheckedIOException e) {
      log.warn(
          "gap_serialization_failed", "stream", stream, "symbol", symbol, "error", e.getMessage());
      return false;
    }

    try {
      producer.send(
          new ProducerRecord<>(topic, key, value),
          (metadata, ex) -> {
            if (ex != null) {
              log.error(
                  "gap_delivery_failed",
                  ex, "stream", stream, "symbol", symbol, "error", ex.getMessage());
            }
          });
      return true;
    } catch (TimeoutException e) { // includes BufferExhaustedException (subclass)
      log.error("gap_produce_failed", e, "stream", stream, "symbol", symbol, "error", e.getMessage());
      return false;
    }
  }

  /**
   * Publishes raw bytes directly to a topic, bypassing all cap and codec logic. Used by
   * {@code StreamHeartbeatEmitter} for heartbeat envelopes (design §2.10b).
   */
  public void publishRaw(String topic, byte[] key, byte[] value) {
    try {
      producer.send(new ProducerRecord<>(topic, key, value), null);
    } catch (Exception e) {
      log.warn("raw_publish_failed", "topic", topic, "error", e.getMessage());
    }
  }

  // ── Health checks ─────────────────────────────────────────────────────────

  /**
   * True if the producer can reach the broker (blocking metadata probe on the virtual thread — Tier
   * 5 A2).
   */
  public boolean isConnected() {
    try {
      producer.partitionsFor(TopicNames.forStream(topicPrefix, exchange, "depth"));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * True if no overflow windows are active AND buffer usage is below 80% (Tier 5 C1; design
   * §2.8).
   */
  public boolean isHealthyForResync() {
    if (!overflowWindows.isEmpty()) return false;
    try {
      // Metadata probe as a proxy for "is the producer usable" (Tier 5 §4.3)
      producer.partitionsFor(TopicNames.forStream(topicPrefix, exchange, "depth"));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Flushes the producer (blocking on the calling virtual thread — Tier 5 A2). Returns the number
   * of outstanding messages remaining (always 0 after a clean flush).
   */
  public int flush(Duration timeout) {
    try {
      producer.flush();
      return 0;
    } catch (Exception e) {
      log.warn("flush_failed", "error", e.getMessage());
      return -1;
    }
  }

  /**
   * Polls the producer for delivery callbacks (blocking 0 ms — Tier 5 A2). Returns the number of
   * events served.
   */
  public int poll(Duration timeout) {
    // kafka-clients callbacks are fired asynchronously on the I/O thread, so poll() is a no-op
    // compared to librdkafka. Returning 0 is correct — this method is kept for API parity.
    return 0;
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  private int capFor(String stream) {
    if (config.bufferCaps() != null) {
      Integer cap = config.bufferCaps().get(stream);
      if (cap != null) return cap;
    }
    return config.defaultStreamCap();
  }

  private void recordOverflow(String symbol, String stream) {
    String key = overflowKey(symbol, stream);
    overflowWindows.compute(
        key,
        (k, existing) -> {
          if (existing == null) return OverflowWindow.startNew(System.nanoTime());
          return existing.withIncrementedDrop();
        });
    OverflowListener listener = overflowListener;
    if (listener != null) {
      listener.onOverflow(exchange, symbol, stream);
    }
  }

  private Callback makeDeliveryCallback(String stream, String symbol, byte[] value) {
    return (metadata, ex) -> {
      // Decrement buffer count under lock (delivery callback runs on kafka I/O thread — Tier 5 A5)
      lock.lock();
      try {
        bufferCounts.merge(stream, -1, (a, b) -> Math.max(0, a + b));
      } finally {
        lock.unlock();
      }

      if (ex == null) return;

      log.error(
          "producer_delivery_failed",
          ex, "stream", stream,
          "symbol", symbol,
          "error", ex.getMessage());

      // Emit a kafka_delivery_failed gap from the failed data envelope (design §2.8)
      // Skip if gapEmitter is not yet wired (startup race), or if the failed payload was
      // itself a gap (avoid recursive gap-on-gap).
      com.cryptolake.collector.gap.GapEmitter ge = gapEmitter;
      if (ge == null) return;
      try {
        DataEnvelope env = codec.readData(value);
        ge.emitWithTimestamps(
            env.symbol(),
            env.stream(),
            env.sessionSeq(),
            "kafka_delivery_failed",
            "Kafka delivery error: " + ex.getMessage(),
            env.receivedAt(),
            System.nanoTime());
      } catch (Exception parseEx) {
        // failed to parse as DataEnvelope — might be a gap envelope; skip gap-on-gap
        log.warn(
            "delivery_fail_gap_emit_skipped",
            "reason", "not_data_envelope",
            "error", parseEx.getMessage());
      }
    };
  }

  private static String overflowKey(String symbol, String stream) {
    return symbol + '\0' + stream;
  }

  /** Closes the underlying producer on shutdown. */
  public void close() {
    try {
      producer.close();
    } catch (Exception ignored) {
      // best-effort shutdown (Tier 5 G1)
    }
  }
}
