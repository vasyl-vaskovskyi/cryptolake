package com.cryptolake.collector.producer;

import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.common.config.ProducerConfig;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Test-only stub {@link KafkaProducerBridge} that records envelopes without Kafka I/O.
 *
 * <p>Lives in the {@code producer} package to access the package-private test constructor.
 */
public final class TestProducerBridge extends KafkaProducerBridge {

  public final List<DataEnvelope> dataEnvelopes = new ArrayList<>();
  public final List<GapEnvelope> gapEnvelopes = new ArrayList<>();
  public final List<byte[]> rawPublished = new ArrayList<>();

  public TestProducerBridge() {
    super(
        noopProducer(),
        "binance",
        "",
        new ProducerConfig(100_000, null, 10_000),
        new EnvelopeCodec(EnvelopeCodec.newMapper()),
        new CollectorMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)),
        List.of("localhost:9092"));
  }

  @Override
  public boolean produce(DataEnvelope env) {
    dataEnvelopes.add(env);
    return true;
  }

  @Override
  public boolean produceGap(GapEnvelope gap) {
    gapEnvelopes.add(gap);
    return true;
  }

  @Override
  public void publishRaw(String topic, byte[] key, byte[] value) {
    rawPublished.add(value);
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public boolean isHealthyForResync() {
    return true;
  }

  private static KafkaProducer<byte[], byte[]> noopProducer() {
    // Deliberately null — the overridden methods never call super
    return null;
  }
}
