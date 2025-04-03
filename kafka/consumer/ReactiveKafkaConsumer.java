package com.kmbl.cros.accountinquiryservice.service.kafka.consumer;

import static com.kmbl.cros.accountinquiryservice.utils.PartitionUtils.inMemoryPartitionId;

import com.kmbl.cros.accountinquiryservice.utils.MetricUtil;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.observation.ObservationRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.Disposable;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.observation.KafkaReceiverObservation.DefaultKafkaReceiverObservationConvention;
import reactor.util.retry.Retry;

/**
 * Reactive Kafka Consumer for processing events of generic type {@code T}.
 *
 * <p>
 * This class provides a reactive implementation of a Kafka consumer, designed for processing events
 * asynchronously and efficiently. It extends the {@link Thread} class and implements the
 * {@link AutoCloseable} interface to allow for proper resource management.
 *
 * <p>
 * Usage: - Create an instance of this class by supplying a {@link ConsumerConfiguration} during
 * instantiation. - Start the Kafka consumer by invoking the {@code run()} method. - Implement
 * custom logic for processing received Kafka records by overriding the {@code processRecord}
 * method. - Gracefully close the Kafka consumer by invoking the {@code close()} method when it is
 * no longer needed.
 *
 * @param <T> The generic type representing the value type of Kafka records.
 * @see ConsumerConfiguration
 * @see Disposable
 * @see ReceiverRecord
 * @see AutoCloseable
 */
@Slf4j
public class ReactiveKafkaConsumer<T> extends Thread implements AutoCloseable {

  private final ConsumerConfiguration<T> config;
  private final KafkaReceiver<String, T> receiver;
  protected MetricUtil metricUtil;
  private Disposable flux;

  /**
   * Constructor for creating a Reactive Kafka Consumer.
   *
   * @param config The configuration for the Kafka consumer, including properties like bootstrap
   *               servers, group ID, topic, SSL settings, and more.
   */
  public ReactiveKafkaConsumer(ConsumerConfiguration<T> config, MetricUtil metricUtil) {

    this.config = config;
    this.metricUtil = metricUtil;

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollRecords());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        config.getValueDeserializer().getName());
    if ("AWS_MSK_IAM".equals(config.getSecurityProtocol())) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      props.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
      props.put(SaslConfigs.SASL_JAAS_CONFIG,
          "software.amazon.msk.auth.iam.IAMLoginModule required;");
      props.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
          "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
    }

    log.info("Kafka Consumer Properties: {}", props);

    var registry = ObservationRegistry.create();
    var commitIntervalMillis = config.getDeferredCommitConfig().getCommitIntervalMillis();
    var options = ReceiverOptions.<String, T>create(props)
        .subscription(List.of(config.getTopic(), config.getDlqConfig().getDlqTopic()))
        .maxDeferredCommits(config.getDeferredCommitConfig().getMaxDeferredCommits())
        .commitInterval(Duration.ofMillis(commitIntervalMillis))
        .commitBatchSize(config.getDeferredCommitConfig().getCommitBatchSize())
        .withObservation(registry, DefaultKafkaReceiverObservationConvention.INSTANCE)
        .addAssignListener(partitions -> log.info("Partitions assigned: {}", partitions))
        .addRevokeListener(partitions -> log.info("Partitions revoked: {}", partitions));
    this.receiver = KafkaReceiver.create(new ReactiveConsumerFactory(), options);
  }

  class ReactiveConsumerFactory extends ConsumerFactory {

    @Override
    public <K, V> Consumer<K, V> createConsumer(ReceiverOptions<K, V> config) {
      var consumer = super.createConsumer(config);
      var kmetrics = new KafkaClientMetrics(consumer);
      kmetrics.bindTo(metricUtil.getRegistry());
      return consumer;
    }
  }

  /**
   * Starts the Kafka consumer to receive and process Kafka records asynchronously.
   *
   * <p>
   * Overrides the {@code run()} method from the {@link Thread} class. Creates a reactive flow using
   * Reactor's {@link Disposable} to handle Kafka message reception and processing.
   */
  @Override
  public void run() {
    log.info("CROS Kafka consumer started successfully for topic: {}.", config.getTopic());
    var scheduler = scheduler();
    flux = receiver.receive()
        .doOnError(e -> log.error("Error receiving event in Kafka consumer. Will retry", e))
        .retryWhen(Retry.fixedDelay(60L, Duration.ofMinutes(1)))
        .groupBy(r -> inMemoryPartitionId(config.getProcessor().partitionKey(r.value()),
            config.getInMemoryPartitions()))
        .flatMap(pf -> pf.publishOn(scheduler).map(this::processRecord))
        .subscribe();
    log.info("Kafka consumer stopped for topic: {}", config.getTopic());
  }

  /**
   * Processes a received Kafka record.
   *
   * <p>
   * Implement this method to define custom logic for processing each Kafka record. This method is
   * called for each record received by the Kafka consumer.
   *
   * @param message The Kafka record encapsulated in a {@link ReceiverRecord}. returns
   *                ReceiverRecord.
   */
  private ReceiverRecord<String, T> processRecord(ReceiverRecord<String, T> message) {
    try (var timeIt = metricUtil.timeIt("MessageProcessingLatency")) {
      log.debug("Processing message: {} from topic: {}", message, config.getTopic());
      var startTime = System.nanoTime();
      try {
        var currInstant = Instant.now();
        // This timestamp is set by either the producer or Kafka
        var kafkaTimestamp = Instant.ofEpochMilli(message.timestamp());
        var klag = Duration.between(kafkaTimestamp, currInstant);
        timeIt.emitTimeMetric("ConsumptionLag." + config.getTopic(), klag.toMillis());
        config.getProcessor().process(message);

      } catch (Exception e) {
        log.error(
            "Error processing event with key: {}, value: {} from topic: {}",
            message.key(),
            message.value(),
            config.getTopic(),
            e);
        if (config.getFailureHandler() != null) {
          config.getFailureHandler()
              .handle(
                  config.getBootstrapServers(),
                  config.getDlqConfig(),
                  config.getSecurityProtocol(),
                  message);
        } else {
          // TODO : not to acknowledge message in case of failed to push in DLQ topic.
          log.error("DLQ Failure Handler is not configured for topic: {}", config.getTopic());
        }
      }

      message.receiverOffset().acknowledge();
      var endTime = System.nanoTime();
      var totalTimeMillis = (endTime - startTime) / 1000_000L;
      timeIt.emitTimeMetric("Latency." + config.getTopic(), totalTimeMillis);
      log.trace("Finished processing message: {} from topic: {}", message, config.getTopic());
      return message;
    }
  }

  private Scheduler scheduler() {
    var scheduler = Schedulers.newBoundedElastic(
        config.getInMemoryPartitions(),
        Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
        config.getProcessorThreadPoolName());
    return Micrometer.timedScheduler(scheduler, metricUtil.getRegistry(),
        config.getTopic() + "-consumers.scheduler");
  }

  /**
   * Closes the Kafka consumer, releasing associated resources.
   *
   * <p>
   * Overrides the {@code close()} method from the {@link AutoCloseable} interface. Invoking this
   * method ensures a graceful shutdown of the Kafka consumer.
   */
  @Override
  public void close() {
    log.info("CROS-ReactiveKafkaConsumer termination initiated for topic: {}", config.getTopic());
    try {
      this.flux.dispose();
    } catch (Exception e) {
      log.info("Ignoring error while disposing consumer flux for topic: {} with exception: {}",
          config.getTopic(), e);
    }
  }
}