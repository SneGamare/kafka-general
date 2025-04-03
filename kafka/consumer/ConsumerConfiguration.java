package com.kmbl.cros.accountinquiryservice.service.kafka.consumer;

import com.kmbl.cros.accountinquiryservice.service.kafka.failurehandler.FailureHandler;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Configuration class for Kafka Consumers handling events of generic type {@code T}.
 *
 * <p>
 * This class encapsulates the configuration parameters required for setting up a Kafka consumer to
 * receive events in Avro format. It provides flexibility by allowing the specification of the
 * generic type for the deserialization of event values.
 *
 * <p>
 * Properties:
 * - {@code bootstrapServers}: The Kafka bootstrap servers' addresses.
 * - {@code topic}: The Kafka topic from which events will be consumed.
 * - {@code groupId}: The consumer group ID.
 * - {@code valueDeserializer}: The class of the deserializer for decoding Avro-encoded values.
 * - {@code processor}: The {@link MessageConsumer} responsible for processing the received events.
 * - {@Code securityProtocol}: Security protocol for msk authentication.
 *
 * @param <T> The generic type representing the deserialized value type of Kafka events.
 *            <p>
 *            Usage: - This class is used to configure a Kafka consumer with specific parameters for
 *            consuming Avro-encoded events. - The properties are set through the constructor or
 *            appropriate setter methods. - It is intended for use in conjunction with a Kafka
 *            consumer implementation, such as {@link ReactiveKafkaConsumer}, to handle the
 *            reception and processing of events.
 * @see ReactiveKafkaConsumer
 * @see MessageConsumer
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfiguration<T> {

  private String bootstrapServers;
  private String topic;
  private String groupId;
  private Class<? extends Deserializer<T>> valueDeserializer;
  private int inMemoryPartitions;
  private int maxPollRecords;
  private String processorThreadPoolName;
  private MessageConsumer<T> processor;
  private DeferredCommitConfiguration deferredCommitConfig;
  private String securityProtocol;
  private FailureHandler<T> failureHandler;
  private DlqConfiguration<T> dlqConfig;

  @Builder
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DeferredCommitConfiguration {

    /**
     * Maximum number of messages that are not yet committed after which we pause the consumer to
     * apply backpressure.
     */
    private int maxDeferredCommits;

    /**
     * Frequency of committing the earliest contiguous offset (if possible) since the last commit
     * for a partition.
     * <p>
     * A commit is triggered (if possible) when either the commitIntervalMillis timer expires or the
     * batch size is exceeded.
     */
    private long commitIntervalMillis;

    /**
     * Maximum batch size after which the earliest contiguous offset (if possible) since the last
     * commit for a partition is committed.
     */
    private int commitBatchSize;
  }

  @Builder
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DlqConfiguration<T> {
    /**
     * dead-letter queue topic name, The failed consumer events will be published to.
     */
    private String dlqTopic;

    /**
     * Class to use for Serializing a Kafka message before publishing to
     */
    private Class<? extends Serializer> valueSerializer;
  }
}