package com.kmbl.cros.accountinquiryservice.service.kafka.consumer;

import reactor.kafka.receiver.ReceiverRecord;

/**
 * Interface for a generic message consumer.
 *
 * <p>
 * This interface defines the contract for a message consumer that processes Kafka records with key
 * of type {@code String} and value of generic type {@code T}.
 *
 * <p>
 * Methods: - {@code process}: This method is responsible for processing a Kafka record encapsulated
 * in a {@link ReceiverRecord}. Implementations should define the logic for handling the events.
 *
 * @param <T> The generic type representing the value type of Kafka records.
 *            <p>
 *            Usage: - Implement this interface in classes that will handle the processing of Kafka
 *            records. - The `process` method is invoked by the Kafka consumer to handle each
 *            received message.
 * @see ReceiverRecord
 */
public interface MessageConsumer<T> {

  /**
   * Processes a Kafka record.
   *
   * <p>
   * Implementations of this method should define the logic for handling the received Kafka record.
   *
   * @param message The Kafka record encapsulated in a {@link ReceiverRecord}.
   */
  void process(ReceiverRecord<String, T> message);

  String partitionKey(T message);
}