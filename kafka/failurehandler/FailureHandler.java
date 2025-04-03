package com.kmbl.cros.accountinquiryservice.service.kafka.failurehandler;

import com.kmbl.cros.accountinquiryservice.service.kafka.consumer.ConsumerConfiguration.DlqConfiguration;
import reactor.kafka.receiver.ReceiverRecord;

public interface FailureHandler<T> {
    /**
     * usecase : To handle events which failed in message processing.
     *
     * @param bootstrapServers kafka server endpoints
     * @param dlqConfiguration has properties related to publishing to dlq.
     * @param securityProtocol used for SSL
     * @param message          record received by consumer.
     */
    void handle(
            String bootstrapServers,
            DlqConfiguration<T> dlqConfiguration,
            String securityProtocol,
            ReceiverRecord<String, T> message
    );
}