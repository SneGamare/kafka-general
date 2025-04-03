package com.kmbl.cros.accountinquiryservice.service.kafka;

import static com.kmbl.cros.accountinquiryservice.constants.MetricConstants.TRANSACTION_DUPLICATE;
import static com.kmbl.cros.accountinquiryservice.constants.MetricConstants.TRANSACTION_LAG_ERROR;
import static com.kmbl.cros.accountinquiryservice.utils.CbsUtils.byteBufferToStr;

import com.kmbl.cros.accountinquiryservice.event.schema.DtdEvents.DailyTranDetailTable;
import com.kmbl.cros.accountinquiryservice.monitor.TransactionEventConsumptionTracker;
import com.kmbl.cros.accountinquiryservice.service.TransactionEventsIngestionService;
import com.kmbl.cros.accountinquiryservice.service.kafka.consumer.MessageConsumer;
import com.kmbl.cros.accountinquiryservice.utils.EpochProvider;
import com.kmbl.cros.accountinquiryservice.utils.MetricUtil;
import java.time.Duration;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.redisson.api.RedissonClient;
import reactor.kafka.receiver.ReceiverRecord;

/**
 * Kafka Message Consumer for handling events related to DailyTranDetailTable.
 *
 * <p>
 * This class implements the {@link MessageConsumer} interface, specifying the processing logic for
 * Kafka records with key of type {@code String} and value of type {@code DailyTranDetailTable}. The
 * processing involves invoking the {@link TransactionEventsIngestionService} to handle the
 * ingestion of transaction events.
 *
 * @see MessageConsumer
 * @see ReceiverRecord
 * @see DailyTranDetailTable
 * @see TransactionEventsIngestionService
 */
@Slf4j
public class CbsTransactionConsumer implements MessageConsumer<DailyTranDetailTable> {
  private final TransactionEventsIngestionService svc;
  private final MetricUtil metricUtil;
  @Nullable
  private final TransactionEventConsumptionTracker<DailyTranDetailTable> transactionEventsConsumptionTracker;

  public CbsTransactionConsumer(TransactionEventsIngestionService svc, EpochProvider epochProvider,
      RedissonClient redissonClient, MetricUtil metricUtil) {
    this.svc = svc;
    this.transactionEventsConsumptionTracker = tryCreateTracker(redissonClient, epochProvider);
    this.metricUtil = metricUtil;
  }

  /**
   * Processes a received Kafka record containing DailyTranDetailTable event data, facilitating
   * transaction event handling and ensuring proper logging and error handling.
   *
   * <p>
   * **Steps:** 1. Logs a message indicating the receipt of the Kafka record. 2. Extracts the event
   * data if present in the message value. 3. Delegates the handling of the extracted event data to
   * the {@link TransactionEventsIngestionService}. 4. In case of unexpected database errors, logs
   * an error message and includes the message value and exception details.
   *
   * @param message The Kafka record encapsulated in a {@link ReceiverRecord} containing
   *                DailyTranDetailTable event data.
   */
  @Override
  public void process(ReceiverRecord<String, DailyTranDetailTable> message) {
    if (message.value() != null) {
      var event = message.value();
      var before = event.getBefore();
      var after = event.getAfter();

      if (event.getPos() == null || event.getSourceScn() == null) {
        log.error("Aborting request as received DTD event with empty scn or pos field : {}",
            message);
      } else if (after != null && after.equals(before)) {
        log.debug(
            "Aborting request as received DTD event with same before and after state for ACID: {}",
            byteBufferToStr(after.getACID()));
        metricUtil.counter(TRANSACTION_DUPLICATE).increment();
      } else if (after != null) {
        boolean isDataProcessed = svc.handleTransactionEventIngestion(message);
        try {
          if (isDataProcessed && transactionEventsConsumptionTracker != null && after != null
              && after.getPSTDDATE() != null) {
            var marker = transactionEventsConsumptionTracker.markConsumed(message);
          }
        } catch (Exception e) {
          log.error("Transaction lag monitor error", e);
          metricUtil.counter(TRANSACTION_LAG_ERROR).increment();
        }
      }
    }
  }

  @Override
  public String partitionKey(DailyTranDetailTable message) {
    try {
      var acid = byteBufferToStr(message.getAfter().getACID());
      return acid == null ? "" : acid;
    } catch (Exception e) {
      return "";
    }
  }

  public static <DailyTranDetailTable> TransactionEventConsumptionTracker<com.kmbl.cros.accountinquiryservice.event.schema.DtdEvents.DailyTranDetailTable> tryCreateTracker(
      @Nullable RedissonClient redisson,
      @Nullable EpochProvider epochProvider
  ) {
    Validate.notNull(redisson, "Redisson has to be specified if tracking is enabled");
    Validate.notNull(epochProvider, "Epoch provider has to be specified if "
        + "tracking is enabled");
    log.info("Creating event consumption tracker for topic: {}", "topics");
    return new TransactionEventConsumptionTracker<>(redisson, "cros-transaction-events",
        epochProvider, Duration.ofMillis(Long.valueOf(2000)));
  }
}