package com.kmbl.cros.accountinquiryservice.service.kafka;

import static com.kmbl.cros.accountinquiryservice.constants.AppConstants.SCHEMA_TYPE_TDA;
import static com.kmbl.cros.accountinquiryservice.constants.MetricConstants.TD_SUMMARY_BALANCE_EVENT_INGESTION_FAILURE;
import static com.kmbl.cros.accountinquiryservice.utils.CbsUtils.byteBufferToStr;
import com.kmbl.cros.accountinquiryservice.event.schema.GamEvents.GeneralAcctMastTable;
import com.kmbl.cros.accountinquiryservice.exception.TdBalanceIngestionException;
import com.kmbl.cros.accountinquiryservice.monitor.BalanceEventConsumptionTracker;
import com.kmbl.cros.accountinquiryservice.service.BalanceEventsIngestionService;
import com.kmbl.cros.accountinquiryservice.service.TdBalanceEventsIngestionService;
import com.kmbl.cros.accountinquiryservice.service.kafka.consumer.MessageConsumer;
import com.kmbl.cros.accountinquiryservice.utils.EpochProvider;
import java.time.Duration;
import java.util.Objects;

import com.kmbl.cros.accountinquiryservice.utils.MetricUtil;
import org.apache.commons.lang3.Validate;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import reactor.kafka.receiver.ReceiverRecord;

/**
 * Kafka Message Consumer for handling events related to GeneralAcctMastTable.
 *
 * <p>
 * This class implements the {@link MessageConsumer} interface, specifying the processing logic for
 * Kafka records with key of type {@code String} and value of type {@code GeneralAcctMastTable}. The
 * processing involves invoking the {@link BalanceEventsIngestionService} to handle the ingestion of
 * balance events.
 *
 * @see MessageConsumer
 * @see ReceiverRecord
 * @see GeneralAcctMastTable
 * @see BalanceEventsIngestionService
 */
@Slf4j
public class CbsGamConsumer implements MessageConsumer<GeneralAcctMastTable> {

  private final BalanceEventsIngestionService svc;
  private final TdBalanceEventsIngestionService tdBalanceEventsIngestionService;
  private final MetricUtil metricUtil;

  @Nullable
  private final BalanceEventConsumptionTracker<GeneralAcctMastTable> balanceEventConsumptionTracker;

  public CbsGamConsumer(BalanceEventsIngestionService svc, EpochProvider epochProvider,
      RedissonClient redissonClient,TdBalanceEventsIngestionService tdBalanceEventsIngestionService,MetricUtil metricUtil ) {
    this.svc = svc;
    this.balanceEventConsumptionTracker = tryCreateTracker(redissonClient, epochProvider);
    this.tdBalanceEventsIngestionService = tdBalanceEventsIngestionService;
    this.metricUtil = metricUtil;
  }

  /**
   * Processes a received Kafka record containing GeneralAcctMastTable event data, facilitating
   * balance event handling and ensuring proper logging and error handling.
   *
   * <p>
   * **Steps:** 1. Logs a message indicating the receipt of the Kafka record.
   * 2. Extracts the event data if present in the message value.
   * 3. Delegates the handling of the extracted event data to the {@link BalanceEventsIngestionService} and {@link TdBalanceEventsIngestionService}.
   * 4. In case of unexpected database errors, logs an error message and includes the message value and exception details.
   *
   * @param message The Kafka record encapsulated in a {@link ReceiverRecord} containing
   *                GeneralAcctMastTable event data.
   */
  @Override
  public void process(ReceiverRecord<String, GeneralAcctMastTable> message) {
    if (message.value() != null) {
      var event = message.value();
      var before = event.getBefore();
      var after = event.getAfter();
      boolean tdException = false;
      if (event.getPos() == null || event.getSourceScn() == null) {
        log.info("Aborting request as received GAM event with empty scn or pos field : {}",
            message);
      } else if (after != null && !after.equals(before)) {
        boolean isDataProcessed = svc.handleBalanceEventIngestion(message);
        if (SCHEMA_TYPE_TDA.equals(byteBufferToStr(after.getSCHMTYPE()))
                && Objects.nonNull(byteBufferToStr(after.getFORACID()))
                && !byteBufferToStr(after.getFORACID()).endsWith("X")) {
          try {
            boolean isTdDataProcessed = tdBalanceEventsIngestionService.handleTdBalanceEventsIngestion(message);
          } catch (Exception e) {
            metricUtil.counter(TD_SUMMARY_BALANCE_EVENT_INGESTION_FAILURE).increment();
            log.error("Td summary ingestion failed for gam data for accountId {}. Exception: ",
                    byteBufferToStr(after.getFORACID()), e);
            tdException = true;
          }
        }
        try {
          if (isDataProcessed && balanceEventConsumptionTracker != null
              && after.getLCHGTIME() != null) {
            var marker = balanceEventConsumptionTracker.markConsumed(message);
          }
        } catch (Exception e) {
          log.error("Balance lag monitor error", e);
        }
      } else if (after != null) {
        log.debug("Aborting request as received GAM event with same before and after for"
            + " FORACID: {}", byteBufferToStr(message.value().getAfter().getFORACID()));
      }
      if (tdException) {
        throw new TdBalanceIngestionException("Exception occurred while ingesting gam data in td summary table");
      }
    }
  }


  @Override
  public String partitionKey(GeneralAcctMastTable message) {
    try {
      var forAcid = byteBufferToStr(message.getAfter().getFORACID());
      return forAcid != null ? forAcid : "";
    } catch (Exception e) {
      return "";
    }
  }

  public static <GeneralAcctMastTable> BalanceEventConsumptionTracker<com.kmbl.cros.accountinquiryservice.event.schema.GamEvents.GeneralAcctMastTable> tryCreateTracker(
      @Nullable RedissonClient redisson,
      @Nullable EpochProvider epochProvider
  ) {
    Validate.notNull(redisson, "Redisson has to be specified if tracking is enabled");
    Validate.notNull(epochProvider, "Epoch provider has to be specified if "
        + "tracking is enabled");
    log.info("Creating event consumption tracker for topic: {}", "cros-account-master-events");
    return new BalanceEventConsumptionTracker<>(redisson, "cros-account-master-events",
        epochProvider, Duration.ofMillis(Long.valueOf(2000)));
  }
}