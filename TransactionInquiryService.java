package com.kmbl.cros.accountinquiryservice.service;

import static com.kmbl.cros.accountinquiryservice.constants.AppConstants.TRANSACTION;
import static com.kmbl.cros.accountinquiryservice.constants.AppConstants.TRANSACTION_INQUIRY;
import static com.kmbl.cros.accountinquiryservice.constants.MetricConstants.TRANSACTION_LAZY_LOADING;
import static com.kmbl.cros.accountinquiryservice.constants.MetricConstants.TRANSACTION_PRIMARY_COMPARISON_COUNT;
import static com.kmbl.cros.accountinquiryservice.enums.DataSource.CROS;

import com.kmbl.cros.accountinquiryservice.config.channel.ChannelConfig;
import com.kmbl.cros.accountinquiryservice.constants.AppConstants;
import com.kmbl.cros.accountinquiryservice.enums.counters.TransactionChannelCounters;
import com.kmbl.cros.accountinquiryservice.event.schema.DtdEvents.DailyTranDetailTable;
import com.kmbl.cros.accountinquiryservice.exception.AccountBalancesInquiryServiceException;
import com.kmbl.cros.accountinquiryservice.exception.CbsClientException;
import com.kmbl.cros.accountinquiryservice.exception.DatabaseException;
import com.kmbl.cros.accountinquiryservice.exception.DatabaseTransientException;
import com.kmbl.cros.accountinquiryservice.monitor.Lag;
import com.kmbl.cros.accountinquiryservice.monitor.TransactionLagMonitor;
import com.kmbl.cros.accountinquiryservice.repository.dao.TransactionDao;
import com.kmbl.cros.accountinquiryservice.repository.dao.dynamodb.AsyncLazyLoader;
import com.kmbl.cros.accountinquiryservice.service.clients.FinacleClient;
import com.kmbl.cros.accountinquiryservice.service.dtos.AccountBalanceMasterData;
import com.kmbl.cros.accountinquiryservice.service.dtos.TransactionMasterData;
import com.kmbl.cros.accountinquiryservice.service.readlag.AsyncDataComparator;
import com.kmbl.cros.accountinquiryservice.service.utils.GradualDialUpPropertiesUtils;
import com.kmbl.cros.accountinquiryservice.utils.CounterMapUtil;
import com.kmbl.cros.accountinquiryservice.utils.EpochProvider;
import com.kmbl.cros.accountinquiryservice.utils.MetricUtil;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

/**
 * Encapsulates the logic for retrieving account recent transaction information.
 *
 * <p>This class acts as a service layer component responsible for handling recent transaction
 * inquiries. It interacts with both a database or a Finacle account inquiry client to retrieve
 * transaction data from different sources, providing a unified interface for recent transaction
 * information retrieval within the application.
 *
 * <p>This service primarily offers methods for fetching account recent transaction based on
 * account IDs, potentially handling data transformation, caching, or error handling to ensure
 * reliable and consistent access to  transaction information.
 *
 * @see TransactionDao
 */
@Slf4j
public class TransactionInquiryService {

    private static final int DEFAULT_TRANSACTION_COUNT = 10;

    private final TransactionDao transactionDao;
    private final FinacleClient finacleClient;
    private final AccountBalanceInquiryService bis;
    private final EpochProvider epochProvider;
    private final AsyncDataComparator asyncDataComparator;
    private final TransactionLagMonitor<DailyTranDetailTable> lagMonitorService;
    private final AsyncLazyLoader asyncLazyLoader;
    private final GradualDialUpPropertiesUtils gradualDialUpPropertiesUtils;
    private final MetricUtil metricUtil;
    private final ChannelConfig channelConfig;
    private final Map<String, Counter> counters; 

    @Setter // Adding setter for Unit test purpose.
    @Value("${transactions.comparison.percentage.rollout}")
    private int percentageRollout;

    @Setter // Adding setter for Unit test purpose.
    @Value("${cros.lazyLoading.enabled}")
    private boolean lazyLoadingEnabled;

    /**
     * Constructs a new {@link TransactionInquiryService} instance.
     *
     * @param transactionDao      A {@link TransactionDao} instance for accessing transaction data stored
     *                            in the database.
     * @param finacleClient       A {@link FinacleClient} instance for retrieving account transaction
     *                            information from Finacle.
     * @param bis                 A {@link AccountBalanceInquiryService} instance for fetching balance
     *                            details stored in the database.
     * @param epochProvider       The {@link EpochProvider} instance for epoch related operations.
     * @param lagMonitorService   A {@link TransactionLagMonitor} instance for accessing and computing
     *                            Transaction Lag Details.
     * @param asyncDataComparator A {@link AsyncDataComparator} instance for asynchronously comparing
     *                            transaction and balance inquiry data.
     * @param asyncLazyLoader     A {@link AsyncLazyLoader} instance for accessing AsyncLazyLoader.
     * @param gradualDialUpPropertiesUtils  A {@link GradualDialUpPropertiesUtils} instance for accessing
     *                                      Gradual DialUp configuration.
     * @param metricUtil          A {@link MetricUtil} instance for accessing Metric configurations.
     * @param channelConfig       A {@link ChannelConfig} instance for accessing channel level configurations.
     */
    public TransactionInquiryService(
            TransactionDao transactionDao,
            FinacleClient finacleClient,
            AccountBalanceInquiryService bis,
            EpochProvider epochProvider,
            TransactionLagMonitor<DailyTranDetailTable> lagMonitorService,
            AsyncDataComparator asyncDataComparator,
            AsyncLazyLoader asyncLazyLoader,
            GradualDialUpPropertiesUtils gradualDialUpPropertiesUtils,
            MetricUtil metricUtil,
            ChannelConfig channelConfig) {
        this.transactionDao = transactionDao;
        this.finacleClient = finacleClient;
        this.bis = bis;
        this.epochProvider = epochProvider;
        this.asyncDataComparator = asyncDataComparator;
        this.lagMonitorService = lagMonitorService;
        this.asyncLazyLoader = asyncLazyLoader;
        this.gradualDialUpPropertiesUtils = gradualDialUpPropertiesUtils;
        this.metricUtil = metricUtil;
        this.channelConfig = channelConfig;
        this.counters = CounterMapUtil.transactionCounters(metricUtil, channelConfig.getChannelConfigs());
    }

    /**
     * Retrieves the most recent transactions (last N) for a specified account.
     *
     * This method prioritizes retrieving transaction data from the database for efficiency.
     * However, it includes fallback mechanisms to Finacle for ensuring data availability:
     *
     * 1. **Fire and Forget Mode:** If enabled for the channel, the method fetches data from the database only.
     * 2. **Passthrough Mode:** If not in gradual dialup mode, retrieves data directly from Finacle.
     * 3. **System in Distress:** If the system is experiencing high lag, falls back to Finacle.
     * 4. **Database Retrieval:** Fetches data from the database as the primary source.
     * 5. **Finacle Fallback:**
     *    - If database retrieval fails to return the expected number of transactions, falls back to Finacle.
     *    - If the system is in distress, skips further database checks and falls back to Finacle.
     *
     * The method also performs asynchronous data comparison between retrieved data and Finacle
     * (if enabled) for shadow testing purposes.
     *
     * @param accountId The ID of the account to retrieve transactions for.
     * @param txnCount The desired number of recent transactions.
     * @param channel Name of the Client (e.g., mb1, embark).
     * @return An {@link Optional} containing the {@link TransactionMasterData} if found, or an
     * empty {@link Optional} if no data is available.
     */
    public Optional<TransactionMasterData> getLastNTransactions(String accountId, int txnCount, String channel) {
        channelConfig.validateChannel(channel);
        // gradualDialupToRos is true => gets data from ROS otherwise from CBS
        var gradualDialupToRos = gradualDialUpPropertiesUtils.getGradualDialupStatus(TRANSACTION_INQUIRY, channel);
        var metricName = getMetricName(channel, gradualDialupToRos);
        var adjustedTxnCount = txnCount <= 0 ? DEFAULT_TRANSACTION_COUNT : txnCount;

        try (var timeIt = metricUtil.timeIt(metricName)) {

            var lagThreshold = channelConfig.getLagThreshold(channel);
            var systemLag = lagMonitorService.computeLag(Duration.ofSeconds(lagThreshold));

            if (systemLag.value() != null && systemLag.value().isPresent()) {
                timeIt.emitTimeMetric("transaction_lag", systemLag.value().get().toMillis());
            }
            log.debug("[Transaction]-[{}]-[{}] : Fetching system lag: {}", channel, accountId, systemLag);
            var systemInDistress = !systemLag.withinLimits();
            if(systemInDistress){
                counters
                    .get(
                        channel.toLowerCase()
                            + TransactionChannelCounters.TRANSACTION_SYSTEM_DISTRESS.getValue())
                    .increment();
            }
            var fireAndForget = channelConfig.isFireAndForgetEnabled(channel);
            log.info(
                    "[Transaction]-[{}]-[{}] : fireAndForget - {} gradualDialupToRos - {} systemInDistress - {}",
                    channel,
                    accountId,
                    fireAndForget,
                    gradualDialupToRos,
                    systemInDistress);

            // Fire and Forget Mode
            if (fireAndForget) {
                log.debug(
                        "[Transaction]-[{}]-[{}] : Fire and Forget mode has been enabled - fetching details from ROS",
                        channel,
                        accountId);
                counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_FIRE_AND_FORGET.getValue())
                        .increment();
                return fetchLastNTransactionsFromDb(accountId, adjustedTxnCount, systemLag);
            }

            // pass through mode
            if (!gradualDialupToRos) {
                counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_PASSTHROUGH.getValue())
                        .increment();
                var finacleResponse = handleFallbackToFinacle(accountId, adjustedTxnCount);
                asyncDataComparator.checkTransactionDataMismatchInShadow(finacleResponse, accountId, adjustedTxnCount);
                return finacleResponse;
            }

            // if system distress fallback to Finacle
            else if (systemInDistress) {
                try {
                    counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_FALLBACK_TO_FINACLE.getValue())
                            .increment();
                    return handleFallbackToFinacle(accountId, adjustedTxnCount);
                } catch (CbsClientException e) {
                    counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_FALLBACK_TO_FINACLE_FAILURE.getValue())
                            .increment();
                    log.warn(
                            "[Transaction]-[{}]-[{}] : Fallback to Finacle failed with exception",
                            channel,
                            accountId,
                            e);
                }
            }

            var transactionMasterData = fetchLastNTransactionsFromDb(accountId, adjustedTxnCount, systemLag);

            return transactionMasterData
                    .filter(data -> data.transactionDetails().size() == adjustedTxnCount
                            || systemInDistress) // avoid fallback to finacle
                    .map(data -> {
                        handleDataMismatchComparison(transactionMasterData, accountId, adjustedTxnCount);
                        counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_PRIMARY.getValue())
                                .increment();
                        return transactionMasterData;
                    })
                    .orElseGet(() -> {
                        log.warn(
                                "[Transaction]-[{}]-[{}] : Database query returned empty data "
                                        + "or returned transaction less than the expected count "
                                        + "or Exception, attempting fallback to Finacle",
                                channel,
                                accountId);
                        if (systemInDistress) // avoid fallback to finacle
                             throw new CbsClientException(
                                    "SystemInDistress and Error communicating with Finacle Transaction Inquiry API ");
                        try {
                            counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_DB_QUERY_FALLBACK_TO_FINACLE.getValue())
                                    .increment();
                            return handleFallbackToFinacle(accountId, adjustedTxnCount);
                        } catch (CbsClientException e) {
                            log.warn(
                                    "[Transaction]-[{}]-[{}] : Fallback to Finacle failed with exception",
                                    channel,
                                    accountId,
                                    e);
                            counters.get(channel.toLowerCase() + TransactionChannelCounters.TRANSACTION_DB_QUERY_FALLBACK_TO_FINACLE_FAILURE.getValue())
                                    .increment();
                            return transactionMasterData
                                    .filter(data -> !data.transactionDetails().isEmpty())
                                    .map(data -> {
                                        handleDataMismatchComparison(
                                                transactionMasterData, accountId, adjustedTxnCount);
                                        return transactionMasterData;
                                    })
                                    .orElseThrow(() -> e);
                        }
                    });
        }
    }

    private Optional<TransactionMasterData> handleFallbackToFinacle(String accountId, int txnCount) {
        var finacleResponse = finacleClient.fetchLastNTransactions(accountId, txnCount);
        handleLazyLoading(finacleResponse);
        return finacleResponse;
    }

    private boolean shouldPerformComparison(int percentageRollout) {
        return ThreadLocalRandom.current().nextInt(100) < percentageRollout;
    }

    private String getMetricName(String channel, boolean gradualDialupToRos) {
        return String.format(
                "%s.%s.transaction_latency",
                channel.toLowerCase(), gradualDialupToRos ? "ros_enabled" : "pass_through");
    }

    private void handleLazyLoading(Optional<TransactionMasterData> finacleResponse) {
        if (lazyLoadingEnabled) {
            counters.get(TRANSACTION_LAZY_LOADING).increment();
            finacleResponse.ifPresent(asyncLazyLoader::lazyLoadTransactionData);
        }
    }

    private void handleDataMismatchComparison(
            Optional<TransactionMasterData> transactionDBData, String accountId, int txnCount) {
        if (shouldPerformComparison(percentageRollout)) {
            counters.get(TRANSACTION_PRIMARY_COMPARISON_COUNT).increment();
            transactionDBData.ifPresent(
                    data -> asyncDataComparator.checkTransactionDataMismatch(data, accountId, txnCount));
        }
    }

    private Optional<TransactionMasterData> fetchLastNTransactionsFromDb(String accountId, int txnCount, Lag lag) {
        try {
            return bis.fetchAccountBalancesFromDb(accountId, lag, TRANSACTION).flatMap(accountBalanceMasterData -> {
                var acid = accountBalanceMasterData.acid(); // null acid?
                var transactionMasterData = transactionDao.getLastNTransactions(acid, txnCount);
                log.debug(
                        "[Transaction] : DDB entry for account {} is present in CROS: {}",
                        accountId,
                        transactionMasterData.isPresent());
                return transactionMasterData.flatMap(
                        data -> enrichTransactionMasterData(data, accountBalanceMasterData, lag));
            });
        } catch (DatabaseTransientException | DatabaseException e) {
            log.warn("Faced Exception while hitting Transaction DDB ", e);
            return Optional.empty();
        } catch (AccountBalancesInquiryServiceException e) {
            log.error("Error in Fetching balance form ROS Balance dynamoDb", e);
            return Optional.empty();
        }
    }

    private Optional<TransactionMasterData> enrichTransactionMasterData(
            TransactionMasterData transactionMasterData, AccountBalanceMasterData balanceData, Lag lag) {
        if (transactionMasterData.transactionDetails().isEmpty()) return Optional.empty();

        var latestIngestedTime = lag.value().isPresent()
                ? epochProvider.currentEpoch() - lag.value().get().toMillis()
                : AppConstants.DEFAULT_INGESTION_TIME;

        return Optional.ofNullable(transactionMasterData.toBuilder()
                .accountId(balanceData.accountId())
                .acid(balanceData.accountId())
                .accountBalance(balanceData.signedAvailableBalance())
                .currencyCode(balanceData.accountCurrencyCode())
                .clearBalanceAmount(balanceData.clearBalanceAmount())
                .transactionDetails(transactionMasterData.transactionDetails())
                .source(CROS.name())
                .lastValidAsOf(latestIngestedTime)
                .build());
    }
}