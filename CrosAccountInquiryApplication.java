package com.kmbl.cros.accountinquiryservice;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.BALANCE_CONSUMER_JOB_ENABLED;
import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.BALANCE_INQUIRY_SERVICE_ENABLED;
import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.CRNTOACC_INQUIRY_SERVICE_ENABLED;
import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.TDSUMMARY_INQUIRY_SERVICE_ENABLED;
import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.TRANSACTION_CONSUMER_JOB_ENABLED;
import static com.kmbl.cros.accountinquiryservice.constants.ServiceEnableConstants.TRANSACTION_INQUIRY_SERVICE_ENABLED;

/**
 * Spring Boot Application.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.kmbl.cros.accountinquiryservice"})
@EnableFeignClients
@EnableAsync
@Slf4j
public class CrosAccountInquiryApplication {

    /**
     * Spring Boot Application main function.
     */
    public static void main(String[] args) {
        String deploymentType = args.length > 0 ? args[0] : System.getenv("ROS_DEPLOYMENT_TYPE");

        if (deploymentType == null || deploymentType.isBlank()) {
            enableServiceOnly();
        } else {
            switch (deploymentType) {
                case "all":
                    enableAll();
                    break;
                case "balance-inquiry":
                    enableBalanceInquiryOnly();
                    break;
                case "transaction-inquiry":
                    enableTransactionInquiryOnly();
                case "balance-consumer":
                    enableBalanceConsumerOnly();
                    break;
                case "transaction-consumer":
                    enableTransactionConsumerOnly();
                    break;
                case "recon-job":
                    //          enableReconJobOnly();
                    enableDistributedGAMBalanceReconciliationOnly();
                    break;
                case "crn-to-acc-inquiry":
                    enableCrnToAcOnly();
                    break;
                case "ros-dtd-recon-distributed-job":
                    enableDistributedDTDTransactionReconciliationOnly();
                    break;
                case "ros-gam-recon-distributed-job":
                    enableDistributedGAMBalanceReconciliationOnly();
                    break;
                case "cros-cmg-recon-job":
                    enableDistributedCMGBalanceReconciliationOnly();
                    break;
                case "cros-aas-recon-job":
                    enableDistributedAASBalanceReconciliationOnly();
                    break;
                case "cros-common-recon-job":
                    enableDistributedCommonReconReconciliation();
                    break;
                case "td-summary-inquiry":
                    enableTdSummaryOnly();
                    break;
                case "service":
                default:
                    enableServiceOnly();
                    break;
            }
        }
        SpringApplication.run(CrosAccountInquiryApplication.class, args);
    }

    private static void enableDistributedGAMBalanceReconciliationOnly() {
        enableService(false);
        enableROSService(false);
        enableRedis(false);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableDistributedGAMBalanceReconciliation(true);
    }

    private static void enableDistributedCMGBalanceReconciliationOnly() {
        enableROSService(false);
        enableRedis(false);
        enableBalanceInquiry(false);
        enableTransactionInquiry(false);
        enableFeatureFlagInquiry(false);
        enableCrnToAccountInquiry(false);
        enableDistributedCMGReconciliation(true);
    }

    private static void enableDistributedAASBalanceReconciliationOnly() {
        enableROSService(false);
        enableRedis(false);
        enableBalanceInquiry(false);
        enableTransactionInquiry(false);
        enableFeatureFlagInquiry(false);
        enableCrnToAccountInquiry(false);
        enableDistributedAASReconciliation(true);
    }

    private static void enableDistributedDTDTransactionReconciliationOnly() {
        log.info("Enabling DTD recon");
        enableService(false);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableROSService(false);
        enableRedis(false);
        enableDistributedDTDTransactionReconciliation(true);
    }

    private static void enableDistributedCommonReconReconciliation() {
        enableROSService(false);
        enableRedis(false);
        enableBalanceInquiry(false);
        enableTransactionInquiry(false);
        enableFeatureFlagInquiry(false);
        enableCrnToAccountInquiry(false);
        System.setProperty("app.ros-distributed-recon-enabled", Boolean.toString(true));
        enableDistributedSMTReconciliation(true);
        enableDistributedCAMReconciliation(true);
        enableDistributedRCTReconciliation(true);
        enableDistributedSOLReconciliation(true);
    }

    /**
     * Enable inquiry API only.
     */
    public static void enableServiceOnly() {
        enableService(true);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(true);
        enableTransactionInquiry(true);
        enableFeatureFlagInquiry(true);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable Balance inquiry API only.
     */
    public static void enableBalanceInquiryOnly() {
        enableService(false);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(true);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable Transaction inquiry API only.
     */
    public static void enableTransactionInquiryOnly() {
        enableService(false);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(true);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable balance consumer only.
     */
    public static void enableBalanceConsumerOnly() {
        enableService(false);
        enableROSService(false);
        enableRedis(true);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(true);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable transaction consumer only.
     */
    public static void enableTransactionConsumerOnly() {
        enableService(false);
        enableROSService(false);
        enableRedis(true);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(false);
        enableTransactionInquiry(false);
        enableTransactionConsumer(true);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable both consumer and inquiry APIs.
     */
    public static void enableAll() {
        enableService(true);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(true);
        enableFeatureFlagInquiry(true);
        enableTransactionInquiry(true);
        enableTransactionConsumer(true);
        enableBalanceConsumer(true);
        enableCrnToAccountInquiry(true);
        enableTdSummaryInquiry(true);
    }

    /**
     * Enable CrnToAc only
     */
    private static void enableCrnToAcOnly() {
        enableService(false);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(true);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(true);
        enableTdSummaryInquiry(false);
    }

    /**
     * Enable TdSummary only
     */
    private static void enableTdSummaryOnly() {
        enableService(false);
        enableROSService(true);
        enableRedis(true);
        enableBalanceInquiry(false);
        enableFeatureFlagInquiry(true);
        enableTransactionInquiry(false);
        enableTransactionConsumer(false);
        enableBalanceConsumer(false);
        enableCrnToAccountInquiry(false);
        enableTdSummaryInquiry(true);
    }

    private static void enableService(boolean enable) {
        System.setProperty("app.server-enabled", Boolean.toString(enable));
    }

    private static void enableBalanceInquiry(boolean enable) {
        System.setProperty(BALANCE_INQUIRY_SERVICE_ENABLED, Boolean.toString(enable));
    }

    private static void enableTransactionInquiry(boolean enable) {
        System.setProperty(TRANSACTION_INQUIRY_SERVICE_ENABLED, Boolean.toString(enable));
    }

    private static void enableTransactionConsumer(boolean enable) {
        System.setProperty(TRANSACTION_CONSUMER_JOB_ENABLED, Boolean.toString(enable));
    }

    private static void enableBalanceConsumer(boolean enable) {
        System.setProperty(BALANCE_CONSUMER_JOB_ENABLED, Boolean.toString(enable));
    }

    private static void enableFeatureFlagInquiry(boolean enable) {
        System.setProperty("app.ros-feature-flag-inquiry-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedDTDTransactionReconciliation(boolean enable) {
        System.setProperty("app.ros-distributed-recon-enabled", Boolean.toString(enable));
        System.setProperty("app.ros-dtd-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedGAMBalanceReconciliation(boolean enable) {
        System.setProperty("app.ros-distributed-recon-enabled", Boolean.toString(enable));
        System.setProperty("app.ros-gam-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableCrnToAccountInquiry(boolean enable) {
        System.setProperty(CRNTOACC_INQUIRY_SERVICE_ENABLED, Boolean.toString(enable));
    }

    private static void enableTdSummaryInquiry(boolean enable) {
        System.setProperty(TDSUMMARY_INQUIRY_SERVICE_ENABLED, Boolean.toString(enable));
    }

    private static void enableDistributedSMTReconciliation(boolean enable) {
        System.setProperty("app.ros-smt-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedCAMReconciliation(boolean enable) {
        System.setProperty("app.ros-cam-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedRCTReconciliation(boolean enable) {
        System.setProperty("app.ros-rct-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedSOLReconciliation(boolean enable) {
        System.setProperty("app.ros-sol-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedAASReconciliation(boolean enable) {
        System.setProperty("app.ros-distributed-recon-enabled", Boolean.toString(enable));
        System.setProperty("app.ros-aas-recon-distributed-enabled", Boolean.toString(enable));
    }

    private static void enableDistributedCMGReconciliation(boolean enable) {
        System.setProperty("app.ros-distributed-recon-enabled", Boolean.toString(enable));
        System.setProperty("app.ros-cmg-recon-distributed-enabled", Boolean.toString(enable));
    }

    @Bean
    public static OpenTelemetry openTelemetry() {
        return GlobalOpenTelemetry.get();
    }

    private static void enableRedis(boolean enable) {
        System.setProperty("app.ros-redis-enabled", Boolean.toString(enable));
    }

    private static void enableROSService(boolean enable) {
        System.setProperty("app.ros-service-enabled", Boolean.toString(enable));
    }
}
