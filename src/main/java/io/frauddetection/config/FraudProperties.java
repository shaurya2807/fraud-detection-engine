package io.frauddetection.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "fraud")
public class FraudProperties {

    @Valid
    private Rules rules = new Rules();

    @Valid
    private Scoring scoring = new Scoring();

    @Valid
    private Kafka kafka = new Kafka();

    @Getter
    @Setter
    public static class Rules {
        private HighVelocity highVelocity = new HighVelocity();
        private LargeAmount largeAmount = new LargeAmount();
        private HourlyLimit hourlyLimit = new HourlyLimit();

        @Getter
        @Setter
        public static class HighVelocity {
            @Positive
            private int windowSeconds = 60;
            @Positive
            private int maxTransactions = 5;
            @Positive
            private int score = 40;
        }

        @Getter
        @Setter
        public static class LargeAmount {
            @NotNull
            private BigDecimal threshold = new BigDecimal("5000.00");
            @Positive
            private int score = 50;
        }

        @Getter
        @Setter
        public static class HourlyLimit {
            @Positive
            private int windowSeconds = 3600;
            @NotNull
            private BigDecimal maxAmount = new BigDecimal("10000.00");
            @Positive
            private int score = 60;
        }
    }

    @Getter
    @Setter
    public static class Scoring {
        @Positive
        private int flaggedThreshold = 50;
        @Positive
        private int blockedThreshold = 80;
    }

    @Getter
    @Setter
    public static class Kafka {
        private Topics topics = new Topics();

        @Getter
        @Setter
        public static class Topics {
            private String transactions = "transactions";
            private String fraudAlerts = "fraud-alerts";
            private String transactionsDlq = "transactions-dlq";
        }
    }
}
