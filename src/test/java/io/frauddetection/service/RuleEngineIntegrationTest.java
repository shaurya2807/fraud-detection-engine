package io.frauddetection.service;

import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudReason;
import io.frauddetection.model.enums.FraudStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the rule engine with a real Redis container.
 * PostgreSQL is also started to satisfy JPA/Flyway context requirements;
 * Kafka consumers and Streams are disabled — only Redis-backed rules are exercised.
 *
 * The FLAGGED threshold is overridden to 40 so that HighVelocityRule (score=40)
 * alone is sufficient to flag a transaction, making the velocity-only test
 * unambiguous.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.consumer.auto-startup=false",
                "spring.kafka.streams.auto-startup=false",
                "spring.kafka.bootstrap-servers=localhost:9092",   // not connected — consumers disabled
                "fraud.scoring.flagged-threshold=40"               // lower threshold to isolate velocity
        }
)
@Testcontainers
class RuleEngineIntegrationTest {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("frauddb")
                    .withUsername("frauduser")
                    .withPassword("fraudpass");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired RuleEngine          ruleEngine;
    @Autowired StringRedisTemplate redisTemplate;

    @BeforeEach
    void flushRedis() {
        // Each test uses a unique accountId so inter-test state pollution is impossible,
        // but a full flush keeps the Redis container clean between runs.
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private TransactionEvent event(String txnId, String accountId, BigDecimal amount) {
        return TransactionEvent.builder()
                .transactionId(txnId)
                .accountId(accountId)
                .merchantId("MERCH-TEST")
                .amount(amount)
                .currency("USD")
                .countryCode("US")
                .timestamp(Instant.now())
                .build();
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void approvedTransaction_noRulesTriggered() {
        FraudEvaluationResult result = ruleEngine.evaluate(
                event("TXN-OK-001", "ACC-OK-001", new BigDecimal("100.00")));

        assertThat(result.getFraudStatus()).isEqualTo(FraudStatus.APPROVED);
        assertThat(result.getScore()).isZero();
        assertThat(result.getTriggeredRules()).isEmpty();
        assertThat(result.getFraudReason()).isEqualTo(FraudReason.NONE);
    }

    @Test
    void largeAmount_flagged() {
        FraudEvaluationResult result = ruleEngine.evaluate(
                event("TXN-LARGE-001", "ACC-LARGE-001", new BigDecimal("6000.00")));

        assertThat(result.getFraudStatus()).isEqualTo(FraudStatus.FLAGGED);
        assertThat(result.getTriggeredRules()).contains(FraudReason.LARGE_AMOUNT.name());
        assertThat(result.getFraudReason()).isEqualTo(FraudReason.LARGE_AMOUNT);
        assertThat(result.getScore()).isGreaterThanOrEqualTo(50);
    }

    @Test
    void highVelocity_flagged() {
        String accountId = "ACC-VEL-001";
        BigDecimal smallAmount = new BigDecimal("50.00"); // below every threshold

        FraudEvaluationResult result = null;
        for (int i = 1; i <= 6; i++) {
            result = ruleEngine.evaluate(event("TXN-VEL-00" + i, accountId, smallAmount));
        }

        // 6 transactions > maxTransactions(5) → HighVelocityRule fires (score=40)
        // With the test-scoped flagged-threshold=40, score 40 → FLAGGED
        assertThat(result.getTriggeredRules()).contains(FraudReason.HIGH_VELOCITY.name());
        assertThat(result.getFraudStatus()).isEqualTo(FraudStatus.FLAGGED);
        assertThat(result.getScore()).isGreaterThanOrEqualTo(40);
    }

    @Test
    void hourlyLimitExceeded_flagged() {
        String accountId = "ACC-HOURLY-001";

        // First transaction: $6 000 → hourly total = $6 000 (below $10 000 limit)
        FraudEvaluationResult first = ruleEngine.evaluate(
                event("TXN-HOURLY-001", accountId, new BigDecimal("6000.00")));
        assertThat(first.getTriggeredRules()).doesNotContain(FraudReason.AMOUNT_EXCEEDS_HOURLY_LIMIT.name());

        // Second transaction: adds $5 000 → total = $11 000 > $10 000 limit
        FraudEvaluationResult second = ruleEngine.evaluate(
                event("TXN-HOURLY-002", accountId, new BigDecimal("5000.00")));

        assertThat(second.getTriggeredRules()).contains(FraudReason.AMOUNT_EXCEEDS_HOURLY_LIMIT.name());
        assertThat(second.getFraudStatus()).isEqualTo(FraudStatus.FLAGGED);
        assertThat(second.getScore()).isGreaterThanOrEqualTo(60);
    }

    @Test
    void multipleRules_blocked() {
        String accountId = "ACC-BLOCKED-001";
        BigDecimal largeAmount = new BigDecimal("6000.00"); // triggers LargeAmountRule

        FraudEvaluationResult result = null;
        for (int i = 1; i <= 6; i++) {
            result = ruleEngine.evaluate(event("TXN-BLK-00" + i, accountId, largeAmount));
        }

        // After 6 transactions each of $6 000:
        //   LargeAmountRule     → score +50  (amount > $5 000)
        //   HighVelocityRule    → score +40  (count=6 > maxTransactions=5)
        //   HourlyAmountLimit   → score +60  (total=$36 000 > $10 000, triggered earlier)
        //   Combined score ≥ 80 → BLOCKED
        assertThat(result.getScore()).isGreaterThanOrEqualTo(80);
        assertThat(result.getFraudStatus()).isEqualTo(FraudStatus.BLOCKED);
        assertThat(result.getFraudReason()).isEqualTo(FraudReason.MULTIPLE_RULES_TRIGGERED);
        assertThat(result.getTriggeredRules()).hasSizeGreaterThanOrEqualTo(2);
    }
}
