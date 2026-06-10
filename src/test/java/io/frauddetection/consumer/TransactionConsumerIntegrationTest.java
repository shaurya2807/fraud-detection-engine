package io.frauddetection.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudStatus;
import io.frauddetection.repository.FraudAlertRepository;
import io.frauddetection.repository.TransactionRepository;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                // Streams not under test; disable to avoid EOS transaction log issues
                // with the single-broker test cluster.
                "spring.kafka.streams.auto-startup=false"
        }
)
@Testcontainers
class TransactionConsumerIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("frauddb")
                    .withUsername("frauduser")
                    .withPassword("fraudpass");

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",          KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.bootstrap-servers",  KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url",                   POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",              POSTGRES::getUsername);
        registry.add("spring.datasource.password",              POSTGRES::getPassword);
        registry.add("spring.data.redis.host",                  REDIS::getHost);
        registry.add("spring.data.redis.port",   () -> REDIS.getMappedPort(6379));
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired TransactionRepository          transactionRepository;
    @Autowired FraudAlertRepository           fraudAlertRepository;
    @Autowired ObjectMapper                   objectMapper;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String DLQ_TOPIC          = "transactions-dlq";

    @BeforeEach
    void cleanDb() {
        fraudAlertRepository.deleteAll();
        transactionRepository.deleteAll();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private TransactionEvent approvedEvent(String txnId, String accountId) {
        return TransactionEvent.builder()
                .transactionId(txnId)
                .accountId(accountId)
                .merchantId("MERCH-TEST")
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .countryCode("US")
                .timestamp(Instant.now())
                .build();
    }

    private TransactionEvent fraudulentEvent(String txnId, String accountId) {
        return TransactionEvent.builder()
                .transactionId(txnId)
                .accountId(accountId)
                .merchantId("MERCH-TEST")
                .amount(new BigDecimal("7500.00"))   // > $5 000 threshold → FLAGGED
                .currency("USD")
                .countryCode("US")
                .timestamp(Instant.now())
                .build();
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void validTransaction_approved_persistedToDb() {
        String txnId = "TXN-APPROVED-" + UUID.randomUUID();

        kafkaTemplate.send(TRANSACTIONS_TOPIC, txnId, approvedEvent(txnId, "ACC-APPROVED-001"));

        await().atMost(10, SECONDS)
                .pollInterval(500, org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS)
                .until(() -> transactionRepository.findByTransactionId(txnId).isPresent());

        var saved = transactionRepository.findByTransactionId(txnId).orElseThrow();
        assertThat(saved.getFraudStatus()).isEqualTo(FraudStatus.APPROVED);
        assertThat(saved.getFraudScore()).isZero();
    }

    @Test
    void fraudulentTransaction_flagged_alertCreated() {
        String txnId = "TXN-FRAUD-" + UUID.randomUUID();

        kafkaTemplate.send(TRANSACTIONS_TOPIC, txnId, fraudulentEvent(txnId, "ACC-FRAUD-001"));

        await().atMost(10, SECONDS)
                .until(() -> transactionRepository.findByTransactionId(txnId)
                        .filter(t -> t.getFraudStatus() == FraudStatus.FLAGGED)
                        .isPresent());

        var savedTxn = transactionRepository.findByTransactionId(txnId).orElseThrow();
        assertThat(savedTxn.getFraudStatus()).isEqualTo(FraudStatus.FLAGGED);
        assertThat(savedTxn.getFraudScore()).isGreaterThanOrEqualTo(50);

        await().atMost(5, SECONDS)
                .until(() -> fraudAlertRepository.findByTransactionId(txnId).isPresent());
        assertThat(fraudAlertRepository.findByTransactionId(txnId)).isPresent();
    }

    @Test
    void duplicateTransaction_idempotencyGuard() {
        String txnId    = "TXN-DUP-" + UUID.randomUUID();
        String accountId = "ACC-DUP-001";
        TransactionEvent event = approvedEvent(txnId, accountId);

        // Publish the same transaction twice
        kafkaTemplate.send(TRANSACTIONS_TOPIC, txnId, event);
        kafkaTemplate.send(TRANSACTIONS_TOPIC, txnId, event);

        // Wait long enough for both messages to be consumed
        await().atMost(10, SECONDS)
                .until(() -> transactionRepository.findByTransactionId(txnId).isPresent());

        // Extra buffer: let the second message finish processing (either skipped or attempted)
        await().during(2, SECONDS).atMost(4, SECONDS)
                .until(() -> true);

        long count = transactionRepository.findByAccountId(accountId,
                org.springframework.data.domain.Pageable.unpaged()).getTotalElements();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void invalidPayload_routedToDlq() throws Exception {
        // Publish a raw malformed JSON payload directly (bypasses JsonSerializer)
        byte[] malformedBytes = "{NOT_VALID_JSON{{".getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(Map.of(
                "bootstrap.servers",  KAFKA.getBootstrapServers(),
                "key.serializer",     StringSerializer.class.getName(),
                "value.serializer",   ByteArraySerializer.class.getName()))) {

            rawProducer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC, "dlq-test-key", malformedBytes))
                    .get(5, SECONDS);
        }

        // The ErrorHandlingDeserializer sets value=null; the consumer throws;
        // DefaultErrorHandler + DeadLetterPublishingRecoverer routes to DLQ.
        try (KafkaConsumer<String, String> dlqConsumer = new KafkaConsumer<>(Map.of(
                "bootstrap.servers",  KAFKA.getBootstrapServers(),
                "group.id",           "dlq-verifier-" + UUID.randomUUID(),
                "key.deserializer",   StringDeserializer.class.getName(),
                "value.deserializer", StringDeserializer.class.getName(),
                "auto.offset.reset",  "earliest"))) {

            dlqConsumer.subscribe(List.of(DLQ_TOPIC));

            await().atMost(10, SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        ConsumerRecords<String, String> records =
                                dlqConsumer.poll(Duration.ofMillis(300));
                        return !records.isEmpty();
                    });
        }
    }
}
