package io.frauddetection.consumer;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.entity.FraudAlert;
import io.frauddetection.model.entity.Transaction;
import io.frauddetection.producer.FraudAlertProducer;
import io.frauddetection.repository.FraudAlertRepository;
import io.frauddetection.repository.TransactionRepository;
import io.frauddetection.service.FraudMetricsService;
import io.frauddetection.service.RuleEngine;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionConsumer {

    private static final String PROCESSED_KEY_PREFIX = "processed:";
    private static final long   PROCESSED_TTL_HOURS    = 24;
    private static final long   DLQ_TIMEOUT_SECONDS    = 10;

    private final RuleEngine               ruleEngine;
    private final TransactionRepository    transactionRepository;
    private final FraudAlertRepository     fraudAlertRepository;
    private final FraudAlertProducer       fraudAlertProducer;
    private final StringRedisTemplate      redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final FraudProperties          fraudProperties;
    private final Validator                validator;
    private final FraudMetricsService      fraudMetricsService;

    @KafkaListener(
            topics              = "${fraud.kafka.topics.transactions}",
            groupId             = "${spring.kafka.consumer.group-id}",
            containerFactory    = "kafkaListenerContainerFactory",
            concurrency         = "3"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
        TransactionEvent event = record.value();

        // Null value indicates ErrorHandlingDeserializer caught a deserialization failure.
        // The DLQ recoverer in the container factory already routed this to the DLQ topic,
        // so we just ack here to prevent stalling the partition.
        if (event == null) {
            // Throwing activates DefaultErrorHandler + DeadLetterPublishingRecoverer so the
            // original record bytes are forwarded to the DLQ and the offset committed.
            throw new IllegalStateException(String.format(
                    "Deserialization produced null value at topic=%s partition=%d offset=%d",
                    record.topic(), record.partition(), record.offset()));
        }

        String transactionId = event.getTransactionId();
        String processedKey  = PROCESSED_KEY_PREFIX + transactionId;

        try {
            Timer.Sample timerSample = fraudMetricsService.startTimer();

            // ── Idempotency ────────────────────────────────────────────────
            if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
                log.debug("Duplicate transaction={}, skipping", transactionId);
                ack.acknowledge();
                return;
            }

            // ── Validation ─────────────────────────────────────────────────
            Set<ConstraintViolation<TransactionEvent>> violations = validator.validate(event);
            if (!violations.isEmpty()) {
                String detail = violations.stream()
                        .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                        .collect(Collectors.joining("; "));
                throw new IllegalArgumentException("Validation failed — " + detail);
            }

            // ── Rule evaluation ────────────────────────────────────────────
            FraudEvaluationResult result = ruleEngine.evaluate(event);
            fraudMetricsService.recordOutcome(result.getFraudStatus());
            result.getTriggeredRules().forEach(fraudMetricsService::recordRuleTriggered);

            // ── Persistence ────────────────────────────────────────────────
            transactionRepository.save(buildTransaction(event, result));

            if (result.isFraudulent()) {
                FraudAlert alert = buildFraudAlert(event, result);
                fraudAlertRepository.save(alert);
                fraudAlertProducer.publish(alert);
            }

            // ── Mark processed ─────────────────────────────────────────────
            // Set only after all work succeeds; a crash before this line
            // causes safe re-delivery (transaction_id unique constraint will
            // surface on retry, routing the duplicate to the DLQ).
            redisTemplate.opsForValue().set(processedKey, "1", PROCESSED_TTL_HOURS, TimeUnit.HOURS);
            fraudMetricsService.stopTimer(timerSample);
            ack.acknowledge();

            log.info("OK transaction={} account={} status={} score={} rules={}",
                    transactionId, event.getAccountId(),
                    result.getFraudStatus(), result.getScore(), result.getTriggeredRules());

        } catch (Exception ex) {
            log.error("FAILED transaction={} account={} partition={} offset={}: {}",
                    transactionId, event.getAccountId(),
                    record.partition(), record.offset(), ex.getMessage(), ex);
            sendToDlq(record, event, ack);
        }
    }

    // ── DLQ routing ──────────────────────────────────────────────────────────

    private void sendToDlq(ConsumerRecord<String, TransactionEvent> record,
                           TransactionEvent event,
                           Acknowledgment ack) {
        String dlqTopic = fraudProperties.getKafka().getTopics().getTransactionsDlq();
        try {
            kafkaTemplate.send(dlqTopic, record.key(), event)
                    .get(DLQ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.warn("DLQ routed transaction={} to topic={}", event.getTransactionId(), dlqTopic);
            ack.acknowledge();
        } catch (Exception dlqEx) {
            // DLQ broker is also unavailable — do not ack. The container's DefaultErrorHandler
            // will handle the next redelivery attempt. The message is never silently lost.
            log.error("DLQ send FAILED for transaction={}, NOT acking: {}",
                    event.getTransactionId(), dlqEx.getMessage(), dlqEx);
        }
    }

    // ── Mapping helpers ──────────────────────────────────────────────────────

    private Transaction buildTransaction(TransactionEvent event, FraudEvaluationResult result) {
        return Transaction.builder()
                .transactionId(event.getTransactionId())
                .accountId(event.getAccountId())
                .merchantId(event.getMerchantId())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .countryCode(event.getCountryCode())
                .ipAddress(event.getIpAddress())
                .latitude(event.getLatitude())
                .longitude(event.getLongitude())
                .fraudStatus(result.getFraudStatus())
                .fraudScore(result.getScore())
                .fraudReason(result.getFraudReason())
                .rulesTriggered(result.getTriggeredRules())
                .processedAt(Instant.now())
                .build();
    }

    private FraudAlert buildFraudAlert(TransactionEvent event, FraudEvaluationResult result) {
        return FraudAlert.builder()
                .transactionId(event.getTransactionId())
                .accountId(event.getAccountId())
                .fraudScore(result.getScore())
                .fraudReason(result.getFraudReason())
                .rulesTriggered(result.getTriggeredRules())
                .build();
    }
}
