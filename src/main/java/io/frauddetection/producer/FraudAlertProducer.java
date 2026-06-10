package io.frauddetection.producer;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudAlertEvent;
import io.frauddetection.model.entity.FraudAlert;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class FraudAlertProducer {

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final FraudProperties fraudProperties;

    /**
     * Publishes a FraudAlertEvent to the fraud-alerts topic.
     *
     * Key = accountId so that all alerts for the same account are ordered
     * within a single partition. Retried up to 3 times (500 ms backoff) on
     * transient broker failures; after exhaustion the RuntimeException
     * propagates to the consumer's catch block, which routes to the DLQ.
     */
    @Retryable(
            retryFor  = { RuntimeException.class },
            maxAttempts = 3,
            backoff   = @Backoff(delay = 500)
    )
    public void publish(FraudAlert alert) {
        String topic = fraudProperties.getKafka().getTopics().getFraudAlerts();
        FraudAlertEvent event = toEvent(alert);

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, alert.getAccountId(), event);

        // Async callback for success/failure logging
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka send failed transactionId={} topic={}: {}",
                        alert.getTransactionId(), topic, ex.getMessage(), ex);
            } else {
                log.info("Published fraud alert transactionId={} topic={} partition={} offset={}",
                        alert.getTransactionId(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        // Block so @Retryable can observe the exception and the caller receives
        // a synchronous result before marking the transaction as processed.
        try {
            future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            throw new RuntimeException(
                    "Kafka publish failed for transactionId=" + alert.getTransactionId(),
                    ex.getCause());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while publishing transactionId=" + alert.getTransactionId(), ex);
        } catch (TimeoutException ex) {
            throw new RuntimeException(
                    "Timeout publishing transactionId=" + alert.getTransactionId(), ex);
        }
    }

    private FraudAlertEvent toEvent(FraudAlert alert) {
        return FraudAlertEvent.builder()
                .transactionId(alert.getTransactionId())
                .accountId(alert.getAccountId())
                .fraudScore(alert.getFraudScore())
                .fraudReason(alert.getFraudReason())
                .rulesTriggered(alert.getRulesTriggered())
                .alertedAt(alert.getAlertedAt())
                .build();
    }
}
