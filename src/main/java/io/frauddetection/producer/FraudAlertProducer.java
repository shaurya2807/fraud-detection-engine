package io.frauddetection.producer;

import io.frauddetection.model.entity.FraudAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Stub — full Kafka publish logic is added in Feature 6.
 */
@Slf4j
@Component
public class FraudAlertProducer {

    public void publish(FraudAlert alert) {
        log.info("[STUB] FraudAlertProducer: alert queued for transactionId={} accountId={}",
                alert.getTransactionId(), alert.getAccountId());
    }
}
