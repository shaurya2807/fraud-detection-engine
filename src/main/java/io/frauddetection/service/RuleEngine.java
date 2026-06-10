package io.frauddetection.service;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudReason;
import io.frauddetection.model.enums.FraudStatus;
import io.frauddetection.rules.FraudRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RuleEngine {

    private final List<FraudRule> rules;
    private final FraudProperties fraudProperties;

    public FraudEvaluationResult evaluate(TransactionEvent event) {
        FraudEvaluationResult result = FraudEvaluationResult.builder()
                .transactionId(event.getTransactionId())
                .accountId(event.getAccountId())
                .fraudStatus(FraudStatus.APPROVED)
                .fraudReason(FraudReason.NONE)
                .score(0)
                .build();

        for (FraudRule rule : rules) {
            try {
                rule.evaluate(event, result);
            } catch (Exception ex) {
                // A failed rule is logged but does not abort the pipeline.
                // The transaction proceeds without that rule's score, which is
                // the safer choice when Redis is temporarily unavailable.
                log.error("Rule [{}] threw an exception for transaction [{}] — skipping rule: {}",
                        rule.ruleName(), event.getTransactionId(), ex.getMessage(), ex);
            }
        }

        result.setFraudStatus(determineStatus(result.getScore()));
        result.setFraudReason(determineReason(result.getTriggeredRules()));

        log.debug("Evaluated transaction={} account={}: score={} status={} reason={} rules={}",
                event.getTransactionId(), event.getAccountId(),
                result.getScore(), result.getFraudStatus(), result.getFraudReason(),
                result.getTriggeredRules());

        return result;
    }

    private FraudStatus determineStatus(int score) {
        FraudProperties.Scoring scoring = fraudProperties.getScoring();
        if (score >= scoring.getBlockedThreshold()) {
            return FraudStatus.BLOCKED;
        }
        if (score >= scoring.getFlaggedThreshold()) {
            return FraudStatus.FLAGGED;
        }
        return FraudStatus.APPROVED;
    }

    private FraudReason determineReason(List<String> triggeredRules) {
        if (triggeredRules.isEmpty()) {
            return FraudReason.NONE;
        }
        if (triggeredRules.size() == 1) {
            try {
                return FraudReason.valueOf(triggeredRules.get(0));
            } catch (IllegalArgumentException ex) {
                log.warn("Rule name [{}] has no matching FraudReason — defaulting to MULTIPLE_RULES_TRIGGERED",
                        triggeredRules.get(0));
            }
        }
        return FraudReason.MULTIPLE_RULES_TRIGGERED;
    }
}
