package io.frauddetection.model.dto;

import io.frauddetection.model.enums.FraudReason;
import io.frauddetection.model.enums.FraudStatus;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Mutable accumulator built by RuleEngine. Rules append to triggeredRules and
 * increment score; the engine sets fraudStatus/fraudReason after all rules run.
 */
@Getter
@Setter
@Builder
public class FraudEvaluationResult {

    private String transactionId;
    private String accountId;
    private FraudStatus fraudStatus;
    private FraudReason fraudReason;
    private int score;

    @Builder.Default
    private List<String> triggeredRules = new ArrayList<>();

    public void addTriggeredRule(String ruleName) {
        triggeredRules.add(ruleName);
    }

    public void incrementScore(int points) {
        this.score += points;
    }

    public boolean isFraudulent() {
        return fraudStatus != FraudStatus.APPROVED;
    }
}
