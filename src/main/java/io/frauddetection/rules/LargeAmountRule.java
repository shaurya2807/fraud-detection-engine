package io.frauddetection.rules;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudReason;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class LargeAmountRule implements FraudRule {

    private final FraudProperties fraudProperties;

    @Override
    public void evaluate(TransactionEvent event, FraudEvaluationResult result) {
        if (event.getAmount().compareTo(fraudProperties.getRules().getLargeAmount().getThreshold()) > 0) {
            result.addTriggeredRule(ruleName());
            result.incrementScore(scoreWeight());
        }
    }

    @Override
    public String ruleName() {
        return FraudReason.LARGE_AMOUNT.name();
    }

    @Override
    public int scoreWeight() {
        return fraudProperties.getRules().getLargeAmount().getScore();
    }
}
