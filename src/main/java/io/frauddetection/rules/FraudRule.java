package io.frauddetection.rules;

import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;

public interface FraudRule {

    void evaluate(TransactionEvent event, FraudEvaluationResult result);

    String ruleName();

    int scoreWeight();
}
