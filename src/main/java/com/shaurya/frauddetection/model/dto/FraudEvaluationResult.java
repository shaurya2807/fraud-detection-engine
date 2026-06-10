package com.shaurya.frauddetection.model.dto;

import com.shaurya.frauddetection.model.enums.FraudReason;
import com.shaurya.frauddetection.model.enums.FraudStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public class FraudEvaluationResult {

    private final String transactionId;
    private final String accountId;
    private final FraudStatus fraudStatus;
    private final FraudReason fraudReason;
    private final int score;

    @Builder.Default
    private final List<String> triggeredRules = new ArrayList<>();

    public boolean isFraudulent() {
        return fraudStatus != FraudStatus.APPROVED;
    }

    public List<String> getTriggeredRules() {
        return Collections.unmodifiableList(triggeredRules);
    }
}
