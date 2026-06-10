package io.frauddetection.model.dto;

import io.frauddetection.model.enums.FraudReason;
import io.frauddetection.model.enums.FraudStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudDecisionResponse {

    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private FraudStatus fraudStatus;
    private FraudReason fraudReason;
    private int fraudScore;
    private List<String> rulesTriggered;
    private Instant processedAt;
}
