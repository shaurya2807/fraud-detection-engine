package com.shaurya.frauddetection.model.dto;

import com.shaurya.frauddetection.model.enums.FraudReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudAlertResponse {

    private UUID id;
    private String transactionId;
    private String accountId;
    private int fraudScore;
    private FraudReason fraudReason;
    private List<String> rulesTriggered;
    private Instant alertedAt;
    private boolean acknowledged;
    private Instant acknowledgedAt;
    private String acknowledgedBy;
}
