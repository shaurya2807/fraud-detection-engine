package io.frauddetection.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.frauddetection.model.enums.FraudReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FraudAlertEvent {

    private String transactionId;
    private String accountId;
    private int fraudScore;
    private FraudReason fraudReason;
    private List<String> rulesTriggered;
    private Instant alertedAt;
}
