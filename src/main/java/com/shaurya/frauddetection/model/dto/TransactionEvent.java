package com.shaurya.frauddetection.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent {

    @NotBlank(message = "transactionId is required")
    private String transactionId;

    @NotBlank(message = "accountId is required")
    private String accountId;

    @NotBlank(message = "merchantId is required")
    private String merchantId;

    @NotNull(message = "amount is required")
    @DecimalMin(value = "0.01", message = "amount must be greater than zero")
    private BigDecimal amount;

    @NotBlank(message = "currency is required")
    @Size(min = 3, max = 3, message = "currency must be exactly 3 characters")
    private String currency;

    @NotBlank(message = "countryCode is required")
    @Size(min = 2, max = 3, message = "countryCode must be 2-3 characters")
    private String countryCode;

    private String ipAddress;

    private Double latitude;

    private Double longitude;

    @NotNull(message = "timestamp is required")
    private Instant timestamp;
}
