package com.shaurya.frauddetection.model.entity;

import com.shaurya.frauddetection.model.enums.FraudReason;
import com.shaurya.frauddetection.model.enums.FraudStatus;
import io.hypersistence.utils.hibernate.type.array.ListArrayType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Entity
@Table(
    name = "transactions",
    indexes = {
        @Index(name = "idx_transactions_account_id",      columnList = "account_id"),
        @Index(name = "idx_transactions_created_at",      columnList = "created_at DESC"),
        @Index(name = "idx_transactions_fraud_status",    columnList = "fraud_status"),
        @Index(name = "idx_transactions_account_created", columnList = "account_id, created_at DESC")
    }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = "id")
@ToString(exclude = "rulesTriggered")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true, length = 64)
    private String transactionId;

    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "merchant_id", nullable = false, length = 64)
    private String merchantId;

    @Column(name = "amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    @Builder.Default
    private String currency = "USD";

    @Column(name = "country_code", nullable = false, length = 3)
    private String countryCode;

    @Column(name = "ip_address", length = 45)
    private String ipAddress;

    @Column(name = "latitude")
    private Double latitude;

    @Column(name = "longitude")
    private Double longitude;

    @Enumerated(EnumType.STRING)
    @Column(name = "fraud_status", nullable = false, length = 20)
    @Builder.Default
    private FraudStatus fraudStatus = FraudStatus.APPROVED;

    @Column(name = "fraud_score", nullable = false)
    @Builder.Default
    private int fraudScore = 0;

    @Enumerated(EnumType.STRING)
    @Column(name = "fraud_reason", nullable = false, length = 50)
    @Builder.Default
    private FraudReason fraudReason = FraudReason.NONE;

    @Type(ListArrayType.class)
    @Column(name = "rules_triggered", columnDefinition = "text[]")
    private List<String> rulesTriggered;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;
}
