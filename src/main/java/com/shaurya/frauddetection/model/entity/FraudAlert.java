package com.shaurya.frauddetection.model.entity;

import com.shaurya.frauddetection.model.enums.FraudReason;
import io.hypersistence.utils.hibernate.type.array.ListArrayType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Entity
@Table(
    name = "fraud_alerts",
    indexes = {
        @Index(name = "idx_fraud_alerts_account_id",    columnList = "account_id"),
        @Index(name = "idx_fraud_alerts_alerted_at",    columnList = "alerted_at DESC"),
        @Index(name = "idx_fraud_alerts_acknowledged",  columnList = "acknowledged")
    }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = "id")
@ToString(exclude = "rulesTriggered")
public class FraudAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, length = 64)
    private String transactionId;

    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "fraud_score", nullable = false)
    private int fraudScore;

    @Enumerated(EnumType.STRING)
    @Column(name = "fraud_reason", nullable = false, length = 50)
    private FraudReason fraudReason;

    @Type(ListArrayType.class)
    @Column(name = "rules_triggered", columnDefinition = "text[]")
    private List<String> rulesTriggered;

    @CreationTimestamp
    @Column(name = "alerted_at", nullable = false, updatable = false)
    private Instant alertedAt;

    @Column(name = "acknowledged", nullable = false)
    @Builder.Default
    private boolean acknowledged = false;

    @Column(name = "acknowledged_at")
    private Instant acknowledgedAt;

    @Column(name = "acknowledged_by", length = 128)
    private String acknowledgedBy;
}
