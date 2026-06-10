package io.frauddetection.service;

import io.frauddetection.model.dto.AcknowledgeRequest;
import io.frauddetection.model.dto.FraudAlertResponse;
import io.frauddetection.model.dto.FraudDecisionResponse;
import io.frauddetection.model.entity.FraudAlert;
import io.frauddetection.model.entity.Transaction;
import io.frauddetection.repository.FraudAlertRepository;
import io.frauddetection.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class FraudDecisionService {

    private final TransactionRepository transactionRepository;
    private final FraudAlertRepository  fraudAlertRepository;

    public FraudDecisionResponse getByTransactionId(String transactionId) {
        Transaction tx = transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Transaction not found: " + transactionId));
        return toDecisionResponse(tx);
    }

    public Page<FraudDecisionResponse> getByAccountId(String accountId, Pageable pageable) {
        return transactionRepository.findByAccountId(accountId, pageable)
                .map(this::toDecisionResponse);
    }

    @Transactional
    public FraudAlertResponse acknowledgeAlert(UUID alertId, AcknowledgeRequest request) {
        FraudAlert alert = fraudAlertRepository.findById(alertId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Alert not found: " + alertId));

        if (alert.isAcknowledged()) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "Alert " + alertId + " has already been acknowledged");
        }

        alert.setAcknowledged(true);
        alert.setAcknowledgedAt(Instant.now());
        alert.setAcknowledgedBy(request.getAcknowledgedBy());

        return toAlertResponse(fraudAlertRepository.save(alert));
    }

    // ── Mappers ───────────────────────────────────────────────────────────────

    private FraudDecisionResponse toDecisionResponse(Transaction t) {
        return FraudDecisionResponse.builder()
                .transactionId(t.getTransactionId())
                .accountId(t.getAccountId())
                .amount(t.getAmount())
                .currency(t.getCurrency())
                .fraudStatus(t.getFraudStatus())
                .fraudReason(t.getFraudReason())
                .fraudScore(t.getFraudScore())
                .rulesTriggered(t.getRulesTriggered())
                .processedAt(t.getProcessedAt())
                .build();
    }

    private FraudAlertResponse toAlertResponse(FraudAlert a) {
        return FraudAlertResponse.builder()
                .id(a.getId())
                .transactionId(a.getTransactionId())
                .accountId(a.getAccountId())
                .fraudScore(a.getFraudScore())
                .fraudReason(a.getFraudReason())
                .rulesTriggered(a.getRulesTriggered())
                .alertedAt(a.getAlertedAt())
                .acknowledged(a.isAcknowledged())
                .acknowledgedAt(a.getAcknowledgedAt())
                .acknowledgedBy(a.getAcknowledgedBy())
                .build();
    }
}
