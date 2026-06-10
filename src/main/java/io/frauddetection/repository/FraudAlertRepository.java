package io.frauddetection.repository;

import io.frauddetection.model.entity.FraudAlert;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface FraudAlertRepository extends JpaRepository<FraudAlert, UUID> {

    Page<FraudAlert> findByAccountId(String accountId, Pageable pageable);

    Optional<FraudAlert> findByTransactionId(String transactionId);
}
