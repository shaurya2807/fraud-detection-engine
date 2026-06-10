package io.frauddetection.service;

import io.frauddetection.model.enums.FraudStatus;
import io.frauddetection.repository.FraudAlertRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class FraudMetricsService {

    private final MeterRegistry        meterRegistry;
    private final FraudAlertRepository fraudAlertRepository;

    private Timer processingTimer;

    @PostConstruct
    void registerMeters() {
        // Pre-register all outcome tag values so Prometheus exposes zero-value
        // series from startup — important for alerting rules that fire on absence.
        for (String outcome : List.of("approved", "flagged", "blocked")) {
            Counter.builder("fraud.transactions.total")
                    .tag("outcome", outcome)
                    .description("Total processed transactions by fraud outcome")
                    .register(meterRegistry);
        }

        // Pre-register known rule names for the same reason.
        for (String rule : List.of("HIGH_VELOCITY", "LARGE_AMOUNT", "AMOUNT_EXCEEDS_HOURLY_LIMIT")) {
            Counter.builder("fraud.rules.triggered")
                    .tag("rule", rule)
                    .description("Total times each fraud rule was triggered")
                    .register(meterRegistry);
        }

        processingTimer = Timer.builder("fraud.processing.duration")
                .description("Time from Kafka message receipt to successful DB persist")
                .register(meterRegistry);

        // Live gauge — calls the DB on every Prometheus scrape.
        Gauge.builder("fraud.alerts.unacknowledged",
                        fraudAlertRepository,
                        FraudAlertRepository::countByAcknowledgedFalse)
                .description("Number of fraud alerts currently awaiting acknowledgement")
                .register(meterRegistry);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public void recordOutcome(FraudStatus status) {
        // Resolves the pre-registered counter; creates a new one if the tag value
        // is somehow unknown (defensive — should never happen with the current enum).
        meterRegistry.counter("fraud.transactions.total",
                "outcome", status.name().toLowerCase()).increment();
    }

    public void recordRuleTriggered(String ruleName) {
        meterRegistry.counter("fraud.rules.triggered", "rule", ruleName).increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopTimer(Timer.Sample sample) {
        sample.stop(processingTimer);
    }
}
