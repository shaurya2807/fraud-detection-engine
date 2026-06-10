package io.frauddetection.rules;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudReason;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class HourlyAmountLimitRule implements FraudRule {

    private static final String KEY_PREFIX = "velocity:amount:";

    private final StringRedisTemplate redisTemplate;
    private final FraudProperties fraudProperties;

    @Override
    public void evaluate(TransactionEvent event, FraudEvaluationResult result) {
        FraudProperties.Rules.HourlyLimit cfg = fraudProperties.getRules().getHourlyLimit();
        String key = KEY_PREFIX + event.getAccountId();

        // Atomically add the transaction amount to the rolling hourly total (INCRBYFLOAT)
        Double newTotal = redisTemplate.opsForValue()
                .increment(key, event.getAmount().doubleValue());

        // Set TTL only if the key has no expiry (newly created or expiry was lost)
        Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
        if (ttl != null && ttl < 0) {
            redisTemplate.expire(key, cfg.getWindowSeconds(), TimeUnit.SECONDS);
        }

        if (newTotal != null && BigDecimal.valueOf(newTotal).compareTo(cfg.getMaxAmount()) > 0) {
            log.debug("HourlyAmountLimitRule triggered for account={}: total={} > max={}",
                    event.getAccountId(), newTotal, cfg.getMaxAmount());
            result.addTriggeredRule(ruleName());
            result.incrementScore(scoreWeight());
        }
    }

    @Override
    public String ruleName() {
        return FraudReason.AMOUNT_EXCEEDS_HOURLY_LIMIT.name();
    }

    @Override
    public int scoreWeight() {
        return fraudProperties.getRules().getHourlyLimit().getScore();
    }
}
