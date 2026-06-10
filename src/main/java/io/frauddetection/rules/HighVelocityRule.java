package io.frauddetection.rules;

import io.frauddetection.config.FraudProperties;
import io.frauddetection.model.dto.FraudEvaluationResult;
import io.frauddetection.model.dto.TransactionEvent;
import io.frauddetection.model.enums.FraudReason;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class HighVelocityRule implements FraudRule {

    private static final String KEY_PREFIX = "velocity:txn:";
    private static final long TTL_MINUTES = 2;

    private final StringRedisTemplate redisTemplate;
    private final FraudProperties fraudProperties;

    @Override
    public void evaluate(TransactionEvent event, FraudEvaluationResult result) {
        FraudProperties.Rules.HighVelocity cfg = fraudProperties.getRules().getHighVelocity();
        String key = KEY_PREFIX + event.getAccountId();
        long nowMillis = System.currentTimeMillis();
        long windowStart = nowMillis - (cfg.getWindowSeconds() * 1000L);

        ZSetOperations<String, String> zOps = redisTemplate.opsForZSet();

        // Add current transaction; member is transactionId so duplicates are idempotent
        zOps.add(key, event.getTransactionId(), nowMillis);
        // Evict entries that have aged out of the window
        zOps.removeRangeByScore(key, 0, windowStart - 1);
        Long count = zOps.zCard(key);
        // Reset TTL so the key expires 2 min after last activity
        redisTemplate.expire(key, TTL_MINUTES, TimeUnit.MINUTES);

        if (count != null && count > cfg.getMaxTransactions()) {
            log.debug("HighVelocityRule triggered for account={}: count={} > max={}",
                    event.getAccountId(), count, cfg.getMaxTransactions());
            result.addTriggeredRule(ruleName());
            result.incrementScore(scoreWeight());
        }
    }

    @Override
    public String ruleName() {
        return FraudReason.HIGH_VELOCITY.name();
    }

    @Override
    public int scoreWeight() {
        return fraudProperties.getRules().getHighVelocity().getScore();
    }
}
