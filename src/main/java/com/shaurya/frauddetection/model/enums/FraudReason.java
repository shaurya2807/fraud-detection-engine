package com.shaurya.frauddetection.model.enums;

public enum FraudReason {
    NONE,
    HIGH_VELOCITY,
    LARGE_AMOUNT,
    IMPOSSIBLE_TRAVEL,
    AMOUNT_EXCEEDS_HOURLY_LIMIT,
    MULTIPLE_RULES_TRIGGERED
}
