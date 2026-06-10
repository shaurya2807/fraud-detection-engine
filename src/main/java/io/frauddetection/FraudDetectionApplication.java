package io.frauddetection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
@EnableScheduling
@EnableRetry
public class FraudDetectionApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionApplication.class, args);
    }
}
