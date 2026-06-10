package io.frauddetection.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaStreamsConfig {

    /**
     * Overrides and extends Spring Boot's auto-configured Kafka Streams settings.
     * Properties that cannot be expressed in application.yml (e.g. the uncaught
     * exception handler lambda) are set here; scalar overrides are explicit so
     * the config class is the single authoritative source for Streams tuning.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer kafkaStreamsFactoryBeanConfigurer(
            @Value("${spring.kafka.streams.application-id}") String appId,
            @Value("${spring.kafka.bootstrap-servers}")       String bootstrapServers) {

        return fb -> {
            Map<String, Object> props = new HashMap<>();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,           appId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);

            // String/String serdes for the raw-JSON source and sink streams
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,     StreamsConfig.EXACTLY_ONCE_V2);
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,        2);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,       1_000);

            // Keep internal topic replication at 1 for single-broker dev clusters
            props.put("replication.factor", 1);

            fb.setStreamsConfiguration(props);

            fb.setStreamsUncaughtExceptionHandler(ex -> {
                log.error("Uncaught Kafka Streams exception on stream thread — replacing thread: {}",
                        ex.getMessage(), ex);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
        };
    }
}
