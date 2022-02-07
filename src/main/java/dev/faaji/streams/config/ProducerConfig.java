package dev.faaji.streams.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

//@Configuration
public class ProducerConfig {

    private final KafkaProperties kafkaProperties;

    public ProducerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    <T> ProducerFactory<String, T> producerFactory() {
        Map<String, Object> configMap = kafkaProperties.buildProducerProperties();
        configMap.putAll(Map.of(
                org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));

        return new DefaultKafkaProducerFactory<>(configMap, new StringSerializer(),
                new JsonSerializer<>());
    }

    @Bean
    <T> KafkaTemplate<String, T> genericTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
