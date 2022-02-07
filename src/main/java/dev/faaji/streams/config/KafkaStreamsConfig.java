package dev.faaji.streams.config;

import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

//@Configuration
//@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration streamsConfiguration(KafkaStreamsBinderConfigurationProperties properties) {
        var props = properties.getConfiguration();
        return new KafkaStreamsConfiguration(Map.copyOf(props));
    }

    @Bean
    public NewTopic tableTopic() {
        return TopicBuilder.name(StreamBindings.INPUT_BINDING_OUT_DESTINATION)
                .partitions(2)
                .compact()
                .config("max.message.bytes", "2048")
                .build();
    }
}
