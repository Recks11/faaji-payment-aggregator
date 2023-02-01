package dev.faaji.streams.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfiguration {
    @Value("${spring.cloud.stream.bindings.input-in-0.destination}")
    private String inputInTopic;
    @Value("${spring.cloud.stream.bindings.input-out-0.destination}")
    private String inputOutTopic;
    @Value("${spring.cloud.stream.bindings.partyinit-in-0.destination}")
    private String partyInitInTopic;
    @Value("${spring.cloud.stream.bindings.partyinit-out-0.destination}")
    private String partyInitOutTopic;
    @Value("${spring.cloud.stream.bindings.userregister-in-0.destination}")
    private String userRegisterInTopic;
    @Value("${spring.cloud.stream.bindings.userregister-out-0.destination}")
    private String userRegisterOutTopic;
    @Value("${spring.cloud.stream.bindings.userinterest-in-0.destination}")
    private String userInterestInTopic;
    @Value("${spring.cloud.stream.bindings.userinterest-out-0.destination}")
    private String userInterestOutTopic;
    @Value("${spring.cloud.stream.bindings.room-recommender-in-0.destination}")
    private String userRecommenderInTopic;
    @Value("${spring.cloud.stream.bindings.room-recommender-out-0.destination}")
    private String userRecommenderOutTopic;

    @Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        // Depending on you Kafka Cluster setup you need to configure
        // additional properties!
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(inputInTopic)
                .partitions(1)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic inputOut() {
        return TopicBuilder.name(inputOutTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic partyInitTopic() {
        return TopicBuilder.name(partyInitInTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic partyInitTopicOut() {
        return TopicBuilder.name(partyInitOutTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userRegIn() {
        return TopicBuilder.name(userRegisterInTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userRegOut() {
        return TopicBuilder.name(userRegisterOutTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userIntsIn() {
        return TopicBuilder.name(userInterestInTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userIntsOut() {
        return TopicBuilder.name(userInterestOutTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userRecIn() {
        return TopicBuilder.name(userRecommenderInTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic userRecOut() {
        return TopicBuilder.name(userRecommenderOutTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
