package dev.faaji.streams.config;

import dev.faaji.streams.api.v1.domain.PartyModificationEvent;
import dev.faaji.streams.api.v1.domain.ValentineUserRegistration;
import dev.faaji.streams.model.PaymentUpdateEvent;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public static Serde<PartyModificationEvent> PartySerde() {
        return new JsonSerde<>(PartyModificationEvent.class);
    }

    @Bean
    public static Serde<ValentineUserRegistration> UserRegSerde() {
        return new JsonSerde<>(ValentineUserRegistration.class);
    }

    @Bean
    public static Serde<PaymentUpdateEvent> PaymentUpdateSerde() {
        return new JsonSerde<>(PaymentUpdateEvent.class);
    }
}
