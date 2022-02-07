package dev.faaji.streams.service;

import dev.faaji.streams.model.Events;
import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.model.TotalView;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;


@Component
public class PaymentProcessor implements EventProcessor<KeyValue<String, PaymentUpdateEvent>, KeyValue<String, TotalView>> {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentProcessor.class);

    @Override
    public KeyValue<String, TotalView> process(KeyValue<String, PaymentUpdateEvent> event) {
        return parseTotalView(event.key, event.value);
    }

    private BigDecimal parseTotal(PaymentUpdateEvent event) {
        return switch (Events.valueOf(event.getType())) {
            case DEBIT -> event.getData().amount().abs().negate();
            case CREDIT, REPLACE -> event.getData().amount().abs();
        };
    }

    private KeyValue<String, TotalView> parseTotalView(String key, PaymentUpdateEvent paymentUpdateEvent) {
        return new KeyValue<>(key, new TotalView(key, paymentUpdateEvent.getData().partyId(), parseTotal(paymentUpdateEvent)));
    }
}
