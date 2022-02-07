package dev.faaji.streams.events.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.service.EventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;

//@Service
//@KafkaListener(topics = StreamBindings.PAYMENT_TOPIC)
public class PaymentListenerBean {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final ObjectMapper objectMapper;
    private final EventProcessor<PaymentUpdateEvent, Boolean> eventProcessor;

    public PaymentListenerBean(ObjectMapper objectMapper,
                               EventProcessor<PaymentUpdateEvent, Boolean> eventProcessor) {
        this.objectMapper = objectMapper;
        this.eventProcessor = (event) -> {
            LOG.info("received event %s%n".formatted(event));
            return true;
        };
    }

    @KafkaHandler
    public void processPaymentEvent(ConsumerRecord<String, PaymentUpdateEvent> event) {
        eventProcessor.process(event.value());
    }

    @KafkaHandler(isDefault = true)
    public void processGenericObject(ConsumerRecord<String, Object> data) {
        try {
            PaymentUpdateEvent event = objectMapper.convertValue(data.value(), PaymentUpdateEvent.class);
            eventProcessor.process(event);
        } catch (Exception ex) {
            // nothing
        }
    }
}
