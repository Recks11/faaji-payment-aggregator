package dev.faaji.streams.auto;

import dev.faaji.streams.model.Events;
import dev.faaji.streams.model.Payment;
import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.math.BigDecimal;
import java.util.Random;

//@Component
//@EnableScheduling
public class Bench {
    private static final
    Logger LOG = LoggerFactory.getLogger(Bench.class);

    private final KafkaTemplate<String, PaymentUpdateEvent> producer;

    public Bench(KafkaTemplate<String, PaymentUpdateEvent> producer) {
        this.producer = producer;
    }

    @Scheduled(fixedDelay = 8000L)
    public void produceIndefinitely() {
        BigDecimal[] options = {new BigDecimal("1000"), new BigDecimal("2000"), new BigDecimal("3000")};
        Events[] evOps = {Events.DEBIT, Events.CREDIT};
        Random random = new Random();
        try {
            var event = new PaymentUpdateEvent(
                    evOps[random.nextInt(evOps.length)],
                    new Payment(
                            "t7QpJEHXrB",
                            "2020-12-22T09:41:41.245Z",
                            "paystack",
                            options[random.nextInt(options.length)],
                            "success",
                            "5fe1c197f1cc6a00176200c6",
                            "5fdb6ea1d952cb0017c2fc99"
                    )
            );
            producer.send(StreamBindings.INPUT_BINDING_IN_DESTINATION, event.getData().userId(), event);
            LOG.info("Emitted %s event %s by user %s with amount %s".formatted(event.getType(), event.getData().partyId(), event.getData().userId(), event.getData().amount()));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }
}
