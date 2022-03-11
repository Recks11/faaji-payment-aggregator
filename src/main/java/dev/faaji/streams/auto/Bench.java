package dev.faaji.streams.auto;

import dev.faaji.streams.api.v1.domain.PartyModification;
import dev.faaji.streams.api.v1.domain.PartyModificationEvent;
import dev.faaji.streams.api.v1.domain.UserRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Random;
import java.util.UUID;

//@Component
//@EnableScheduling
public class Bench {
    private static final
    Logger LOG = LoggerFactory.getLogger(Bench.class);
    private final KafkaTemplate<String, Object> producer;

    public Bench(KafkaTemplate<String, Object> producer) {
        this.producer = producer;
    }

    @Scheduled(fixedDelay = 8000L)
    public void produceIndefinitely() {
        try {
            var event = new PartyModificationEvent(
                    "CREATE",
                    new PartyModification("6206989d921c1500161a7487")
            );
            producer.send("valentines-parties", event.getData().getEventId(), event);
            LOG.info("Emitted %s".formatted(event));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }

        try {
            var event = new PartyModificationEvent(
                    "CREATE",
                    new PartyModification("6202d1a4a891210016548311")
            );
            producer.send("valentines-parties", event.getData().getEventId(), event);
            LOG.info("Emitted %s".formatted(event));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

//    @Scheduled(fixedDelay = 10000L)
    public void produceUserData() {
        var random = new Random();
        try {
            var event = new UserRegistration(
                    "DUMMY_USER-%s".formatted(random.nextInt(100)),
                    new String[]{UUID.randomUUID().toString().substring(0, 4), UUID.randomUUID().toString().substring(0, 4)},
                    "non-binary",
                    "9qwyfibaoeeqr77"
            );
            producer.send("valentine-user-registration", event.eventId(), event);
            LOG.info("Emitted %s".formatted(event));
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }

    }
}
