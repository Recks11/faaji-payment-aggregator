package dev.faaji.streams.events.generator;

import dev.faaji.streams.model.Events;
import dev.faaji.streams.model.Payment;
import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.PaymentProcessor;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public abstract class AbstractPaymentEventGenerator {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Map<String, TotalView> accumulatedTotalCounts = new LinkedHashMap<>();

    public Map<String, TotalView> generateRandomEvents(int numberKeys, int eventsPerKey) {
        Events[] actions = {Events.CREDIT, Events.DEBIT};
        return doGenerateEvents(numberKeys, eventsPerKey, actions);
    }

    /**
     * Resets the Kafka stream materialized state and the internal state by sending a 0 count for each existing key.
     *
     * @return the state prior to invoking this method.
     */
    public Map<String, TotalView> reset() {
        Map<String, TotalView> current
                = Map.copyOf(accumulatedTotalCounts);
        // TODO do something to clear existing state for materialised view
        accumulatedTotalCounts.keySet().forEach(key -> {
            var event = new PaymentUpdateEvent(Events.REPLACE,
                    new Payment("RESET_EVENT", new Date().toString(), BigDecimal.ZERO, "success", null, key));
            sendEvent(key, event);
        });
        accumulatedTotalCounts.clear();
        return current;
    }

    /**
     * @param numberKeys   number of keys to generate events for.
     * @param eventsPerKey number of events per key.
     * @param actions      the list of update actions to include.
     * @return expected calculated counts. Accumulates values since last reset to simulate what the aggregator does.
     */
    private Map<String, TotalView> doGenerateEvents(int numberKeys, int eventsPerKey, Events[] actions) {
        Random random = new Random();
        String[] values = {"1000.00", "2000.00", "3000.00", "4000.00"};

        for (int j = 0; j < numberKeys; j++) {
            String userId = "TEST_USER_"+j;
            String eventId = "TEST_EVENT_"+j;
            BigDecimal total = accumulatedTotalCounts.containsKey(eventId) ? accumulatedTotalCounts.get(eventId).total() : BigDecimal.ZERO;
            for (int i = 0; i < eventsPerKey; i++) {
                PaymentUpdateEvent event = new PaymentUpdateEvent(
                        actions[random.nextInt(actions.length)],
                        new Payment(UUID.randomUUID().toString(),
                                new Date().toString(),
                                new BigDecimal(values[random.nextInt(values.length)]),
                                "success",
                                userId, eventId));
                KeyValue<String, TotalView> processed = getEventProcessor().process(KeyValue.pair(userId, event));
                total = total.add(processed.value.total());
                sendEvent(userId, event);
            }
            accumulatedTotalCounts.put(eventId, new TotalView(userId, eventId, total));
        }

        return Collections.unmodifiableMap(new LinkedHashMap<>(accumulatedTotalCounts));

    }

    protected void sendEvent(String key, PaymentUpdateEvent value) {
        LOG.debug("Sending PaymentUpdateEvent: key {} payment {}",
                key, value.toString());
        doSendEvent(key, value);
    }

    protected abstract void doSendEvent(String key, PaymentUpdateEvent value);
    protected abstract  <E extends PaymentProcessor> E getEventProcessor();
}
