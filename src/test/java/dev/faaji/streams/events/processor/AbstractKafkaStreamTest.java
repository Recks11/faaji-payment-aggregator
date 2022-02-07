package dev.faaji.streams.events.processor;

import dev.faaji.streams.events.generator.AbstractPaymentEventGenerator;
import dev.faaji.streams.model.TotalView;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public abstract class AbstractKafkaStreamTest {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected static final short REPETITION_COUNT = 3;

    private static AbstractPaymentEventGenerator eventGenerator;

    protected Consumer<String, TotalView> consumer;

    /**
     * @param eventGenerator an {@link AbstractPaymentEventGenerator} implementation.
     */
    protected static void setEventGenerator(AbstractPaymentEventGenerator eventGenerator) {
        AbstractKafkaStreamTest.eventGenerator = eventGenerator;
    }

    @RepeatedTest(REPETITION_COUNT)
    public void processMessagesForSingleKey() {

        Map<String, TotalView> expectedCounts = eventGenerator.generateRandomEvents(1, 3);

        Map<String, TotalView> actualEvents = consumeActualInventoryCountEvents(3);

        assertThat(actualEvents.size()).isEqualTo(1);

        expectedCounts.forEach((key, value) ->
                assertThat(actualEvents.get(key).total()).isEqualTo(value.total()));
    }

    @RepeatedTest(REPETITION_COUNT)
    public void processAggregatedEventsForSingleKey() {
        Map<String, TotalView> expectedCounts = eventGenerator.generateRandomEvents(1, 5);

        Map<String, TotalView> originalCount = consumeActualInventoryCountEvents(5);

        expectedCounts.forEach((key, value) ->
                assertThat(originalCount.get(key).total()).isEqualTo(value.total()));

        expectedCounts = eventGenerator.generateRandomEvents(1, 5);

        Map<String, TotalView> actualCount = consumeActualInventoryCountEvents(5);

        expectedCounts.forEach((key, value) ->
                assertThat(actualCount.get(key).total()).isEqualTo(value.total()));
    }

    @RepeatedTest(REPETITION_COUNT)
    public void processAggregatedEventsForMultipleKeys() {
        Map<String, TotalView> initialCounts = eventGenerator.generateRandomEvents(10, 5);
        Map<String, TotalView> expectedEvents;

        expectedEvents = consumeActualInventoryCountEvents(50);

        expectedEvents.forEach((key, value) ->
                assertThat(initialCounts.get(key).total()).isEqualTo(value.total()));

        Map<String, TotalView> updatedCounts = eventGenerator.generateRandomEvents(10, 5);

        expectedEvents = consumeActualInventoryCountEvents(50);

        boolean atLeastOneUpdatedCountIsDifferent = false;

        for (String key : updatedCounts.keySet()) {
            assertThat(expectedEvents.get(key).total()).isEqualTo(updatedCounts.get(key).total());
            atLeastOneUpdatedCountIsDifferent = atLeastOneUpdatedCountIsDifferent || !initialCounts.get(key).equals(updatedCounts.get(key));
        }

        //Verify that the expected counts changed from the first round of events.
        assertThat(atLeastOneUpdatedCountIsDifferent).isTrue();
    }

    /**
     * Reset the state by sending 0 count values to the aggregator.
     * These events are also consumed so that subsequent tests do not have to deal with additional events.
     */
    @AfterEach
    void tearDown() {
        if (eventGenerator == null) return;
        Map<String, TotalView> events = eventGenerator.reset();
        consumeActualInventoryCountEvents(events.size());
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Consume the actual events from the output topic.
     * This implementation uses a {@link Consumer}, assuming a (an embedded) Kafka Broker but may be overridden.
     * @param expectedCount the expected number of messages is known. This avoids a timeout delay if all is well.
     *
     * @return the consumed data.
     */
    protected Map<String, TotalView> consumeActualInventoryCountEvents(int expectedCount) {
        Map<String, TotalView> inventoryCountEvents = new LinkedHashMap<>();
        int receivedCount = 0;
        while (receivedCount < expectedCount) {
            ConsumerRecords<String, TotalView> records = KafkaTestUtils.getRecords(consumer, 1000);
            if (records.isEmpty()) {
                LOG.error("No more records received. Expected {} received {}.", expectedCount, receivedCount);
                break;
            }
            receivedCount += records.count();
            for (ConsumerRecord<String, TotalView> consumerRecord : records) {
                LOG.debug("consumed " + consumerRecord.key() + " = " + consumerRecord.value().total());
                inventoryCountEvents.put(consumerRecord.key(), consumerRecord.value());
            }
        }
        return inventoryCountEvents;
    }
}

