package dev.faaji.streams.events.processor;

import dev.faaji.streams.events.generator.TopologyTestDriverEventGenerator;
import dev.faaji.streams.model.Events;
import dev.faaji.streams.model.Payment;
import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.PaymentProcessor;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.util.*;

import static dev.faaji.streams.util.TestDecimalUtils.decimalValue;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PaymentAggregatorUnitTest extends AbstractKafkaStreamTest {

    private final Serde<TotalView> countEventSerde = new JsonSerde<>(TotalView.class);
    private final Serde<PaymentUpdateEvent> updateEventSerde = new JsonSerde<>(PaymentUpdateEvent.class);
    private final Serde<String> keySerde = new Serdes.StringSerde();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, PaymentUpdateEvent> testInputTopic;
    private TestOutputTopic<String, TotalView> testOutputTopic;

    static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();
        // Need to be set even these do not matter with TopologyTestDriver
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "TopologyTestDriver");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        return streamsConfiguration;
    }

    private void configureDeserializer(Deserializer<?> deserializer,
                                       Class<?> keyDefaultType,
                                       Class<?> valueDefaultType, boolean isKey) {
        Map<String, Object> deserializerConfig = new HashMap<>();
        deserializerConfig.put(JsonDeserializer.KEY_DEFAULT_TYPE, keyDefaultType);
        deserializerConfig.put(JsonDeserializer.VALUE_DEFAULT_TYPE, valueDefaultType);
        deserializer.configure(deserializerConfig, isKey);
    }

    @BeforeEach
    void setup() {
        configureDeserializer(countEventSerde.deserializer(), String.class, TotalView.class, false);
        configureDeserializer(keySerde.deserializer(), String.class, null, true);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PaymentUpdateEvent> input = builder.stream(StreamBindings.INPUT_BINDING_IN_DESTINATION, Consumed.with(keySerde, updateEventSerde));
        PaymentAggregator paymentAggregator = new PaymentAggregator(new PaymentProcessor());

        KTable<String, TotalView> output = paymentAggregator.process().apply(input);
        output.toStream().to(StreamBindings.INPUT_BINDING_OUT_DESTINATION);

        Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, getStreamsConfiguration());

        LOG.debug(topology.describe().toString());


        TopologyTestDriverEventGenerator eventGenerator = new TopologyTestDriverEventGenerator(testDriver);

        eventGenerator.configureInputTopic(StreamBindings.INPUT_BINDING_IN_DESTINATION, keySerde.serializer(),
                updateEventSerde.serializer());
        eventGenerator.configureOutputTopic(StreamBindings.INPUT_BINDING_OUT_DESTINATION, keySerde.deserializer(),
                countEventSerde.deserializer());

        setEventGenerator(eventGenerator);
        testOutputTopic = eventGenerator.getTestOutputTopic();
        testInputTopic = eventGenerator.getTestInputTopic();
    }

    @AfterEach
    void tearDown() {
        super.tearDown();
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @RepeatedTest(REPETITION_COUNT)
    void successfullyComputesAggregates() {
        final String EVENT_NAME = "TEST_EVENT";
        var events = List.of(
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(1000.00)),
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(2000.00)),
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(1500.00)),
                createTestEvent(Events.DEBIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(500.00))
        );

        events.stream().map(PaymentUpdateEvent::from).forEach(e -> testInputTopic.pipeInput(e.getData().userId(), e));

        KeyValueStore<String, ValueAndTimestamp<TotalView>> store = testDriver.getTimestampedKeyValueStore(StreamBindings.PAYMENT_TOTAL_STATE_STORE);
        ValueAndTimestamp<TotalView> test_event = store.get("TEST_EVENT");

        assertThat(test_event.value().total()).isEqualTo(decimalValue(4000.00));
    }

    @RepeatedTest(REPETITION_COUNT)
    void given_multipleEvents_then_computeAccurate_sum() {
        final String EVENT_NAME = "TEST_EVENT_1";
        final String EVENT_NAME2 = "TEST_EVENT_2";
        var events = List.of(
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(1000.00)),
                createTestEvent(Events.CREDIT, "ANOTHER_USER", EVENT_NAME2, BigDecimal.valueOf(2000.00)),
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(1500.00)),
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(500.00)),
                createTestEvent(Events.CREDIT, "ANOTHER_USER", EVENT_NAME2, BigDecimal.valueOf(1000.00)),
                createTestEvent(Events.CREDIT, "ANOTHER_USER", EVENT_NAME2, BigDecimal.valueOf(850.00)),
                createTestEvent(Events.CREDIT, "TEST_USER", EVENT_NAME, BigDecimal.valueOf(500.00))
        );

        events.forEach(e -> testInputTopic.pipeInput(e.getData().userId(), e));

        KeyValueStore<String, ValueAndTimestamp<TotalView>> store = testDriver.getTimestampedKeyValueStore(StreamBindings.PAYMENT_TOTAL_STATE_STORE);
        ValueAndTimestamp<TotalView> test_event = store.get(EVENT_NAME);
        ValueAndTimestamp<TotalView> another_event = store.get(EVENT_NAME2);

        assertThat(test_event.value().total()).isEqualTo(decimalValue(3500));
        assertThat(another_event.value().total()).isEqualTo(decimalValue(3850));
    }

    @Override
    protected Map<String, TotalView> consumeActualInventoryCountEvents(int expectedCount) {
        Map<String, TotalView> inventoryCountEvents = new LinkedHashMap<>();
        int receivedCount = 0;
        while (receivedCount < expectedCount) {
            TestRecord<String, TotalView> record = testOutputTopic.readRecord();
            if (record == null) {
                break;
            }
            receivedCount++;
            LOG.debug("consumed " + record.key() + " = " + record.value().total());
            inventoryCountEvents.put(record.key(), record.value());
        }
        return inventoryCountEvents;
    }

    private PaymentUpdateEvent createTestEvent(Events type, String userId, String eventId, BigDecimal amount) {
        return new PaymentUpdateEvent(type,
                new Payment("REFERENCE", new Date().toString(),"paystack", amount, "success", userId, eventId));
    }
}
