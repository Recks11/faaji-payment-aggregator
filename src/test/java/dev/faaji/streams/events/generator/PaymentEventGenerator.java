package dev.faaji.streams.events.generator;

import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.PaymentProcessor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

public class PaymentEventGenerator extends AbstractPaymentEventGenerator {

    private final TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, PaymentUpdateEvent> testInputTopic;
    private TestOutputTopic<String, TotalView> testOutputTopic;


    public PaymentEventGenerator(TopologyTestDriver topologyTestDriver) {
        this.topologyTestDriver = topologyTestDriver;
    }

    public void configureInputTopic(String inputTopic,
                                    Serializer<String> keySerializer,
                                    Serializer<PaymentUpdateEvent> valueSerializer) {
        this.testInputTopic = topologyTestDriver.createInputTopic(inputTopic, keySerializer, valueSerializer);
    }

    public void configureOutputTopic(String outputTopic,
                                     Deserializer<String> keyDeserializer,
                                     Deserializer<TotalView> valueDeserializer) {
        this.testOutputTopic = topologyTestDriver.createOutputTopic(outputTopic, keyDeserializer, valueDeserializer);
    }

    @Override
    protected void doSendEvent(String key, PaymentUpdateEvent value) {
        testInputTopic.pipeInput(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected PaymentProcessor getEventProcessor() {
        return new PaymentProcessor();
    }

    public TestInputTopic<String, PaymentUpdateEvent> getTestInputTopic() {
        return testInputTopic;
    }

    public TestOutputTopic<String, TotalView> getTestOutputTopic() {
        return testOutputTopic;
    }
}
