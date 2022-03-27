package dev.faaji.streams.events.generator;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

public abstract class AbstractTopologyTestDriverEventGenerator<KI, VI, KO, VO> {

    private final TopologyTestDriver topologyTestDriver;
    private TestInputTopic<KI, VI> testInputTopic;
    private TestOutputTopic<KO, VO> testOutputTopic;

    public AbstractTopologyTestDriverEventGenerator(TopologyTestDriver testDriver) {
        this.topologyTestDriver = testDriver;
    }


    public void configureInputTopic(String inputTopic,
                                    Serializer<KI> keySerializer,
                                    Serializer<VI> valueSerializer) {
        this.testInputTopic = topologyTestDriver.createInputTopic(inputTopic, keySerializer, valueSerializer);
    }

    public void configureOutputTopic(String outputTopic,
                                     Deserializer<KO> keyDeserializer,
                                     Deserializer<VO> valueDeserializer) {
        this.testOutputTopic = topologyTestDriver.createOutputTopic(outputTopic, keyDeserializer, valueDeserializer);
    }

    protected void sendEvent(KI key, VI value) {
        getTestInputTopic().pipeInput(key, value);
    }

    protected void generateEvents(int numKeys, int numEventsPerKey) {

    }

    public TestInputTopic<KI, VI> getTestInputTopic() {
        return testInputTopic;
    }

    public TestOutputTopic<KO, VO> getTestOutputTopic() {
        return testOutputTopic;
    }
}
