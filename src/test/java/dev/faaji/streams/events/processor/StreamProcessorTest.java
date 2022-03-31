package dev.faaji.streams.events.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.Properties;

public abstract class StreamProcessorTest {
    private TopologyTestDriver testDriver;

    static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();
        // Need to be set even these do not matter with TopologyTestDriver
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "TopologyTestDriver");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return streamsConfiguration;
    }

    @BeforeEach
    void setup() {

    }

    @AfterEach
    void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    protected <T> Serde<T> createSerde() {
        return new JsonSerde<>();
    }

    protected <T> Serde<T> createSerde(Class<T> tClass) {
        var serde = new JsonSerde<>(tClass);
        serde.configure(Map.of(JsonDeserializer.TRUSTED_PACKAGES, "*"), false);
        return serde;
    }

    public TopologyTestDriver getTestDriver() {
        return testDriver;
    }

    public void setTestDriver(TopologyTestDriver testDriver) {
        this.testDriver = testDriver;
    }
}
