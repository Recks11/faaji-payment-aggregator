package dev.faaji.streams.events.generator;

import dev.faaji.streams.api.v1.domain.UserRegistration;
import dev.faaji.streams.api.v1.response.RoomRecommendationResponse;
import org.apache.kafka.streams.TopologyTestDriver;

import static dev.faaji.streams.util.RandomUtils.generateString;
import static dev.faaji.streams.util.RandomUtils.randomGender;

public class RoomRecommendationEventGenerator extends
        AbstractTopologyTestDriverEventGenerator<String, UserRegistration, String, RoomRecommendationResponse> {

    public RoomRecommendationEventGenerator(TopologyTestDriver testDriver) {
        super(testDriver);
    }

    @Override
    public void generateEvents(int numKeys, int numEventsPerKey) {
        for (int i = 0; i < numKeys; i++) {
            String key = generateString(6);
            for (int j = 0; j < numEventsPerKey; j++) {
                var value = new UserRegistration(
                        generateString(8),
                        null,
                        randomGender(),
                        "ROOM-EVENT"
                );

                sendEvent(key, value);
            }
        }
    }

    @Override
    public void sendEvent(String key, UserRegistration value) {
        super.sendEvent(key, value);
    }
}
