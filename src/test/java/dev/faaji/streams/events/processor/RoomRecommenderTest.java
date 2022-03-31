package dev.faaji.streams.events.processor;

import dev.faaji.streams.api.v1.domain.UserRegistration;
import dev.faaji.streams.api.v1.response.RoomRecommendationResponse;
import dev.faaji.streams.events.generator.RoomRecommendationEventGenerator;
import dev.faaji.streams.service.bindings.MaterialBinding;
import dev.faaji.streams.util.KeyUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import static dev.faaji.streams.util.RandomUtils.generateString;

public class RoomRecommenderTest extends StreamProcessorTest {
    private static final String IN_TOPIC = "FaajiUserRegistration";
    private static final String OUT_TOPIC = "FaajiUserRooms";
    private RoomRecommendationEventGenerator generator;

    @BeforeEach
    void setup() {
        WebClient webClient = WebClient.builder()
                .baseUrl("https://faaji-backend-staging.herokuapp.com")
                .build();
        RoomRecommender recommender = new RoomRecommender(createSerde(RoomRecommendationResponse.class), webClient);
        final StreamsBuilder sb = new StreamsBuilder();
        var userRegSerde = createSerde(UserRegistration.class);
        var roomRecommendationResponseSerde = createSerde(RoomRecommendationResponse.class);

        var in = sb.stream(IN_TOPIC, Consumed.with(Serdes.String(), userRegSerde));
        var out = recommender.recommendRoom().apply(in);

        out.toStream().to(OUT_TOPIC);

        Topology streamTopology = sb.build();
        setTestDriver(new TopologyTestDriver(streamTopology, getStreamsConfiguration()));

        generator = new RoomRecommendationEventGenerator(getTestDriver());
        generator.configureInputTopic(IN_TOPIC, Serdes.String().serializer(), userRegSerde.serializer());
        generator.configureOutputTopic(OUT_TOPIC, Serdes.String().deserializer(), roomRecommendationResponseSerde.deserializer());
    }

    @Test
    void canRecommendRoomWithValidRooms() {
        UserRegistration userRegistration = new UserRegistration(
                "MOCK_USER",
                new String[]{},
                "FEMALE",
                "623b8aac5fe029001650990b"
        );
        generator.sendEvent(generateString(12), userRegistration);

        var store = getTestDriver()
                .<String, RoomRecommendationResponse>getTimestampedKeyValueStore(MaterialBinding.USER_ROOM_STORE);
        var key = KeyUtils.merge(userRegistration.eventId(), userRegistration.userId());
        var recommendation = store.get(key);

        Assertions.assertNotNull(recommendation.value());
        Assertions.assertEquals(recommendation.value().partyId(), userRegistration.eventId());
        Assertions.assertEquals(recommendation.value().userId(), userRegistration.userId());
        Assertions.assertEquals(recommendation.value().roomId(), "623b9c3fec1f8a0016e577d5");
    }

    @Test
    void canRecommendNONEwithNoRooms() {
        UserRegistration userRegistration = new UserRegistration(
                "MOCK_USER",
                new String[]{},
                "FEMALE",
                "NEXT_EVENT"
        );
        generator.sendEvent(generateString(12), userRegistration);

        var store = getTestDriver()
                .<String, RoomRecommendationResponse>getTimestampedKeyValueStore(MaterialBinding.USER_ROOM_STORE);
        var key = KeyUtils.merge(userRegistration.eventId(), userRegistration.userId());
        var recommendation = store.get(key);

        Assertions.assertNotNull(recommendation.value());
        Assertions.assertEquals(recommendation.value().partyId(), userRegistration.eventId());
        Assertions.assertEquals(recommendation.value().userId(), userRegistration.userId());
        Assertions.assertEquals("NONE", recommendation.value().roomId());
    }

}
