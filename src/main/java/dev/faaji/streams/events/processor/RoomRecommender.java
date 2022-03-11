package dev.faaji.streams.events.processor;

import dev.faaji.streams.api.v1.domain.UserRegistration;
import dev.faaji.streams.api.v1.response.RoomRecommendationResponse;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static dev.faaji.streams.service.bindings.MaterialBinding.USER_ROOM_STORE;
import static dev.faaji.streams.service.bindings.StreamBindings.ROOM_RECOMMENDER;

@Component
public class RoomRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RoomRecommender.class);
    public static final String RECOMMENDER_TABLE_NAME = "user-rooms";
    public final Serde<RoomRecommendationResponse> roomSerde;

    public RoomRecommender(Serde<RoomRecommendationResponse> roomSerde) {
        this.roomSerde = roomSerde;
    }

    @Bean(ROOM_RECOMMENDER)
    public Function<KStream<String, UserRegistration>, KTable<String, RoomRecommendationResponse>> recommendRoom() {
        return userStream -> userStream.map((key, userRegistration) -> {
            LOG.info("recommending room for user %s".formatted(userRegistration.userId()));
            String room = recommendRoom(userRegistration.interests(), getRoomToInterestMap());
            String updatedKey = "%s:%s".formatted(userRegistration.eventId(), userRegistration.userId());
            return new KeyValue<>(updatedKey, new RoomRecommendationResponse(
                    userRegistration.userId(),
                    userRegistration.eventId(),
                    room
            ));
        }).toTable(Named.as(RECOMMENDER_TABLE_NAME), Materialized.<String, RoomRecommendationResponse, KeyValueStore<Bytes, byte[]>>as(USER_ROOM_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(roomSerde));
    }

    private String recommendRoom(String[] userInterests, Map<String, String[]> interestMap) {
        Set<String> userInterest = new HashSet<>(Set.of(userInterests));

        String recommended = "NONE";
        int previousTotal = 0;
        for (String key : interestMap.keySet()) {
            int totalCommon = 0;
            for (String interest : interestMap.get(key)) {
                if (userInterest.contains(interest)) totalCommon++;
            }

            if (totalCommon > previousTotal) {
                previousTotal = totalCommon;
                recommended = key;
            }
        }

        if (recommended.equals("NONE")) {
            int index = new Random().nextInt(interestMap.size());
            recommended = interestMap.keySet().toArray()[index].toString();
        }

        return recommended;
    }

    private Map<String, String[]> getRoomToInterestMap() {
        return Map.of();
    }
}
