package dev.faaji.streams.service;

import dev.faaji.streams.api.v1.domain.User;
import dev.faaji.streams.api.v1.domain.UserMatch;
import org.apache.kafka.streams.KeyValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UserMatchStreamProcessor implements EventProcessor<KeyValue<String, User>, KeyValue<String, Map<String, SortedSet<UserMatch>>>> {

    @Override
    public KeyValue<String, Map<String, SortedSet<UserMatch>>> process(KeyValue<String, User> event) {
        return null;
    }

    private Map<String, SortedSet<UserMatch>> addToMatchMap(Map<String, SortedSet<UserMatch>> matchStore, List<User> users) {
        return matchUsersMatrix(matchStore, users);
    }


    public Map<String, SortedSet<UserMatch>> matchUsersMatrix(Map<String, SortedSet<UserMatch>> matchDictionary, List<User> users) {
        Map<String, Set<String>> matchHistory = new ConcurrentHashMap<>(users.size());

        for (User user : users) {
            Set<String> userMatches = matchHistory.computeIfAbsent(user.id(), o -> new HashSet<>());
            SortedSet<UserMatch> userDict = matchDictionary.computeIfAbsent(user.id(), o -> new TreeSet<>());
            var interests = convertArrayTISet(user.interests());
            for (User other : users) {
                if (user.equals(other)) continue;
                Set<String> otherUserMatches = matchHistory.computeIfAbsent(other.id(), o -> new HashSet<>());
                SortedSet<UserMatch> otherDict = matchDictionary.computeIfAbsent(other.id(), o -> new TreeSet<>());
                if (otherUserMatches.contains(user.id())) continue;
                SortedSet<String> otherInterests = convertArrayTISet(other.interests());
                otherInterests.retainAll(interests);
                int interestInCommon = otherInterests.size();

                var otherUserMatch = new UserMatch(user.id(), other.id(), interestInCommon);
                var userMatch = new UserMatch(other.id(), user.id(), interestInCommon);

                userDict.add(userMatch);
                otherDict.add(otherUserMatch);

                userMatches.add(other.id());
                otherUserMatches.add(user.id());
            }
        }

        return matchDictionary;
    }

    private static <T> SortedSet<T> convertArrayTISet(T[] data) {
        return new TreeSet<>(List.of(data));
    }

}
